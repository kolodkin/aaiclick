"""
aaiclick.oplog.lineage_forest - Route-collapsed row lineage for strategy-matched rows.

Under ``PreservationMode.STRATEGY`` every operation writes the
``kwargs_aai_ids`` / ``result_aai_ids`` arrays, so the oplog carries a
row-level DAG of every matched row from the target backward to its
persistent inputs. This module turns that row-level data into something
an LLM can actually digest:

1. ``build_forest`` walks backward from every strategy-matched row in
   the target table, producing one ``LineageNode`` tree per matched
   row. Each node carries the row's column snapshot pulled from the
   live table.

2. ``collapse_to_routes`` groups paths by their structural signature
   (``(table, operation, role)`` from leaf to root), so N rows sharing
   the same pipeline shape collapse into one ``Route`` record. Match
   count explosion becomes pipeline-shape enumeration.

3. ``render_routes`` formats the collapsed routes as markdown suitable
   for injection into a debug agent's prompt — one block per route with
   aggregated leaf/root values and a single exemplar's full aai_id
   chain for grounding.

The three primitives are independent: callers can build the forest
without rendering, render a pre-built list of routes, or skip straight
to ``build_and_render`` for the default pipeline.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .lineage import _to_aai_ids_dict, _to_dict
from aaiclick.data.data_context.ch_client import get_ch_client


@dataclass
class LineageNode:
    """One row in a lineage tree.

    ``children`` maps an input role (e.g. ``"left"`` / ``"right"``) to
    the upstream ``LineageNode`` that fed that role at this operation.
    Leaves carry ``operation="source"`` and no children.
    """

    table: str
    aai_id: int
    operation: str
    role: Optional[str]
    values: Optional[Dict[str, Any]]
    children: Dict[str, "LineageNode"] = field(default_factory=dict)


@dataclass
class Route:
    """A structural path through the lineage DAG shared by ``match_count`` rows.

    ``signature`` is a leaf-to-root tuple of ``(table, operation, role)``
    triples. Two paths collapse into the same route iff their signatures
    are equal — i.e. they went through the same operations via the same
    roles.

    The aggregated value lists and the exemplar chain together let a
    reader ground one concrete row while still seeing the full set of
    matched values at the leaf and root.
    """

    signature: Tuple[Tuple[str, str, Optional[str]], ...]
    match_count: int
    leaf_table: str
    leaf_values: List[Any]
    root_table: str
    root_values: List[Any]
    exemplar: List[Tuple[str, int, Any]]  # (table, aai_id, value) leaf → root


async def build_forest(
    target_table: str,
    *,
    job_id: Optional[int] = None,
    max_depth: int = 10,
    value_column: str = "value",
) -> List[LineageNode]:
    """Build one lineage tree per strategy-matched row in ``target_table``.

    Reads the target's ``operation_log`` row, takes every ``aai_id`` in
    ``result_aai_ids`` as a forest root, and walks each one backward
    through the positional ``kwargs_aai_ids`` arrays. At every node,
    the row's ``value_column`` is pulled from the live table to ground
    the forest in real data.

    When ``job_id`` is provided the oplog lookup is scoped to that job
    — mandatory when the same table has been re-produced by multiple
    jobs (replays, repeated runs) so the walk doesn't cross job
    boundaries. When ``None``, the most recent matching oplog row wins,
    which is fine for single-job pipelines.

    Returns an empty list when the target has no strategy-matched rows
    (e.g. the job ran in ``NONE`` / ``FULL`` mode) — callers should
    treat empty forests as "no row lineage available" and fall back to
    structural context.
    """
    ch_client = get_ch_client()
    table_escaped = target_table.replace("'", "\\'")

    job_filter = f"AND job_id = {job_id}" if job_id is not None else ""
    row = await ch_client.query(
        f"SELECT result_aai_ids FROM operation_log "
        f"WHERE result_table = '{table_escaped}' {job_filter} "
        f"ORDER BY created_at DESC LIMIT 1"
    )
    if not row.result_rows:
        return []
    root_ids = list(row.result_rows[0][0])
    if not root_ids:
        return []

    return [
        await _walk(
            target_table,
            aai_id,
            role=None,
            depth=0,
            max_depth=max_depth,
            value_column=value_column,
            job_id=job_id,
        )
        for aai_id in root_ids
    ]


async def _walk(
    table: str,
    aai_id: int,
    *,
    role: Optional[str],
    depth: int,
    max_depth: int,
    value_column: str,
    job_id: Optional[int],
) -> LineageNode:
    """Recursively walk one row backward, producing a LineageNode tree."""
    ch_client = get_ch_client()
    values = await _fetch_row_values(table, aai_id, value_column)

    if depth >= max_depth:
        return LineageNode(
            table=table,
            aai_id=aai_id,
            operation="truncated",
            role=role,
            values=values,
        )

    table_escaped = table.replace("'", "\\'")
    job_filter = f"AND job_id = {job_id}" if job_id is not None else ""
    result = await ch_client.query(
        f"SELECT operation, kwargs, kwargs_aai_ids, result_aai_ids "
        f"FROM operation_log "
        f"WHERE result_table = '{table_escaped}' "
        f"  AND has(result_aai_ids, {aai_id}) {job_filter} "
        f"ORDER BY created_at DESC LIMIT 1"
    )
    if not result.result_rows:
        # No upstream op — this row is a leaf (persistent input or
        # pre-existing table).
        return LineageNode(
            table=table,
            aai_id=aai_id,
            operation="source",
            role=role,
            values=values,
        )

    operation, kwargs_raw, kwargs_aai_ids_raw, result_aai_ids_raw = result.result_rows[0]
    kwargs = _to_dict(kwargs_raw)
    kwargs_aai_ids = _to_aai_ids_dict(kwargs_aai_ids_raw)
    result_aai_ids = list(result_aai_ids_raw)

    try:
        pos = result_aai_ids.index(aai_id)
    except ValueError:
        return LineageNode(
            table=table,
            aai_id=aai_id,
            operation=operation,
            role=role,
            values=values,
        )

    children: Dict[str, LineageNode] = {}
    for input_role, source_ids in kwargs_aai_ids.items():
        if pos >= len(source_ids):
            continue
        source_table = kwargs.get(input_role)
        if not source_table:
            continue
        children[input_role] = await _walk(
            source_table,
            source_ids[pos],
            role=input_role,
            depth=depth + 1,
            max_depth=max_depth,
            value_column=value_column,
            job_id=job_id,
        )

    return LineageNode(
        table=table,
        aai_id=aai_id,
        operation=operation,
        role=role,
        values=values,
        children=children,
    )


async def _fetch_row_values(
    table: str,
    aai_id: int,
    value_column: str,
) -> Optional[Dict[str, Any]]:
    """Pull ``value_column`` for one row from ``table``. Returns ``None``
    when the column does not exist or the row is missing."""
    ch_client = get_ch_client()
    table_escaped = table.replace("'", "\\'")
    col_escaped = value_column.replace("`", "\\`")
    try:
        result = await ch_client.query(
            f"SELECT `{col_escaped}` FROM {table_escaped} "
            f"WHERE aai_id = {aai_id} LIMIT 1"
        )
    except Exception:
        return None
    if not result.result_rows:
        return None
    return {value_column: result.result_rows[0][0]}


def collapse_to_routes(forest: List[LineageNode]) -> List[Route]:
    """Group every leaf-to-root path in ``forest`` by its structural signature.

    Each unique signature becomes one ``Route`` with the match count,
    aggregated leaf/root values across every path that shares the
    signature, and the first observed path as the exemplar.
    """
    path_groups: Dict[
        Tuple[Tuple[str, str, Optional[str]], ...],
        List[List[LineageNode]],
    ] = defaultdict(list)

    for tree in forest:
        for path in _enumerate_paths(tree):
            signature = tuple((n.table, n.operation, n.role) for n in path)
            path_groups[signature].append(path)

    routes: List[Route] = []
    for signature, paths in path_groups.items():
        first_path = paths[0]
        leaf_table = first_path[0].table
        root_table = first_path[-1].table
        routes.append(
            Route(
                signature=signature,
                match_count=len(paths),
                leaf_table=leaf_table,
                leaf_values=[_extract_value(p[0]) for p in paths],
                root_table=root_table,
                root_values=[_extract_value(p[-1]) for p in paths],
                exemplar=[
                    (n.table, n.aai_id, _extract_value(n)) for n in first_path
                ],
            )
        )
    return routes


def _enumerate_paths(node: LineageNode) -> List[List[LineageNode]]:
    """Enumerate every leaf-to-root path under ``node``.

    Returned paths are leaf-first lists. A leaf (no children) yields a
    single one-node path; internal nodes concatenate each child's paths
    with themselves appended at the root end.
    """
    if not node.children:
        return [[node]]
    paths: List[List[LineageNode]] = []
    for child in node.children.values():
        for subpath in _enumerate_paths(child):
            paths.append([*subpath, node])
    return paths


def _extract_value(node: LineageNode) -> Any:
    """Return the node's primary column value or ``None``."""
    if node.values is None:
        return None
    if not node.values:
        return None
    return next(iter(node.values.values()))


def render_routes(routes: List[Route]) -> str:
    """Render ``routes`` as a markdown block for LLM consumption.

    Each route gets: a one-line signature header, the match count, the
    leaf/root value lists (deduplicated when all values agree), and the
    exemplar chain with concrete aai_ids. Empty input returns an empty
    string so callers can append unconditionally.
    """
    if not routes:
        return ""

    total_matches = sum(r.match_count for r in routes)
    lines: List[str] = [
        f"## Row-Level Lineage (strategy-matched)",
        f"",
        f"- Unique routes: {len(routes)}",
        f"- Total matched paths: {total_matches}",
    ]

    for idx, route in enumerate(routes, start=1):
        lines.append("")
        lines.append(
            f"### Route {idx}: {_render_signature(route.signature)}"
        )
        lines.append(f"- matched: {route.match_count} rows")
        lines.append(
            f"- leaf values ({route.leaf_table}): "
            f"{_format_values(route.leaf_values)}"
        )
        lines.append(
            f"- root values ({route.root_table}): "
            f"{_format_values(route.root_values)}"
        )
        lines.append(f"- exemplar:")
        for table, aai_id, value in route.exemplar:
            lines.append(
                f"    - `{table}` aai_id={aai_id} value={value}"
            )

    return "\n".join(lines)


def _render_signature(
    signature: Tuple[Tuple[str, str, Optional[str]], ...],
) -> str:
    """Format a signature tuple as ``leaf_table → op(role) → ... → root_table``."""
    if not signature:
        return ""
    parts: List[str] = [signature[0][0]]
    for table, op, role in signature[1:]:
        role_tag = f"[{role}]" if role else ""
        parts.append(f"{op}{role_tag} → {table}")
    return " ".join(parts)


def _format_values(values: List[Any]) -> str:
    """Render a value list, collapsing all-equal lists to a single entry."""
    if not values:
        return "[]"
    unique = {v for v in values if v is not None}
    if len(unique) == 1 and None not in values:
        only = next(iter(unique))
        return f"[{only}] (×{len(values)})"
    return "[" + ", ".join(str(v) for v in values) + "]"


async def build_and_render(
    target_table: str,
    *,
    job_id: Optional[int] = None,
    max_depth: int = 10,
    value_column: str = "value",
) -> str:
    """Convenience: ``build_forest`` → ``collapse_to_routes`` → ``render_routes``.

    Returns an empty string when the target has no strategy-matched
    rows, so callers can safely append the result to an existing prompt
    context without a guard.
    """
    forest = await build_forest(
        target_table,
        job_id=job_id,
        max_depth=max_depth,
        value_column=value_column,
    )
    if not forest:
        return ""
    routes = collapse_to_routes(forest)
    return render_routes(routes)
