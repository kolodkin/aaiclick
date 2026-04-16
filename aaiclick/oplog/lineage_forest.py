"""
aaiclick.oplog.lineage_forest - Route-collapsed row lineage for strategy-matched rows.

Three primitives:

- ``build_forest`` walks backward from every strategy-matched row in a
  target table, producing one tree per matched row grounded in real
  column values pulled from the live tables.
- ``collapse_to_routes`` groups leaf-to-root paths by structural
  signature, converting row-count explosion into pipeline-shape
  enumeration (N matched rows sharing a pipeline shape → 1 route).
- ``render_routes`` formats the collapsed routes as markdown for
  injection into an LLM prompt.

``build_and_render`` composes all three in one call.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from aaiclick.data.data_context.ch_client import get_ch_client
from aaiclick.data.sql_utils import escape_sql_string, quote_identifier

from .lineage import fetch_producing_op

VALUE_COLUMN = "value"
"""Column sampled at every node to ground the forest in real data."""

MAX_FOREST_ROOTS = 200
"""Cap on how many target rows a single forest build fans out across.
Prevents pathological explosions when a strategy matches millions of
rows; the rendered output notes the truncation."""

MAX_EXEMPLARS_PER_ROUTE = 5
"""Cap on how many concrete paths each route renders."""

_SOURCE = "source"
_TRUNCATED = "truncated"


@dataclass
class LineageNode:
    """One row in a lineage tree.

    ``children`` maps an input role (``"left"`` / ``"right"`` / ...) to
    the upstream node that fed that role at this operation. Leaves
    carry ``operation=_SOURCE`` and no children.
    """

    table: str
    aai_id: int
    operation: str
    role: str | None
    values: dict[str, Any] | None
    children: dict[str, LineageNode] = field(default_factory=dict)


@dataclass
class Route:
    """A structural path through the lineage DAG shared by ``match_count`` rows.

    ``signature`` is a leaf-to-root tuple of ``(table, operation, role)``
    triples; two paths collapse into the same route iff their signatures
    match. ``exemplars`` carries up to ``MAX_EXEMPLARS_PER_ROUTE``
    concrete paths so readers can see real rows at every hop.
    """

    signature: tuple[tuple[str, str, str | None], ...]
    match_count: int
    leaf_table: str
    leaf_values: list[Any]
    root_table: str
    root_values: list[Any]
    exemplars: list[list[tuple[str, int, Any]]]


async def build_forest(
    target_table: str,
    *,
    job_id: int | None = None,
    max_depth: int = 10,
    max_roots: int = MAX_FOREST_ROOTS,
) -> list[LineageNode]:
    """Build one lineage tree per strategy-matched row in ``target_table``.

    When ``job_id`` is provided, oplog lookups are scoped to that job so
    repeated runs or replays don't cross boundaries. Walks are memoized
    by ``(table, aai_id)`` within one build, so a row reached from
    multiple roots is queried once and the resulting node is shared
    across trees.

    Returns ``[]`` when the target has no strategy-matched rows; callers
    should treat that as "no row lineage available".
    """
    ch_client = get_ch_client()
    job_filter = _job_filter(job_id)
    row = await ch_client.query(
        f"SELECT result_aai_ids FROM operation_log "
        f"WHERE result_table = '{escape_sql_string(target_table)}' {job_filter} "
        f"ORDER BY created_at DESC LIMIT 1"
    )
    if not row.result_rows:
        return []
    root_ids = list(row.result_rows[0][0])[:max_roots]
    if not root_ids:
        return []

    cache: dict[tuple[str, int], LineageNode] = {}
    return await asyncio.gather(
        *(
            _walk(
                target_table,
                aai_id,
                role=None,
                depth=0,
                max_depth=max_depth,
                job_id=job_id,
                cache=cache,
            )
            for aai_id in root_ids
        )
    )


async def _walk(
    table: str,
    aai_id: int,
    *,
    role: str | None,
    depth: int,
    max_depth: int,
    job_id: int | None,
    cache: dict[tuple[str, int], LineageNode],
) -> LineageNode:
    """Recursively walk one row backward, producing a LineageNode tree."""
    key = (table, aai_id)
    cached = cache.get(key)
    if cached is not None:
        # Role is encoded on the edge (parent's ``children`` dict key),
        # not on the node, so sharing a node across parents is safe.
        return cached

    values = await _fetch_row_values(table, aai_id)

    if depth >= max_depth:
        return _memo(cache, key, LineageNode(
            table=table, aai_id=aai_id, operation=_TRUNCATED,
            role=role, values=values,
        ))

    upstream = await fetch_producing_op(table, aai_id, job_id=job_id)
    if upstream is None:
        return _memo(cache, key, LineageNode(
            table=table, aai_id=aai_id, operation=_SOURCE,
            role=role, values=values,
        ))

    child_roles = list(upstream.sources)
    child_nodes = await asyncio.gather(
        *(
            _walk(
                upstream.sources[input_role][0],
                upstream.sources[input_role][1],
                role=input_role, depth=depth + 1,
                max_depth=max_depth, job_id=job_id, cache=cache,
            )
            for input_role in child_roles
        )
    )
    children = dict(zip(child_roles, child_nodes, strict=False))

    return _memo(cache, key, LineageNode(
        table=table, aai_id=aai_id, operation=upstream.operation,
        role=role, values=values, children=children,
    ))


def _memo(
    cache: dict[tuple[str, int], LineageNode],
    key: tuple[str, int],
    node: LineageNode,
) -> LineageNode:
    cache[key] = node
    return node


async def _fetch_row_values(
    table: str,
    aai_id: int,
) -> dict[str, Any] | None:
    """Pull ``VALUE_COLUMN`` for one row. Returns ``None`` when missing."""
    ch_client = get_ch_client()
    try:
        result = await ch_client.query(
            f"SELECT {quote_identifier(VALUE_COLUMN)} FROM {quote_identifier(table)} "
            f"WHERE aai_id = {aai_id} LIMIT 1"
        )
    except Exception:
        return None
    if not result.result_rows:
        return None
    return {VALUE_COLUMN: result.result_rows[0][0]}


def _job_filter(job_id: int | None) -> str:
    return f"AND job_id = {job_id}" if job_id is not None else ""


def collapse_to_routes(forest: list[LineageNode]) -> list[Route]:
    """Group every leaf-to-root path in ``forest`` by structural signature.

    Each unique signature becomes one ``Route`` with match count,
    aggregated leaf/root values, and up to ``MAX_EXEMPLARS_PER_ROUTE``
    concrete paths.
    """
    path_groups: dict[
        tuple[tuple[str, str, str | None], ...],
        list[list[LineageNode]],
    ] = defaultdict(list)

    for tree in forest:
        for path in _enumerate_paths(tree):
            signature = tuple((n.table, n.operation, n.role) for n in path)
            path_groups[signature].append(path)

    return [
        Route(
            signature=signature,
            match_count=len(paths),
            leaf_table=paths[0][0].table,
            leaf_values=[_extract_value(p[0]) for p in paths],
            root_table=paths[0][-1].table,
            root_values=[_extract_value(p[-1]) for p in paths],
            exemplars=[
                [(n.table, n.aai_id, _extract_value(n)) for n in path]
                for path in paths[:MAX_EXEMPLARS_PER_ROUTE]
            ],
        )
        for signature, paths in path_groups.items()
    ]


def _enumerate_paths(node: LineageNode) -> list[list[LineageNode]]:
    """Yield every leaf-to-root path under ``node`` as a leaf-first list."""
    if not node.children:
        return [[node]]
    paths: list[list[LineageNode]] = []
    for child in node.children.values():
        for subpath in _enumerate_paths(child):
            paths.append([*subpath, node])
    return paths


def _extract_value(node: LineageNode) -> Any:
    if not node.values:
        return None
    return next(iter(node.values.values()))


def render_routes(routes: list[Route]) -> str:
    """Render ``routes`` as a markdown block for LLM consumption.

    Empty input returns an empty string so callers can append
    unconditionally to an existing prompt context.
    """
    if not routes:
        return ""

    total_matches = sum(r.match_count for r in routes)
    lines: list[str] = [
        "## Row-Level Lineage (strategy-matched)",
        "",
        f"- Unique routes: {len(routes)}",
        f"- Total matched paths: {total_matches}",
    ]

    for idx, route in enumerate(routes, start=1):
        lines.append("")
        lines.append(f"### Route {idx}: {_render_signature(route.signature)}")
        lines.append(f"- matched: {route.match_count} rows")
        lines.append(
            f"- leaf values (`{route.leaf_table}`): "
            f"{_format_values(route.leaf_values)}"
        )
        lines.append(
            f"- root values (`{route.root_table}`): "
            f"{_format_values(route.root_values)}"
        )

        shown = len(route.exemplars)
        hidden = route.match_count - shown
        if shown == 1:
            lines.append("- exemplar path:")
        elif hidden > 0:
            lines.append(f"- exemplar paths ({shown} of {route.match_count}):")
        else:
            lines.append(f"- exemplar paths ({shown}):")
        for path in route.exemplars:
            lines.append(f"    - {_render_exemplar(path)}")
        if hidden > 0:
            lines.append(f"    - …and {hidden} more path(s) with the same shape")

    return "\n".join(lines)


def _render_signature(
    signature: tuple[tuple[str, str, str | None], ...],
) -> str:
    """Format a signature as ``leaf → op(role) → ... → root``.

    Each non-leaf hop is annotated with the role that the *previous*
    hop's table filled at this op — so the reader can see exactly how
    each table entered its downstream op.
    """
    if not signature:
        return ""
    parts: list[str] = [f"`{signature[0][0]}`"]
    for i in range(1, len(signature)):
        table, op, _ = signature[i]
        prev_role = signature[i - 1][2]
        role_tag = f" as `{prev_role}`" if prev_role else ""
        parts.append(f"→ `{op}`{role_tag} → `{table}`")
    return " ".join(parts)


def _render_exemplar(path: list[tuple[str, int, Any]]) -> str:
    return " → ".join(
        f"`{table}`#{aai_id}={value}" for table, aai_id, value in path
    )


def _format_values(values: list[Any]) -> str:
    """Render a value list, collapsing all-equal lists to a single entry."""
    if not values:
        return "[]"
    unique = {v for v in values if v is not None}
    if len(unique) == 1 and None not in values:
        return f"[{next(iter(unique))}] (×{len(values)})"
    return "[" + ", ".join(str(v) for v in values) + "]"


async def build_and_render(
    target_table: str,
    *,
    job_id: int | None = None,
    max_depth: int = 10,
) -> str:
    """Convenience: ``build_forest`` → ``collapse_to_routes`` → ``render_routes``."""
    forest = await build_forest(
        target_table, job_id=job_id, max_depth=max_depth,
    )
    if not forest:
        return ""
    return render_routes(collapse_to_routes(forest))
