"""
aaiclick.oplog.lineage - Oplog graph traversal (backward and forward lineage).
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Literal

from aaiclick.data.data_context.ch_client import _ch_client_var, create_ch_client, get_ch_client
from aaiclick.data.sql_utils import escape_sql_string

LineageDirection = Literal["backward", "forward"]


def _to_dict(kwargs_raw: Any) -> dict[str, str]:
    """Normalize kwargs from ClickHouse Map column.

    chdb returns Map(String, String) as a list of (key, value) tuples;
    clickhouse-connect returns a dict. Accept both.
    """
    if isinstance(kwargs_raw, dict):
        return kwargs_raw
    return dict(kwargs_raw)


def _to_aai_ids_dict(raw: Any) -> dict[str, list[int]]:
    """Normalize kwargs_aai_ids from ClickHouse Map(String, Array(UInt64)) column."""
    if isinstance(raw, dict):
        return {k: list(v) for k, v in raw.items()}
    return {k: list(v) for k, v in raw}


def _row_to_oplog_node(row: tuple) -> OplogNode:
    """Build an OplogNode from a raw operation_log row tuple.

    The query must select these 8 columns in order:
    result_table, operation, kwargs, kwargs_aai_ids, result_aai_ids,
    sql_template, task_id, job_id.
    """
    (result_table, operation, kwargs_raw, kwargs_aai_ids_raw, result_aai_ids_raw, sql_template, task_id, job_id) = row
    return OplogNode(
        table=result_table,
        operation=operation,
        kwargs=_to_dict(kwargs_raw),
        kwargs_aai_ids=_to_aai_ids_dict(kwargs_aai_ids_raw),
        result_aai_ids=list(result_aai_ids_raw),
        sql_template=sql_template,
        task_id=task_id,
        job_id=job_id,
    )


@dataclass
class OplogNode:
    table: str
    operation: str
    kwargs: dict[str, str]
    kwargs_aai_ids: dict[str, list[int]]
    result_aai_ids: list[int]
    sql_template: str | None
    task_id: int | None
    job_id: int | None


@dataclass
class OplogEdge:
    source: str
    target: str
    operation: str


_OP_LABEL_NAMES: dict[str, str] = {
    "+": "add",
    "-": "subtract",
    "*": "multiply",
    "/": "divide",
}


@dataclass
class OplogGraph:
    nodes: list[OplogNode] = field(default_factory=list)
    edges: list[OplogEdge] = field(default_factory=list)

    _ID_BREAKING_OPS = frozenset({"insert", "concat"})

    @property
    def tables(self) -> set[str]:
        """Return every table that appears in the graph as a node or a kwarg source."""
        return {n.table for n in self.nodes} | {src for n in self.nodes for src in n.kwargs.values() if src}

    def build_labels(self) -> dict[str, str]:
        """Map every referenced table ID to a human-readable label.

        Nodes get operation-derived labels (`source_A`, `multiply_result`).
        Edge endpoints that aren't in `nodes` fall through to generic
        `source_*` labels. Used for post-processing agent responses — NOT
        injected into the prompt so the LLM can still reference real table
        names in tool calls.
        """
        labels: dict[str, str] = {}
        source_counter = 0
        op_counters: dict[str, int] = {}
        source_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

        def _next_source_label() -> str:
            nonlocal source_counter
            letter = source_letters[source_counter % len(source_letters)]
            source_counter += 1
            return f"source_{letter}"

        for node in reversed(self.nodes):
            if node.table in labels:
                continue
            if node.operation == "create_from_value":
                labels[node.table] = _next_source_label()
            else:
                op = _OP_LABEL_NAMES.get(node.operation, node.operation)
                count = op_counters.get(op, 0)
                op_counters[op] = count + 1
                labels[node.table] = f"{op}_result" if count == 0 else f"{op}_result_{count + 1}"

        for edge in self.edges:
            for table in (edge.source, edge.target):
                if table not in labels:
                    labels[table] = _next_source_label()

        return labels

    _SNOWFLAKE_RE = re.compile(r"\bt_\d{16,20}\b|\b\d{16,20}\b")

    @staticmethod
    def replace_labels(text: str, labels: dict[str, str]) -> str:
        """Replace raw table IDs and snowflake IDs in text with labels.

        Builds a lookup from both full table IDs (t_123...) and their bare
        numeric parts (123...), then replaces all snowflake-shaped tokens
        in a single regex pass. Unrecognized IDs are left unchanged.
        """
        lookup: dict[str, str] = {}
        for table_id, label in labels.items():
            lookup[table_id] = label
            if table_id.startswith("t_"):
                lookup[table_id[2:]] = label

        def _sub(m: re.Match) -> str:
            return lookup.get(m.group(), m.group()) or m.group()

        return OplogGraph._SNOWFLAKE_RE.sub(_sub, text)

    def to_prompt_context(self) -> str:
        """Format the graph as human-readable text for LLM consumption."""
        lines = ["# Data Lineage Graph"]

        lines.append(f"\n## Operations ({len(self.nodes)})")
        for node in self.nodes:
            lines.append(f"\n### Table: `{node.table}`")
            lines.append(f"- Operation: `{node.operation}`")
            for k, v in node.kwargs.items():
                lines.append(f"- {k}: `{v}`")
            if node.sql_template:
                lines.append(f"- SQL: `{node.sql_template}`")
            if node.operation in self._ID_BREAKING_OPS:
                lines.append(
                    f"- ⚠ `{node.operation}` generates fresh aai_id values — source and target aai_ids do NOT match."
                )

        if self.edges:
            lines.append(f"\n## Data Flow ({len(self.edges)} edges)")
            for edge in self.edges:
                lines.append(f"- `{edge.source}` → `{edge.target}` (via `{edge.operation}`)")

        return "\n".join(lines)


@asynccontextmanager
async def lineage_context() -> AsyncIterator[None]:
    """Async context manager for lineage queries.

    Sets up a ClickHouse client for querying the operation log.
    Intended to be used after data_context exits:

        async with data_context(oplog=True):
            ...

        async with lineage_context():
            graph = await oplog_subgraph(table, direction="backward")
    """
    ch_client = await create_ch_client()
    token = _ch_client_var.set(ch_client)
    try:
        yield
    finally:
        _ch_client_var.reset(token)


async def backward_oplog(
    table: str,
    max_depth: int = 10,
) -> list[OplogNode]:
    """Trace all upstream operations that produced `table`.

    Uses WITH RECURSIVE for a single SQL round-trip. A `visited` array
    guards against revisiting nodes in diamond-shaped lineage graphs.
    """
    ch_client = get_ch_client()
    table_escaped = escape_sql_string(table)
    result = await ch_client.query(f"""
        WITH RECURSIVE upstream AS (
            SELECT result_table, operation, kwargs, kwargs_aai_ids, result_aai_ids,
                   sql_template, task_id, job_id,
                   0 AS depth, [result_table] AS visited
            FROM operation_log
            WHERE result_table = '{table_escaped}'

            UNION ALL

            SELECT ol.result_table, ol.operation, ol.kwargs, ol.kwargs_aai_ids,
                   ol.result_aai_ids, ol.sql_template, ol.task_id, ol.job_id,
                   u.depth + 1, arrayConcat(u.visited, [ol.result_table])
            FROM upstream u
            INNER JOIN operation_log ol
                ON hasAny(mapValues(u.kwargs), [ol.result_table])
            WHERE u.depth < {max_depth}
              AND NOT has(u.visited, ol.result_table)
        )
        SELECT DISTINCT result_table, operation, kwargs, kwargs_aai_ids, result_aai_ids,
               sql_template, task_id, job_id
        FROM upstream
    """)

    return [_row_to_oplog_node(row) for row in result.result_rows]


async def forward_oplog(
    table: str,
    max_depth: int = 10,
) -> list[OplogNode]:
    """Trace all downstream operations that consumed `table`, including the seed."""
    ch_client = get_ch_client()
    visited: set[str] = set()
    nodes: list[OplogNode] = []

    table_escaped = escape_sql_string(table)
    seed = await ch_client.query(f"""
        SELECT result_table, operation, kwargs, kwargs_aai_ids, result_aai_ids,
               sql_template, task_id, job_id
        FROM operation_log
        WHERE result_table = '{table_escaped}'
        LIMIT 1
    """)
    for row in seed.result_rows:
        node = _row_to_oplog_node(row)
        visited.add(node.table)
        nodes.append(node)

    frontier = [table]
    for _ in range(max_depth):
        if not frontier:
            break

        placeholders = ", ".join(f"'{t}'" for t in frontier)
        result = await ch_client.query(f"""
            SELECT result_table, operation, kwargs, kwargs_aai_ids, result_aai_ids,
                   sql_template, task_id, job_id
            FROM operation_log
            WHERE arrayExists(v -> v IN ({placeholders}), mapValues(kwargs))
            ORDER BY created_at ASC
        """)

        next_frontier: list[str] = []
        for row in result.result_rows:
            node = _row_to_oplog_node(row)
            if node.table in visited:
                continue
            visited.add(node.table)
            nodes.append(node)
            next_frontier.append(node.table)

        frontier = next_frontier

    return nodes


async def oplog_subgraph(
    table: str,
    direction: LineageDirection = "backward",
    max_depth: int = 10,
) -> OplogGraph:
    """Return a structured OplogGraph for visualization or AI context."""
    if direction == "backward":
        nodes = await backward_oplog(table, max_depth)
    elif direction == "forward":
        nodes = await forward_oplog(table, max_depth)
    else:
        raise ValueError(f"direction must be 'backward' or 'forward', got '{direction}'")

    edges: list[OplogEdge] = []
    for node in nodes:
        for src in node.kwargs.values():
            edges.append(OplogEdge(source=src, target=node.table, operation=node.operation))

    return OplogGraph(nodes=nodes, edges=edges)


@dataclass
class RowLineageStep:
    """One hop in a row-level lineage trace.

    ``source_aai_ids`` maps each input role to the aligned source row id
    for the current position, so callers can walk backward one role at
    a time or render the full graph.
    """

    table: str
    aai_id: int
    operation: str
    source_aai_ids: dict[str, int]


@dataclass
class RowUpstream:
    """Producing-operation record for one ``(table, aai_id)`` pair.

    Returned by :func:`fetch_producing_op`. ``sources`` maps each input
    role to the ``(source_table, source_aai_id)`` that fed this target
    at that role — already positionally resolved, so consumers never
    touch raw ``result_aai_ids`` / ``kwargs_aai_ids`` arrays.
    """

    operation: str
    sources: dict[str, tuple[str, int]]


async def fetch_producing_op(
    table: str,
    aai_id: int,
    *,
    job_id: int | None = None,
) -> RowUpstream | None:
    """Return the oplog row that produced ``(table, aai_id)``, or ``None``.

    When ``job_id`` is provided the lookup is scoped to that job so the
    walk doesn't cross job boundaries — strongly recommended for
    persistent tables that may have been re-produced by multiple jobs
    (replays / repeated runs with the same persistent name). Without
    ``job_id`` the most recent matching oplog row wins, which is fine
    for fresh ephemeral ``t_*`` tables whose snowflake ids make
    collisions impossible.

    Returns ``None`` for rows whose lineage arrays are empty (the
    operation ran under ``PreservationMode.NONE`` / ``FULL``).
    """
    ch_client = get_ch_client()
    job_filter = f"AND job_id = {job_id}" if job_id is not None else ""
    result = await ch_client.query(
        f"SELECT operation, kwargs, kwargs_aai_ids, result_aai_ids "
        f"FROM operation_log "
        f"WHERE result_table = '{escape_sql_string(table)}' "
        f"  AND has(result_aai_ids, {aai_id}) {job_filter} "
        f"ORDER BY created_at DESC LIMIT 1"
    )
    if not result.result_rows:
        return None
    operation, kwargs_raw, kwargs_aai_ids_raw, result_aai_ids_raw = result.result_rows[0]
    result_aai_ids = list(result_aai_ids_raw)
    try:
        position = result_aai_ids.index(aai_id)
    except ValueError:
        return None

    kwargs = _to_dict(kwargs_raw)
    kwargs_aai_ids = _to_aai_ids_dict(kwargs_aai_ids_raw)
    sources: dict[str, tuple[str, int]] = {}
    for role, ids in kwargs_aai_ids.items():
        if position >= len(ids):
            continue
        source_table = kwargs.get(role)
        if not source_table:
            continue
        sources[role] = (source_table, ids[position])

    return RowUpstream(operation=operation, sources=sources)


async def backward_oplog_row(
    table: str,
    aai_id: int,
    max_depth: int = 10,
    *,
    job_id: int | None = None,
) -> list[RowLineageStep]:
    """Trace a single ``aai_id`` backward through the oplog.

    Reads the per-operation ``kwargs_aai_ids`` / ``result_aai_ids`` that
    are populated under ``PreservationMode.STRATEGY``. Steps are ordered
    most-recent-first. Returns ``[]`` when no oplog row carries this id
    (common under ``NONE`` / ``FULL`` mode).

    When ``job_id`` is provided, every hop's oplog lookup is scoped to
    that job so the walk cannot cross job boundaries. Pass it when the
    starting table is persistent (``p_*``) and may have been re-produced
    by other jobs; omit it for freshly-generated ephemeral tables whose
    snowflake ids guarantee uniqueness.
    """
    steps: list[RowLineageStep] = []
    current_table = table
    current_id = aai_id

    for _ in range(max_depth):
        upstream = await fetch_producing_op(
            current_table,
            current_id,
            job_id=job_id,
        )
        if upstream is None:
            break

        source_aai_ids = {role: aid for role, (_, aid) in upstream.sources.items()}
        steps.append(
            RowLineageStep(
                table=current_table,
                aai_id=current_id,
                operation=upstream.operation,
                source_aai_ids=source_aai_ids,
            )
        )

        if not upstream.sources:
            break
        first_role = next(iter(upstream.sources))
        current_table, current_id = upstream.sources[first_role]

    return steps
