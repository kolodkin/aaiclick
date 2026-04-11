"""
aaiclick.oplog.lineage - Oplog graph traversal (backward and forward lineage).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
import re
from typing import Any, AsyncIterator

from aaiclick.data.data_context.ch_client import create_ch_client, get_ch_client, _ch_client_var


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


@dataclass
class OplogGraph:
    nodes: list[OplogNode] = field(default_factory=list)
    edges: list[OplogEdge] = field(default_factory=list)

    _ID_BREAKING_OPS = frozenset({"insert", "concat"})

    def build_labels(self) -> dict[str, str]:
        """Assign human-readable labels to each table based on its operation.

        Returns a mapping from table ID to label (e.g. source_A, multiply_result).
        Used for post-processing agent responses — NOT injected into the prompt
        so the LLM can still reference real table names in tool calls.
        """
        labels: dict[str, str] = {}
        source_counter = 0
        op_counters: dict[str, int] = {}
        source_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

        for node in reversed(self.nodes):
            if node.table in labels:
                continue
            if node.operation == "create_from_value":
                letter = source_letters[source_counter % len(source_letters)]
                labels[node.table] = f"source_{letter}"
                source_counter += 1
            else:
                op = node.operation.replace("+", "add").replace("*", "multiply").replace(
                    "-", "subtract").replace("/", "divide")
                count = op_counters.get(op, 0)
                op_counters[op] = count + 1
                labels[node.table] = f"{op}_result" if count == 0 else f"{op}_result_{count + 1}"

        return labels

    _SNOWFLAKE_RE = re.compile(r"\bt_\d{16,20}\b|\b\d{16,20}\b")

    @staticmethod
    def replace_labels(text: str, labels: dict[str, str]) -> str:
        """Replace raw table IDs and snowflake IDs in text with labels.

        Builds a lookup from both full table IDs (t_123...) and their bare
        numeric parts (123...), then replaces all snowflake-shaped tokens
        in a single regex pass. Unknown IDs are replaced with ``…``.
        """
        lookup: dict[str, str] = {}
        for table_id, label in labels.items():
            lookup[table_id] = label
            if table_id.startswith("t_"):
                lookup[table_id[2:]] = label

        def _sub(m: re.Match) -> str:
            return lookup.get(m.group(), "…")

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
                    f"- ⚠ `{node.operation}` generates fresh aai_id values — "
                    "source and target aai_ids do NOT match."
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
    table_escaped = table.replace("'", "\\'")
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

    return [
        OplogNode(
            table=result_table,
            operation=operation,
            kwargs=_to_dict(kwargs_raw),
            kwargs_aai_ids=_to_aai_ids_dict(kwargs_aai_ids_raw),
            result_aai_ids=list(result_aai_ids_raw),
            sql_template=sql_template,
            task_id=task_id,
            job_id=job_id,
        )
        for result_table, operation, kwargs_raw, kwargs_aai_ids_raw, result_aai_ids_raw,
            sql_template, task_id, job_id
        in result.result_rows
    ]


async def forward_oplog(
    table: str,
    max_depth: int = 10,
) -> list[OplogNode]:
    """Trace all downstream operations that consumed `table`.

    Returns nodes in BFS order starting from operations that used `table`.
    """
    ch_client = get_ch_client()
    visited: set[str] = set()
    frontier = [table]
    nodes: list[OplogNode] = []

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
            (result_table, operation, kwargs_raw, kwargs_aai_ids_raw,
             result_aai_ids_raw, sql_template, task_id, job_id) = row
            if result_table in visited:
                continue
            visited.add(result_table)
            node = OplogNode(
                table=result_table,
                operation=operation,
                kwargs=_to_dict(kwargs_raw),
                kwargs_aai_ids=_to_aai_ids_dict(kwargs_aai_ids_raw),
                result_aai_ids=list(result_aai_ids_raw),
                sql_template=sql_template,
                task_id=task_id,
                job_id=job_id,
            )
            nodes.append(node)
            next_frontier.append(result_table)

        frontier = next_frontier

    return nodes


async def oplog_subgraph(
    table: str,
    direction: str = "backward",
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
    """One step in a row-level lineage trace."""

    table: str
    aai_id: int
    operation: str
    source_aai_ids: dict[str, int]


async def backward_oplog_row(
    table: str,
    aai_id: int,
    max_depth: int = 10,
) -> list[RowLineageStep]:
    """Trace a specific aai_id backward through the lineage chain.

    Walks the oplog samples: finds the operation that produced this aai_id,
    extracts the corresponding source aai_ids, then recurses into each source.

    Returns steps in reverse order (most recent operation first).
    """
    ch_client = get_ch_client()
    steps: list[RowLineageStep] = []
    current_table = table
    current_id = aai_id

    for _ in range(max_depth):
        table_escaped = current_table.replace("'", "\\'")

        # Find the operation that produced this aai_id
        result = await ch_client.query(f"""
            SELECT operation, kwargs, kwargs_aai_ids, result_aai_ids
            FROM operation_log
            WHERE result_table = '{table_escaped}'
              AND has(result_aai_ids, {current_id})
            LIMIT 1
        """)

        if not result.result_rows:
            break

        operation, kwargs_raw, kwargs_aai_ids_raw, result_aai_ids_raw = result.result_rows[0]
        kwargs = _to_dict(kwargs_raw)
        kwargs_aai_ids = _to_aai_ids_dict(kwargs_aai_ids_raw)
        result_aai_ids = list(result_aai_ids_raw)

        # Find the position of current_id in result_aai_ids
        try:
            pos = result_aai_ids.index(current_id)
        except ValueError:
            break

        # Extract corresponding source aai_ids at the same position
        source_aai_ids: dict[str, int] = {}
        for role, ids in kwargs_aai_ids.items():
            if pos < len(ids):
                source_aai_ids[role] = ids[pos]

        steps.append(RowLineageStep(
            table=current_table,
            aai_id=current_id,
            operation=operation,
            source_aai_ids=source_aai_ids,
        ))

        # Pick one source to follow (first source with an aai_id)
        if not source_aai_ids:
            break
        first_role = next(iter(source_aai_ids))
        current_id = source_aai_ids[first_role]
        current_table = kwargs.get(first_role, "")
        if not current_table:
            break

    return steps
