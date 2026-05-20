"""
aaiclick.oplog.lineage - Oplog graph traversal (backward and forward lineage).
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Literal

from pydantic import BaseModel, Field

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


def _row_to_oplog_node(row: tuple) -> OplogNode:
    """Build an OplogNode from a raw operation_log row tuple.

    The query must select these 6 columns in order:
    result_table, operation, kwargs, sql_template, task_id, job_id.
    """
    (result_table, operation, kwargs_raw, sql_template, task_id, job_id) = row
    return OplogNode(
        table=result_table,
        operation=operation,
        kwargs=_to_dict(kwargs_raw),
        sql_template=sql_template,
        task_id=task_id,
        job_id=job_id,
    )


class OplogNode(BaseModel):
    table: str
    operation: str
    kwargs: dict[str, str]
    sql_template: str | None = None
    task_id: int | None = None
    job_id: int | None = None


class OplogEdge(BaseModel):
    source: str
    target: str
    operation: str


_OP_LABEL_NAMES: dict[str, str] = {
    "+": "add",
    "-": "subtract",
    "*": "multiply",
    "/": "divide",
}


class OplogGraph(BaseModel):
    nodes: list[OplogNode] = Field(default_factory=list)
    edges: list[OplogEdge] = Field(default_factory=list)

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

    @staticmethod
    def replace_labels(text: str, labels: dict[str, str]) -> str:
        """Replace raw table identifiers in text with human-readable labels.

        Handles every identifier shape that LLMs emit when describing
        lineage: full table names (`t_<id>`, `j_<job_id>_<name>`,
        `p_<name>`, or any custom string in `labels`) and the bare
        snowflake form of `t_<id>` keys. Longer keys are tried first
        so a shorter substring cannot pre-empt a longer match.
        Unregistered tokens are left unchanged.
        """
        if not labels:
            return text

        lookup: dict[str, str] = {}
        for table_id, label in labels.items():
            lookup[table_id] = label
            if table_id.startswith("t_"):
                lookup[table_id[2:]] = label

        keys = sorted(lookup, key=len, reverse=True)
        pattern = re.compile(r"(?<!\w)(?:" + "|".join(re.escape(k) for k in keys) + r")(?!\w)")
        return pattern.sub(lambda m: lookup[m.group()], text)

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
            SELECT result_table, operation, kwargs,
                   sql_template, task_id, job_id,
                   0 AS depth, [result_table] AS visited
            FROM operation_log
            WHERE result_table = '{table_escaped}'

            UNION ALL

            SELECT ol.result_table, ol.operation, ol.kwargs,
                   ol.sql_template, ol.task_id, ol.job_id,
                   u.depth + 1, arrayConcat(u.visited, [ol.result_table])
            FROM upstream u
            INNER JOIN operation_log ol
                ON hasAny(mapValues(u.kwargs), [ol.result_table])
            WHERE u.depth < {max_depth}
              AND NOT has(u.visited, ol.result_table)
        )
        SELECT DISTINCT result_table, operation, kwargs,
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
        SELECT result_table, operation, kwargs,
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
            SELECT result_table, operation, kwargs,
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
