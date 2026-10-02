"""
aaiclick.oplog.lineage - Oplog graph traversal (backward and forward lineage).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Literal

from pydantic import BaseModel, Field

from aaiclick.data.data_context.ch_client import _ch_client_var, create_ch_client, get_ch_client
from aaiclick.data.sql_utils import quote_sql_literal

LineageDirection = Literal["backward", "forward"]
DEFAULT_MAX_DEPTH = 10

NodeKind = Literal["input", "intermediate", "target"]


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


class GraphNode(BaseModel):
    """Single node in the lineage graph with kind + liveness."""

    table: str
    kind: NodeKind
    operation: str
    live: bool
    task_id: int | None = None
    job_id: int | None = None


class OplogGraph(BaseModel):
    nodes: list[OplogNode] = Field(default_factory=list)
    edges: list[OplogEdge] = Field(default_factory=list)

    @property
    def tables(self) -> set[str]:
        """Return every table that appears in the graph as a node or a kwarg source."""
        return {n.table for n in self.nodes} | {src for n in self.nodes for src in n.kwargs.values() if src}


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
        await ch_client.close()


async def backward_oplog(
    table: str,
    max_depth: int = DEFAULT_MAX_DEPTH,
) -> list[OplogNode]:
    """Trace all upstream operations that produced `table`.

    Uses WITH RECURSIVE for a single SQL round-trip. A `visited` array
    guards against revisiting nodes in diamond-shaped lineage graphs.
    """
    ch_client = get_ch_client()
    result = await ch_client.query(f"""
        WITH RECURSIVE upstream AS (
            SELECT result_table, operation, kwargs,
                   sql_template, task_id, job_id,
                   0 AS depth, [result_table] AS visited
            FROM operation_log
            WHERE result_table = {quote_sql_literal(table)}

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
    max_depth: int = DEFAULT_MAX_DEPTH,
) -> list[OplogNode]:
    """Trace all downstream operations that consumed `table`, including the seed."""
    ch_client = get_ch_client()
    visited: set[str] = set()
    nodes: list[OplogNode] = []

    seed = await ch_client.query(f"""
        SELECT result_table, operation, kwargs,
               sql_template, task_id, job_id
        FROM operation_log
        WHERE result_table = {quote_sql_literal(table)}
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

        placeholders = ", ".join(quote_sql_literal(t) for t in frontier)
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
    max_depth: int = DEFAULT_MAX_DEPTH,
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


def _target_tables(graph: OplogGraph) -> set[str]:
    """Nodes that no other node in the graph consumes."""
    consumed = {src for n in graph.nodes for src in n.kwargs.values() if src}
    return {n.table for n in graph.nodes if n.table not in consumed}


def _input_tables(graph: OplogGraph) -> set[str]:
    """Tables referenced as sources but never produced — plus any ``p_*`` node."""
    produced = {n.table for n in graph.nodes}
    referenced = {src for n in graph.nodes for src in n.kwargs.values() if src}
    inputs = referenced - produced
    inputs |= {n.table for n in graph.nodes if n.table.startswith("p_")}
    return inputs


def classify_nodes(graph: OplogGraph) -> dict[str, NodeKind]:
    """Label every table in the graph as input / intermediate / target."""
    targets = _target_tables(graph)
    inputs = _input_tables(graph)
    kinds: dict[str, NodeKind] = {}
    for table in graph.tables:
        if table in inputs:
            kinds[table] = "input"
        elif table in targets:
            kinds[table] = "target"
        else:
            kinds[table] = "intermediate"
    return kinds
