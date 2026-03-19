"""
aaiclick.oplog.lineage - Oplog graph traversal (backward and forward lineage).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

from aaiclick.data.ch_client import create_ch_client, get_ch_client, _ch_client_var


def _to_dict(kwargs_raw: Any) -> dict[str, str]:
    """Normalize kwargs from ClickHouse Map column.

    chdb returns Map(String, String) as a list of (key, value) tuples;
    clickhouse-connect returns a dict. Accept both.
    """
    if isinstance(kwargs_raw, dict):
        return kwargs_raw
    return dict(kwargs_raw)


@dataclass
class OplogNode:
    table: str
    operation: str
    args: list[str]
    kwargs: dict[str, str]
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
            SELECT result_table, operation, args, kwargs, sql_template, task_id, job_id,
                   0 AS depth, [result_table] AS visited
            FROM operation_log
            WHERE result_table = '{table_escaped}'

            UNION ALL

            SELECT ol.result_table, ol.operation, ol.args, ol.kwargs,
                   ol.sql_template, ol.task_id, ol.job_id,
                   u.depth + 1, arrayConcat(u.visited, [ol.result_table])
            FROM upstream u
            INNER JOIN operation_log ol
                ON hasAny(arrayConcat(u.args, mapValues(u.kwargs)), [ol.result_table])
            WHERE u.depth < {max_depth}
              AND NOT has(u.visited, ol.result_table)
        )
        SELECT DISTINCT result_table, operation, args, kwargs, sql_template, task_id, job_id
        FROM upstream
    """)

    return [
        OplogNode(
            table=result_table,
            operation=operation,
            args=list(args),
            kwargs=_to_dict(kwargs_raw),
            sql_template=sql_template,
            task_id=task_id,
            job_id=job_id,
        )
        for result_table, operation, args, kwargs_raw, sql_template, task_id, job_id
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
            SELECT result_table, operation, args, kwargs, sql_template, task_id, job_id
            FROM operation_log
            WHERE hasAny(args, [{placeholders}])
               OR arrayExists(v -> v IN ({placeholders}), mapValues(kwargs))
            ORDER BY created_at ASC
        """)

        next_frontier: list[str] = []
        for row in result.result_rows:
            result_table, operation, args, kwargs, sql_template, task_id, job_id = row
            if result_table in visited:
                continue
            visited.add(result_table)
            node = OplogNode(
                table=result_table,
                operation=operation,
                args=list(args),
                kwargs=_to_dict(kwargs),
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
        for src in node.args:
            edges.append(OplogEdge(source=src, target=node.table, operation=node.operation))
        for src in node.kwargs.values():
            edges.append(OplogEdge(source=src, target=node.table, operation=node.operation))

    return OplogGraph(nodes=nodes, edges=edges)
