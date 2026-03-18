"""
aaiclick.lineage.graph - Lineage graph traversal and formatting.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from aaiclick.data.ch_client import ChClient


def _to_dict(kwargs_raw: Any) -> dict[str, str]:
    """Normalize kwargs from ClickHouse Map column.

    chdb returns Map(String, String) as a list of (key, value) tuples;
    clickhouse-connect returns a dict. Accept both.
    """
    if isinstance(kwargs_raw, dict):
        return kwargs_raw
    return dict(kwargs_raw)


@dataclass
class LineageNode:
    table: str
    operation: str
    args: list[str]
    kwargs: dict[str, str]
    sql_template: str | None
    task_id: int | None
    job_id: int | None


@dataclass
class LineageEdge:
    source: str
    target: str
    operation: str


@dataclass
class LineageGraph:
    nodes: list[LineageNode] = field(default_factory=list)
    edges: list[LineageEdge] = field(default_factory=list)

    def to_prompt_context(self) -> str:
        """Format the lineage graph as plain text for LLM consumption."""
        if not self.nodes:
            return "No lineage information found."

        lines: list[str] = ["Data lineage graph:", ""]
        for node in self.nodes:
            sources = list(node.args) + list(node.kwargs.values())
            src_str = ", ".join(sources) if sources else "(none)"
            lines.append(f"  {node.table}")
            lines.append(f"    operation : {node.operation}")
            lines.append(f"    inputs    : {src_str}")
            if node.sql_template:
                lines.append(f"    sql       : {node.sql_template}")
            lines.append("")

        lines.append("Edges:")
        for edge in self.edges:
            lines.append(f"  {edge.source} -> {edge.target} [{edge.operation}]")

        return "\n".join(lines)


async def backward_lineage(
    table: str,
    ch_client: ChClient,
    max_depth: int = 10,
) -> list[LineageNode]:
    """Trace all upstream operations that produced `table`.

    Returns nodes in BFS order starting from `table`.
    """
    visited: set[str] = set()
    frontier = [table]
    nodes: list[LineageNode] = []

    for _ in range(max_depth):
        if not frontier:
            break

        placeholders = ", ".join(f"'{t}'" for t in frontier)
        result = await ch_client.query(f"""
            SELECT result_table, operation, args, kwargs, sql_template, task_id, job_id
            FROM operation_log
            WHERE result_table IN ({placeholders})
            ORDER BY created_at DESC
        """)

        next_frontier: list[str] = []
        for row in result.result_rows:
            result_table, operation, args, kwargs, sql_template, task_id, job_id = row
            if result_table in visited:
                continue
            visited.add(result_table)
            node = LineageNode(
                table=result_table,
                operation=operation,
                args=list(args),
                kwargs=_to_dict(kwargs),
                sql_template=sql_template,
                task_id=task_id,
                job_id=job_id,
            )
            nodes.append(node)
            for src in list(args) + list(_to_dict(kwargs).values()):
                if src and src not in visited:
                    next_frontier.append(src)

        frontier = next_frontier

    return nodes


async def forward_lineage(
    table: str,
    ch_client: ChClient,
    max_depth: int = 10,
) -> list[LineageNode]:
    """Trace all downstream operations that consumed `table`.

    Returns nodes in BFS order starting from operations that used `table`.
    """
    visited: set[str] = set()
    frontier = [table]
    nodes: list[LineageNode] = []

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
            node = LineageNode(
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


async def lineage_subgraph(
    table: str,
    ch_client: ChClient,
    direction: str = "backward",
    max_depth: int = 10,
) -> LineageGraph:
    """Return a structured LineageGraph for visualization or AI context."""
    if direction == "backward":
        nodes = await backward_lineage(table, ch_client, max_depth)
    elif direction == "forward":
        nodes = await forward_lineage(table, ch_client, max_depth)
    else:
        raise ValueError(f"direction must be 'backward' or 'forward', got '{direction}'")

    edges: list[LineageEdge] = []
    for node in nodes:
        for src in node.args:
            edges.append(LineageEdge(source=src, target=node.table, operation=node.operation))
        for src in node.kwargs.values():
            edges.append(LineageEdge(source=src, target=node.table, operation=node.operation))

    return LineageGraph(nodes=nodes, edges=edges)
