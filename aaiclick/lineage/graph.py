"""
aaiclick.lineage.graph - Lineage graph queries for backward explanation and forward impact.

Provides backward_explain() to trace how a result was produced, and
forward_impact() to trace what downstream operations consumed a table.
Both work on the in-memory buffer of the active LineageCollector.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from ..data.data_context import get_ch_client
from .collector import get_lineage_collector
from .models import OperationLog


@dataclass
class LineageNode:
    """A table in the lineage graph with its producing operation."""

    table: str
    operation: str
    source_tables: list[str] = field(default_factory=list)


@dataclass
class LineageGraph:
    """Directed graph of tables connected by operations."""

    nodes: list[LineageNode] = field(default_factory=list)
    operations: list[OperationLog] = field(default_factory=list)


@dataclass
class LineageContext:
    """Full lineage context — graph + sampled data + schemas.

    Returned by backward_explain() so context is always available
    for LLM consumption without extra calls.
    """

    graph: LineageGraph
    samples: dict[str, list[dict]] = field(default_factory=dict)
    schemas: dict[str, dict[str, str]] = field(default_factory=dict)

    def to_prompt_context(self) -> str:
        """Format everything as text for LLM consumption."""
        lines: list[str] = []

        if self.graph.operations:
            lines.append("## Operation Lineage\n")
            for op in self.graph.operations:
                sources = ", ".join(op.source_tables) if op.source_tables else "(none)"
                lines.append(f"  {sources} → [{op.operation}] → {op.result_table}")

        if self.schemas:
            lines.append("\n## Schemas\n")
            for table, cols in self.schemas.items():
                col_str = ", ".join(f"{k}: {v}" for k, v in cols.items())
                lines.append(f"  {table}: {{{col_str}}}")

        if self.samples:
            lines.append("\n## Sample Data\n")
            for table, rows in self.samples.items():
                lines.append(f"  {table}:")
                for row in rows[:5]:
                    lines.append(f"    {row}")

        return "\n".join(lines)


def _backward_trace(
    table: str,
    operations: list[OperationLog],
    max_depth: int = 10,
    _visited: Optional[set[str]] = None,
) -> list[OperationLog]:
    """Trace all upstream operations that produced `table`.

    Recursively finds operations where result_table matches, then
    recurses on each source_table.
    """
    if _visited is None:
        _visited = set()

    if table in _visited or max_depth <= 0:
        return []

    _visited.add(table)
    result: list[OperationLog] = []

    for op in operations:
        if op.result_table == table:
            result.append(op)
            for source in op.source_tables:
                result.extend(
                    _backward_trace(source, operations, max_depth - 1, _visited)
                )

    return result


def _forward_trace(
    table: str,
    operations: list[OperationLog],
    max_depth: int = 10,
    _visited: Optional[set[str]] = None,
) -> list[OperationLog]:
    """Trace all downstream operations that consumed `table`.

    Recursively finds operations where table appears in source_tables,
    then recurses on result_table.
    """
    if _visited is None:
        _visited = set()

    if table in _visited or max_depth <= 0:
        return []

    _visited.add(table)
    result: list[OperationLog] = []

    for op in operations:
        if table in op.source_tables:
            result.append(op)
            result.extend(
                _forward_trace(op.result_table, operations, max_depth - 1, _visited)
            )

    return result


def _build_graph(operations: list[OperationLog]) -> LineageGraph:
    """Build a LineageGraph from a list of OperationLog entries."""
    seen_tables: set[str] = set()
    nodes: list[LineageNode] = []

    for op in operations:
        if op.result_table not in seen_tables:
            seen_tables.add(op.result_table)
            nodes.append(
                LineageNode(
                    table=op.result_table,
                    operation=op.operation,
                    source_tables=list(op.source_tables),
                )
            )

    return LineageGraph(nodes=nodes, operations=operations)


async def _sample_nodes(
    graph: LineageGraph,
    limit: int = 5,
) -> dict[str, list[dict]]:
    """Fetch sample rows from each table in the graph."""
    samples: dict[str, list[dict]] = {}
    ch_client = get_ch_client()

    tables_to_sample: set[str] = set()
    for node in graph.nodes:
        tables_to_sample.add(node.table)
        for src in node.source_tables:
            tables_to_sample.add(src)

    for table in sorted(tables_to_sample):
        try:
            # Get column names first (system.columns works with both chdb and remote)
            cols_result = await ch_client.query(
                f"SELECT name FROM system.columns "
                f"WHERE table = '{table}' ORDER BY position"
            )
            col_names = [row[0] for row in cols_result.result_rows]
            if not col_names:
                continue

            result = await ch_client.query(
                f"SELECT * FROM {table} ORDER BY aai_id LIMIT {limit}"
            )
            if result.result_rows:
                samples[table] = [
                    dict(zip(col_names, row)) for row in result.result_rows
                ]
        except Exception:
            pass

    return samples


async def _get_schemas(
    graph: LineageGraph,
) -> dict[str, dict[str, str]]:
    """Get column schemas for each table in the graph."""
    schemas: dict[str, dict[str, str]] = {}
    ch_client = get_ch_client()

    tables: set[str] = set()
    for node in graph.nodes:
        tables.add(node.table)
        for src in node.source_tables:
            tables.add(src)

    for table in sorted(tables):
        try:
            result = await ch_client.query(
                f"SELECT name, type FROM system.columns "
                f"WHERE table = '{table}' ORDER BY position"
            )
            if result.result_rows:
                schemas[table] = {row[0]: row[1] for row in result.result_rows}
        except Exception:
            pass

    return schemas


async def backward_explain(
    table: str,
    max_depth: int = 10,
    sample_limit: int = 10,
) -> LineageContext:
    """Trace upstream lineage and return full context (graph, samples, schemas).

    This is the public API — always returns context ready for LLM consumption.
    Internally calls _backward_trace() for the raw operation log traversal,
    then enriches with sample data and schema info.

    Raises:
        RuntimeError: If no active LineageCollector (lineage not enabled).
    """
    collector = get_lineage_collector()
    if collector is None:
        raise RuntimeError(
            "No active LineageCollector — enable lineage with "
            "data_context(lineage=True)"
        )

    ops = _backward_trace(table, collector.operations, max_depth=max_depth)
    graph = _build_graph(ops)
    samples = await _sample_nodes(graph, limit=sample_limit)
    schemas = await _get_schemas(graph)
    return LineageContext(graph=graph, samples=samples, schemas=schemas)


async def forward_impact(
    table: str,
    max_depth: int = 10,
) -> list[OperationLog]:
    """Trace all downstream operations that consumed `table`.

    Returns the list of OperationLog entries for downstream operations.

    Raises:
        RuntimeError: If no active LineageCollector (lineage not enabled).
    """
    collector = get_lineage_collector()
    if collector is None:
        raise RuntimeError(
            "No active LineageCollector — enable lineage with "
            "data_context(lineage=True)"
        )

    return _forward_trace(table, collector.operations, max_depth=max_depth)
