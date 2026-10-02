"""Internal API for lineage queries — the primitives an MCP client composes.

Walk the graph, see which tables are still live, look at schemas, sample
data. The calling agent (Claude Code, Codex, any MCP client) forms and
verifies hypotheses itself. These run inside an active
``orch_context(with_ch=True)``.
"""

from __future__ import annotations

from aaiclick.oplog.lineage import DEFAULT_MAX_DEPTH, GraphNode, LineageDirection, OplogGraph
from aaiclick.oplog.lineage import oplog_subgraph as _oplog_subgraph
from aaiclick.oplog.query_sandbox import (
    DEFAULT_ROW_LIMIT,
    QueryResult,
    SandboxError,
    TableSchema,
    describe_table,
    liveness,
    sandboxed_select,
    validate_select_safety,
)

from .errors import Invalid, NotFound


async def oplog_subgraph(
    target_table: str,
    direction: LineageDirection = "backward",
    max_depth: int = DEFAULT_MAX_DEPTH,
) -> OplogGraph:
    """Return the lineage graph for ``target_table`` in the given direction."""
    return await _oplog_subgraph(target_table, direction=direction, max_depth=max_depth)


async def _lineage_graph(target_table: str, *, direction: LineageDirection, max_depth: int) -> OplogGraph:
    """``target_table``'s lineage graph — the scope every read below is held to.

    Looked up here rather than accepted from the caller, so a token allowed
    to call these tools cannot widen the scope by naming more tables. A
    target no operation produced has no graph and nothing to debug.
    """
    graph = await oplog_subgraph(target_table, direction=direction, max_depth=max_depth)
    if not graph.nodes:
        raise NotFound(f"{target_table} has no lineage.")
    return graph


async def list_graph_nodes(
    target_table: str,
    *,
    direction: LineageDirection = "backward",
    max_depth: int = DEFAULT_MAX_DEPTH,
) -> list[GraphNode]:
    """Every table in ``target_table``'s lineage graph with its kind and liveness.

    ``live`` is whether the table currently exists in ClickHouse; a dropped
    intermediate is still listed, so the caller can tell "gone" from "never
    in the graph". Raises ``NotFound`` if the target has no lineage.
    """
    graph = await _lineage_graph(target_table, direction=direction, max_depth=max_depth)
    return graph.graph_nodes(await liveness(graph.tables))


async def query_table(
    sql: str,
    target_table: str,
    row_limit: int = DEFAULT_ROW_LIMIT,
    *,
    direction: LineageDirection = "backward",
    max_depth: int = DEFAULT_MAX_DEPTH,
) -> QueryResult:
    """Run a sandboxed read-only ``SELECT`` against the lineage graph of ``target_table``.

    The scope is the graph ``oplog_subgraph()`` returns for the same
    arguments. Rejects DDL/DML, multi-statement input, a ``SETTINGS``
    clause, and any table reference outside the graph. Caps rows and pins
    ``max_execution_time``. The read-only check comes first so a rejected
    statement costs no lineage query.
    """
    try:
        validate_select_safety(sql)
        graph = await _lineage_graph(target_table, direction=direction, max_depth=max_depth)
        return await sandboxed_select(sql, graph.tables, row_limit)
    except SandboxError as exc:
        raise Invalid(str(exc)) from exc


async def get_table_schema(
    table: str,
    target_table: str,
    *,
    direction: LineageDirection = "backward",
    max_depth: int = DEFAULT_MAX_DEPTH,
) -> TableSchema:
    """Return columns + types for ``table``, a table in ``target_table``'s lineage graph.

    Raises ``Invalid`` if the table is outside that graph, ``NotFound`` if
    the target has no lineage or ``DESCRIBE TABLE`` fails (e.g. the table
    was dropped after the graph was captured).
    """
    graph = await _lineage_graph(target_table, direction=direction, max_depth=max_depth)
    if table not in graph.tables:
        raise Invalid(f"{table} is not in the lineage of {target_table}.")
    try:
        return await describe_table(table)
    except Exception as exc:
        raise NotFound(f"Could not describe {table}: {exc}") from exc
