"""Internal API for lineage queries — AI-independent primitives.

These functions are the building blocks the calling agent (LLM or otherwise)
composes itself: walk the graph, look at schemas, sample data. They run
inside an active ``orch_context(with_ch=True)`` and do not require the
``ai`` extra.
"""

from __future__ import annotations

from aaiclick.ai.agents.lineage_tools import (
    DEFAULT_ROW_LIMIT,
    QueryResult,
    TableSchema,
    describe_table,
    normalize_sql_for_scan,
    run_select,
    validate_scope,
    validate_select_safety,
)
from aaiclick.oplog.lineage import LineageDirection, OplogGraph
from aaiclick.oplog.lineage import oplog_subgraph as _oplog_subgraph

from .errors import Invalid, NotFound


async def oplog_subgraph(
    target_table: str,
    direction: LineageDirection = "backward",
    max_depth: int = 10,
) -> OplogGraph:
    """Return the lineage graph for ``target_table`` in the given direction."""
    return await _oplog_subgraph(target_table, direction=direction, max_depth=max_depth)


async def _lineage_scope(target_table: str, direction: LineageDirection, max_depth: int) -> set[str]:
    """The tables of ``target_table``'s lineage graph — the scope every read
    below is held to.

    Looked up here rather than accepted from the caller, so a token allowed
    to call these tools cannot widen the scope by naming more tables. A
    target no operation produced has no graph and nothing to debug.
    """
    graph = await _oplog_subgraph(target_table, direction=direction, max_depth=max_depth)
    if not graph.nodes:
        raise NotFound(f"{target_table} has no lineage.")
    return graph.tables


async def query_table(
    sql: str,
    target_table: str,
    row_limit: int = DEFAULT_ROW_LIMIT,
    *,
    direction: LineageDirection = "backward",
    max_depth: int = 10,
) -> QueryResult:
    """Run a sandboxed read-only ``SELECT`` against the lineage graph of ``target_table``.

    The scope is the graph ``oplog_subgraph()`` returns for the same
    arguments. Rejects DDL/DML, multi-statement input, a ``SETTINGS``
    clause, and any table reference outside the graph. Auto-injects
    ``LIMIT`` and pins ``max_execution_time``.
    """
    scope_tables = await _lineage_scope(target_table, direction, max_depth)
    scan = normalize_sql_for_scan(sql)
    if err := validate_select_safety(sql, scan=scan):
        raise Invalid(err.message)
    if err := await validate_scope(sql, scope_tables):
        raise Invalid(err.message)
    return await run_select(sql, row_limit, scan=scan)


async def get_table_schema(
    table: str,
    target_table: str,
    *,
    direction: LineageDirection = "backward",
    max_depth: int = 10,
) -> TableSchema:
    """Return columns + types for ``table``, a table in ``target_table``'s lineage graph.

    Raises ``Invalid`` if the table is outside that graph, ``NotFound`` if
    the target has no lineage or ``DESCRIBE TABLE`` fails (e.g. the table
    was dropped after the graph was captured).
    """
    if table not in await _lineage_scope(target_table, direction, max_depth):
        raise Invalid(f"{table} is not in the lineage of {target_table}.")
    try:
        return await describe_table(table)
    except Exception as exc:
        raise NotFound(f"Could not describe {table}: {exc}") from exc
