"""Internal API for lineage queries — AI-independent primitives.

These functions are the building blocks the calling agent (LLM or otherwise)
composes itself: walk the graph, look at schemas, sample data. They run
inside an active ``orch_context(with_ch=True)`` and do not require the
``ai`` extra.

Turnkey LLM agents (``explain_lineage`` / ``debug_result``) live in
``internal_api.lineage_ai`` so callers without the ``ai`` extra installed
can still import this module.
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


async def query_table(
    sql: str,
    scope_tables: list[str],
    row_limit: int = DEFAULT_ROW_LIMIT,
) -> QueryResult:
    """Run a sandboxed read-only ``SELECT`` against tables in ``scope_tables``.

    Rejects DDL/DML, multi-statement input, and any ``t_*`` / ``p_*`` token
    referencing a table outside ``scope_tables``. Auto-injects ``LIMIT`` and
    pins ``max_execution_time``. Callers should populate ``scope_tables``
    from a prior ``oplog_subgraph()`` (use ``OplogGraph.tables``).
    """
    scan = normalize_sql_for_scan(sql)
    if err := validate_select_safety(sql, scan=scan):
        raise Invalid(err.message)
    if err := validate_scope(sql, set(scope_tables), scan=scan):
        raise Invalid(err.message)
    return await run_select(sql, row_limit, scan=scan)


async def get_table_schema(table: str, scope_tables: list[str]) -> TableSchema:
    """Return columns + types for ``table`` (must be in ``scope_tables``).

    Raises ``Invalid`` if the table is outside the supplied scope, ``NotFound``
    if ``DESCRIBE TABLE`` fails (e.g. the table was dropped after the lineage
    graph was captured).
    """
    if table not in scope_tables:
        raise Invalid(f"{table} is not in scope.")
    try:
        return await describe_table(table)
    except Exception as exc:
        raise NotFound(f"Could not describe {table}: {exc}") from exc
