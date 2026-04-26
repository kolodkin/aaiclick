"""Internal API for lineage queries and AI agents.

Each function runs inside an active ``orch_context(with_ch=True)`` and
reads the ClickHouse client via ``get_ch_client``.

Two layers of surface here:

- **Primitives** — ``oplog_subgraph``, ``query_table``, ``get_table_schema`` —
  the building blocks the calling agent (LLM or otherwise) composes itself.
  Exposed via MCP so an MCP client can run a debug loop entirely on its
  own, without invoking a second LLM (Ollama).
- **Turnkey AI agents** — ``explain_lineage``, ``debug_result`` — wrap an
  Ollama-backed LLM and return a finished answer. Useful from CLI or for
  callers who don't want to drive their own loop. Not exposed via MCP for
  the reasons above.
"""

from __future__ import annotations

from aaiclick.ai.agents.debug_agent import debug_result as _debug_result
from aaiclick.ai.agents.lineage_agent import explain_lineage as _explain_lineage
from aaiclick.ai.agents.lineage_tools import (
    DEFAULT_ROW_LIMIT,
    QueryResult,
    TableSchema,
    describe_table,
    run_select,
    validate_scope,
    validate_select_safety,
)
from aaiclick.oplog.lineage import LineageDirection, OplogGraph
from aaiclick.oplog.lineage import oplog_subgraph as _oplog_subgraph
from aaiclick.oplog.view_models import LineageAnswer

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
    if err := validate_select_safety(sql):
        raise Invalid(err.message)
    if err := validate_scope(sql, set(scope_tables)):
        raise Invalid(err.message)
    return await run_select(sql, row_limit)


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


async def explain_lineage(target_table: str, question: str | None = None) -> LineageAnswer:
    """Trace and explain how ``target_table`` was produced (turnkey LLM agent).

    Calls Ollama internally — prefer the primitives above for MCP / programmatic
    use; this is a CLI / convenience surface only.
    """
    text = await _explain_lineage(target_table, question=question)
    return LineageAnswer(text=text)


async def debug_result(
    target_table: str,
    question: str,
    max_iterations: int = 10,
) -> LineageAnswer:
    """Run the lineage debug agent's tool loop on ``target_table`` (turnkey LLM agent).

    Calls Ollama internally and runs its own tool loop. Prefer the primitives
    above for MCP / programmatic use.
    """
    text = await _debug_result(target_table, question=question, max_iterations=max_iterations)
    return LineageAnswer(text=text)
