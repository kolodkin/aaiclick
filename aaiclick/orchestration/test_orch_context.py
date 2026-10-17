"""Tests for ``orch_context`` nesting behavior.

The engine + handler are constructed only at the outermost call;
nested calls reuse them so SQLAlchemy's connection pool actually
gets to amortize handshakes across calls instead of being torn
down per scope.
"""

from __future__ import annotations

import asyncio
import importlib
from unittest.mock import patch

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from aaiclick.data.object_context.ch_client import _ch_client_var, create_ch_client, get_ch_client
from aaiclick.orchestration.execution.db_handler import _db_handler_var
from aaiclick.orchestration.orch_context import OrchLifecycleHandler, orch_context, task_scope
from aaiclick.orchestration.sql_context import _sql_engine_var, create_sql_engine, get_sql_session

# `aaiclick.orchestration.__init__` re-exports the function ``orch_context``
# from the submodule, so attribute-walk based patch paths
# (``"aaiclick.orchestration.orch_context.X"``) hit the function not the
# module. Resolve via ``importlib`` to get the module object directly.
_orch_module = importlib.import_module("aaiclick.orchestration.orch_context")
_ch_module = importlib.import_module("aaiclick.data.object_context.ch_client")


async def test_nested_orch_context_reuses_outer_engine():
    with patch.object(_orch_module, "create_sql_engine", wraps=create_sql_engine) as spy:
        async with orch_context():
            outer_engine = _sql_engine_var.get()
            outer_handler = _db_handler_var.get()
            assert spy.call_count == 1

            async with orch_context():
                assert _sql_engine_var.get() is outer_engine
                assert _db_handler_var.get() is outer_handler
                assert spy.call_count == 1

                async with orch_context():
                    assert _sql_engine_var.get() is outer_engine
                    assert spy.call_count == 1


async def test_nested_orch_context_does_not_dispose_outer_engine():
    with patch.object(AsyncEngine, "dispose", autospec=True) as dispose_spy:
        async with orch_context():
            async with orch_context():
                pass
            assert dispose_spy.call_count == 0
        assert dispose_spy.call_count == 1


async def test_nested_orch_context_reuses_ch_client():
    async with orch_context():
        outer_ch = _ch_client_var.get()
        async with orch_context():
            assert _ch_client_var.get() is outer_ch


async def test_orch_context_opens_ch_only_on_first_query():
    """SQL-only callers never open ClickHouse (no chdb file lock); concurrent
    first queries share one client."""
    with patch.object(_ch_module, "create_ch_client", wraps=create_ch_client) as spy:
        async with orch_context():
            assert spy.call_count == 0
            client = get_ch_client()
            await asyncio.gather(*(client.query("SELECT 1") for _ in range(3)))
            assert spy.call_count == 1


async def test_orch_context_unwinds_when_ch_client_creation_fails():
    """A failing ``create_ch_client`` surfaces on first use and must not leave
    the ContextVars set or the engine undisposed."""
    with (
        patch.object(_ch_module, "create_ch_client", side_effect=RuntimeError("chdb locked")),
        patch.object(AsyncEngine, "dispose", autospec=True) as dispose_spy,
    ):
        with pytest.raises(RuntimeError, match="chdb locked"):
            async with orch_context():
                await get_ch_client().query("SELECT 1")

    assert _sql_engine_var.get() is None
    assert _db_handler_var.get() is None
    assert _ch_client_var.get() is None
    assert dispose_spy.call_count == 1


async def _run_refs(table_name: str) -> set[str]:
    async with get_sql_session() as session:
        rows = await session.execute(text("SELECT run_id FROM table_run_refs WHERE table_name = :t"), {"t": table_name})
        return {row[0] for row in rows}


async def test_lifecycle_loop_survives_a_failing_message():
    """One SQL error must not kill the FIFO consumer: later messages still
    run and ``flush()`` still returns."""
    async with orch_context():
        handler = OrchLifecycleHandler(task_id=1, job_id=1, run_id=7)
        await handler.start()
        try:
            with patch.object(_orch_module, "lookup_advisory_id", side_effect=RuntimeError("db gone")):
                handler.incref("t_first")
                await asyncio.wait_for(handler.flush(), timeout=5)
            handler.incref("t_second")
            await asyncio.wait_for(handler.flush(), timeout=5)
            assert await _run_refs("t_second") == {"7"}
        finally:
            await handler.stop()


async def test_flush_raises_when_lifecycle_loop_is_gone():
    """``flush()`` after the consumer has exited fails fast instead of
    waiting on an event nothing will ever set."""
    async with orch_context():
        handler = OrchLifecycleHandler(task_id=1, job_id=1, run_id=7)
        await handler.start()
        await handler.stop()
        with pytest.raises(RuntimeError, match="lifecycle"):
            await asyncio.wait_for(handler.flush(), timeout=5)


async def test_task_scope_starts_no_lifecycle_loop_when_setup_fails():
    """A failing oplog init must not leave a lifecycle consumer running with nobody to stop it."""
    async with orch_context():
        with (
            patch.object(_orch_module, "init_oplog_tables", side_effect=RuntimeError("ch down")),
            patch.object(OrchLifecycleHandler, "start", autospec=True) as start_spy,
        ):
            with pytest.raises(RuntimeError, match="ch down"):
                async with task_scope(task_id=1, job_id=1, run_id=7):
                    pass

    assert start_spy.call_count == 0
