"""
Tests for AsyncTableWorker async task lifecycle management.
"""

import asyncio
from unittest.mock import AsyncMock

from aaiclick.data.data_context import get_ch_client
from aaiclick.data.data_context.table_worker import AsyncTableWorker
from aaiclick.testing import create_ch_tables, list_ch_tables


def test_worker_incref_noop_before_start():
    """incref is a no-op before start() (loop not set)."""
    worker = AsyncTableWorker(AsyncMock())
    worker.incref("table_123")


def test_worker_decref_noop_before_start():
    """decref is a no-op before start() (loop not set)."""
    worker = AsyncTableWorker(AsyncMock())
    worker.decref("table_456")


async def test_worker_refcount_drops_only_at_zero(ctx):
    """Table is dropped only when refcount reaches zero, not before."""
    ch = get_ch_client()
    await create_ch_tables(ch, "t_a")
    worker = AsyncTableWorker(ch)
    await worker.start()

    worker.incref("t_a")
    worker.incref("t_a")
    worker.decref("t_a")  # refcount → 1, no drop yet
    await worker.flush()

    assert "t_a" in await list_ch_tables(ch)

    worker.decref("t_a")  # refcount → 0, should drop
    await worker.flush()

    assert "t_a" not in await list_ch_tables(ch)

    await worker.stop()


async def test_worker_stop_drops_tracked_tables(ctx):
    """stop() drops every table still holding a reference."""
    ch = get_ch_client()
    await create_ch_tables(ch, "t_x", "t_y", "t_untracked")
    worker = AsyncTableWorker(ch)
    await worker.start()

    worker.incref("t_x")
    worker.incref("t_x")
    worker.decref("t_x")
    worker.incref("t_y")

    await worker.stop()

    assert await list_ch_tables(ch) & {"t_x", "t_y", "t_untracked"} == {"t_untracked"}


async def test_worker_survives_failed_drop():
    """A failed DROP does not stop the worker from processing later messages."""
    client = AsyncMock()
    client.command.side_effect = RuntimeError("Connection failed")
    worker = AsyncTableWorker(client)
    await worker.start()

    worker.incref("t_first")
    worker.decref("t_first")
    worker.incref("t_second")
    worker.decref("t_second")
    await asyncio.wait_for(worker.flush(), timeout=5)  # a dead loop never answers the flush

    assert client.command.call_count == 2

    await worker.stop()


async def test_worker_never_drops_persistent_tables(ctx):
    """``p_*`` and ``j_<id>_*`` tables survive both refcount zero and shutdown."""
    ch = get_ch_client()
    persistent = {"p_released", "j_1_released", "p_held", "j_1_held"}
    await create_ch_tables(ch, *persistent, "t_held")
    worker = AsyncTableWorker(ch)
    await worker.start()

    for table_name in ("p_released", "j_1_released"):
        worker.incref(table_name)
        worker.decref(table_name)
    for table_name in ("p_held", "j_1_held", "t_held"):
        worker.incref(table_name)

    await worker.stop()

    assert await list_ch_tables(ch) & (persistent | {"t_held"}) == persistent
