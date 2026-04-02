"""
Tests for AsyncTableWorker async task lifecycle management.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from aaiclick.data.data_context.table_worker import AsyncTableWorker, TableOp, TableMessage


def _make_mock_client() -> AsyncMock:
    client = AsyncMock()
    client.command = AsyncMock(return_value=None)
    return client


def test_worker_incref_schedules_message():
    """incref schedules an INCREF message via call_soon_threadsafe."""
    client = _make_mock_client()
    worker = AsyncTableWorker(client)

    mock_loop = MagicMock()
    worker._loop = mock_loop

    worker.incref("table_123")

    mock_loop.call_soon_threadsafe.assert_called_once_with(
        worker._queue.put_nowait, TableMessage(TableOp.INCREF, "table_123")
    )


def test_worker_decref_schedules_message():
    """decref schedules a DECREF message via call_soon_threadsafe."""
    client = _make_mock_client()
    worker = AsyncTableWorker(client)

    mock_loop = MagicMock()
    worker._loop = mock_loop

    worker.decref("table_456")

    mock_loop.call_soon_threadsafe.assert_called_once_with(
        worker._queue.put_nowait, TableMessage(TableOp.DECREF, "table_456")
    )


def test_worker_incref_noop_before_start():
    """incref is a no-op before start() (loop not set)."""
    worker = AsyncTableWorker(_make_mock_client())
    worker.incref("table_123")


def test_worker_decref_noop_before_start():
    """decref is a no-op before start() (loop not set)."""
    worker = AsyncTableWorker(_make_mock_client())
    worker.decref("table_456")


async def test_worker_refcount_drops_only_at_zero():
    """Table is dropped only when refcount reaches zero, not before."""
    client = _make_mock_client()
    worker = AsyncTableWorker(client)
    await worker.start()

    worker.incref("table_a")
    worker.incref("table_a")
    worker.decref("table_a")  # refcount → 1, no drop yet

    await asyncio.sleep(0)
    await asyncio.sleep(0)

    client.command.assert_not_called()

    worker.decref("table_a")  # refcount → 0, should drop

    await asyncio.sleep(0)
    await asyncio.sleep(0)

    client.command.assert_called_once_with("DROP TABLE IF EXISTS table_a")

    await worker.stop()


async def test_worker_start_creates_task():
    """start() creates an asyncio Task and stores the running loop."""
    client = _make_mock_client()
    worker = AsyncTableWorker(client)

    await worker.start()

    assert worker._loop is asyncio.get_running_loop()
    assert worker._task is not None
    assert not worker._task.done()

    await worker.stop()


async def test_worker_full_lifecycle():
    """Worker processes incref/decref and drops tables when refcount hits zero."""
    client = _make_mock_client()
    worker = AsyncTableWorker(client)

    await worker.start()

    worker.incref("table_x")
    worker.incref("table_x")
    worker.decref("table_x")
    worker.incref("table_y")

    await worker.stop()

    # table_x (refcount 1) and table_y (refcount 1) both cleaned up on shutdown
    dropped = {call.args[0] for call in client.command.call_args_list}
    assert dropped == {
        "DROP TABLE IF EXISTS table_x",
        "DROP TABLE IF EXISTS table_y",
    }


async def test_worker_drops_table_when_refcount_zero():
    """Table is dropped immediately when refcount reaches zero."""
    client = _make_mock_client()
    worker = AsyncTableWorker(client)

    await worker.start()

    worker.incref("temp_table")
    worker.decref("temp_table")

    # call_soon_threadsafe callbacks need one iteration to enqueue;
    # worker task needs another to process them
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    client.command.assert_called_once_with("DROP TABLE IF EXISTS temp_table")

    await worker.stop()


async def test_worker_cleanup_all():
    """_cleanup_all drops all tracked tables."""
    client = _make_mock_client()
    worker = AsyncTableWorker(client)
    worker._refcounts = {"table_1": 2, "table_2": 1, "table_3": 5}

    await worker._cleanup_all()

    assert client.command.call_count == 3
    client.command.assert_any_call("DROP TABLE IF EXISTS table_1")
    client.command.assert_any_call("DROP TABLE IF EXISTS table_2")
    client.command.assert_any_call("DROP TABLE IF EXISTS table_3")
    assert worker._refcounts == {}


async def test_worker_drop_table_handles_exception():
    """_drop_table handles exceptions gracefully."""
    client = _make_mock_client()
    client.command.side_effect = Exception("Connection failed")
    worker = AsyncTableWorker(client)

    await worker._drop_table("nonexistent_table")


async def test_worker_skips_persistent_tables_on_cleanup():
    """Persistent tables (p_ prefix) are not dropped during cleanup."""
    client = _make_mock_client()
    worker = AsyncTableWorker(client)
    worker._refcounts = {"p_mydata": 1, "temp_table": 1}

    await worker._cleanup_all()

    dropped = {call.args[0] for call in client.command.call_args_list}
    assert dropped == {"DROP TABLE IF EXISTS temp_table"}
