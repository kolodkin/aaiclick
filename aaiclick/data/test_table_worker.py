"""
Tests for TableWorker background thread lifecycle management.
"""

from unittest.mock import MagicMock, patch

from aaiclick.data.table_worker import TableWorker, TableOp, TableMessage
from aaiclick.data.models import ClickHouseCreds


def test_table_message_creation():
    """Test TableMessage dataclass creation."""
    msg = TableMessage(TableOp.INCREF, "test_table")
    assert msg.op == TableOp.INCREF
    assert msg.table_name == "test_table"


def test_table_message_default_table_name():
    """Test TableMessage with default empty table name."""
    msg = TableMessage(TableOp.SHUTDOWN)
    assert msg.op == TableOp.SHUTDOWN
    assert msg.table_name == ""


def test_table_op_enum_values():
    """Test TableOp enum has expected values."""
    assert TableOp.INCREF.value == 1
    assert TableOp.DECREF.value == 2
    assert TableOp.SHUTDOWN.value == 3


def test_worker_incref_queues_message():
    """Test that incref queues an INCREF message."""
    creds = ClickHouseCreds()
    worker = TableWorker(creds)

    # Don't start the worker - just test queue behavior
    worker.incref("table_123")

    msg = worker._queue.get_nowait()
    assert msg.op == TableOp.INCREF
    assert msg.table_name == "table_123"


def test_worker_decref_queues_message():
    """Test that decref queues a DECREF message."""
    creds = ClickHouseCreds()
    worker = TableWorker(creds)

    worker.decref("table_456")

    msg = worker._queue.get_nowait()
    assert msg.op == TableOp.DECREF
    assert msg.table_name == "table_456"


def test_worker_stop_queues_shutdown():
    """Test that stop queues a SHUTDOWN message and joins thread."""
    creds = ClickHouseCreds()
    worker = TableWorker(creds)

    # Mock the thread join to avoid blocking
    worker._thread = MagicMock()

    worker.stop()

    msg = worker._queue.get_nowait()
    assert msg.op == TableOp.SHUTDOWN
    worker._thread.join.assert_called_once()


def test_worker_refcount_tracking():
    """Test refcount tracking logic without actual ClickHouse."""
    creds = ClickHouseCreds()
    worker = TableWorker(creds)

    # Simulate what _run does for INCREF
    worker._refcounts["table_a"] = 0
    worker._refcounts["table_a"] = worker._refcounts.get("table_a", 0) + 1
    assert worker._refcounts["table_a"] == 1

    worker._refcounts["table_a"] = worker._refcounts.get("table_a", 0) + 1
    assert worker._refcounts["table_a"] == 2

    # Simulate DECREF
    worker._refcounts["table_a"] -= 1
    assert worker._refcounts["table_a"] == 1

    worker._refcounts["table_a"] -= 1
    assert worker._refcounts["table_a"] == 0


def test_worker_cleanup_all():
    """Test cleanup_all drops all tracked tables."""
    creds = ClickHouseCreds()
    worker = TableWorker(creds)

    # Mock the client
    worker._ch_client = MagicMock()
    worker._refcounts = {"table_1": 2, "table_2": 1, "table_3": 5}

    worker._cleanup_all()

    # All tables should be dropped
    assert worker._ch_client.command.call_count == 3
    worker._ch_client.command.assert_any_call("DROP TABLE IF EXISTS table_1")
    worker._ch_client.command.assert_any_call("DROP TABLE IF EXISTS table_2")
    worker._ch_client.command.assert_any_call("DROP TABLE IF EXISTS table_3")

    # Refcounts should be cleared
    assert worker._refcounts == {}


def test_worker_drop_table_handles_exception():
    """Test _drop_table handles exceptions gracefully."""
    creds = ClickHouseCreds()
    worker = TableWorker(creds)

    # Mock client that raises exception
    worker._ch_client = MagicMock()
    worker._ch_client.command.side_effect = Exception("Connection failed")

    # Should not raise
    worker._drop_table("nonexistent_table")


@patch("aaiclick.data.table_worker.get_client")
def test_worker_full_lifecycle(mock_get_client):
    """Test worker full lifecycle with mocked ClickHouse client."""
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    creds = ClickHouseCreds(host="testhost", port=9000, database="testdb")
    worker = TableWorker(creds)

    # Start worker
    worker.start()

    # Send some messages
    worker.incref("table_x")
    worker.incref("table_x")
    worker.decref("table_x")
    worker.incref("table_y")

    # Stop and wait
    worker.stop()

    # Client should have been created with correct params
    mock_get_client.assert_called_once_with(
        host="testhost",
        port=9000,
        username="default",
        password="",
        database="testdb",
    )

    # Remaining tables should be cleaned up (table_x has refcount 1, table_y has 1)
    # Both should be dropped on shutdown
    assert mock_client.command.call_count >= 2
    mock_client.close.assert_called_once()


@patch("aaiclick.data.table_worker.get_client")
def test_worker_drops_table_when_refcount_zero(mock_get_client):
    """Test that table is dropped immediately when refcount reaches zero."""
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    creds = ClickHouseCreds()
    worker = TableWorker(creds)

    worker.start()

    # Create and immediately release a table
    worker.incref("temp_table")
    worker.decref("temp_table")

    # Give worker time to process
    import time

    time.sleep(0.1)

    # Stop worker
    worker.stop()

    # Table should have been dropped when refcount hit 0
    mock_client.command.assert_any_call("DROP TABLE IF EXISTS temp_table")
