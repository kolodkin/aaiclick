"""
Tests for OplogCollector: buffering, flushing, and oplog integration.
"""

from __future__ import annotations

import pytest

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.data_context.ch_client import create_ch_client
from aaiclick.oplog.collector import get_oplog_collector, OplogCollector
from aaiclick.orchestration.orch_context import task_scope


async def test_oplog_lifecycle(orch_ctx):
    """Collector is absent outside task_scope, present inside, removed after exit."""
    assert get_oplog_collector() is None

    async with task_scope(task_id=1, job_id=1):
        assert isinstance(get_oplog_collector(), OplogCollector)

    assert get_oplog_collector() is None


async def test_buffer_records_operations(orch_ctx):
    """Buffer captures exact entries with correct structure for a create/concat pipeline."""
    async with task_scope(task_id=1, job_id=1):
        collector = get_oplog_collector()
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([4, 5, 6])
        result = await a.concat(b)

    by_table = {e.result_table: e for e in collector._buffer}
    assert set(by_table) == {a.table, b.table, result.table}

    assert by_table[a.table].operation == "create_from_value"
    assert by_table[b.table].operation == "create_from_value"

    concat_entry = by_table[result.table]
    assert concat_entry.operation == "concat"
    assert set(concat_entry.kwargs.values()) == {a.table, b.table}

    assert set(collector._table_buffer) == {a.table, b.table, result.table}


async def test_flush_on_clean_exit(orch_ctx):
    """On clean exit, operation_log and table_registry are written with correct metadata."""
    async with task_scope(task_id=42, job_id=99):
        obj = await create_object_from_value([5])
        table_name = obj.table

    ch = await create_ch_client()

    row = (await ch.query(
        f"SELECT operation, task_id, job_id FROM operation_log "
        f"WHERE result_table = '{table_name}' LIMIT 1"
    )).result_rows
    assert row and row[0] == ("create_from_value", 42, 99)

    reg = (await ch.query(
        f"SELECT table_name FROM table_registry WHERE table_name = '{table_name}' LIMIT 1"
    )).result_rows
    assert reg


async def test_no_flush_on_exception(orch_ctx):
    """On error, flush() is NOT called — no entries written to operation_log."""
    table_name = None

    with pytest.raises(RuntimeError, match="test error"):
        async with task_scope(task_id=1, job_id=1):
            collector = get_oplog_collector()
            collector.record("some_table", "test_op")
            table_name = "some_table"
            raise RuntimeError("test error")

    # Buffer still has the entry — proves flush was not called (which would not
    # clear the buffer, but entries would appear in CH if flushed)
    assert len(collector._buffer) >= 1

    # Verify nothing was written to ClickHouse
    ch = await create_ch_client()
    rows = (await ch.query(
        f"SELECT count() FROM operation_log WHERE result_table = '{table_name}'"
    )).result_rows
    assert rows[0][0] == 0
