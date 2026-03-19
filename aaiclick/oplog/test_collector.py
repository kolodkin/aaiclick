"""
Tests for OplogCollector: buffering, flushing, and oplog integration.
"""

from __future__ import annotations

import pytest

from aaiclick.data.data_context import data_context, create_object_from_value
from aaiclick.data.ch_client import create_ch_client
from aaiclick.oplog.collector import get_oplog_collector, OplogCollector


async def test_oplog_lifecycle():
    """Collector is absent by default, present with oplog=True, removed after exit."""
    async with data_context():
        assert get_oplog_collector() is None

    async with data_context(oplog=True):
        assert isinstance(get_oplog_collector(), OplogCollector)

    assert get_oplog_collector() is None


async def test_buffer_records_operations():
    """Buffer captures exact entries with correct structure for a create/concat pipeline."""
    async with data_context(oplog=True):
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
    assert set(concat_entry.args) == {a.table, b.table}

    assert collector._table_buffer == {a.table, b.table, result.table}


async def test_flush_on_clean_exit():
    """On clean exit, operation_log and table_registry are written with correct metadata."""
    async with data_context(oplog=OplogCollector(task_id=42, job_id=99)):
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


async def test_no_flush_on_exception():
    """On error, flush() is NOT called — no entries written to operation_log."""
    flushed = False

    class TrackingCollector(OplogCollector):
        async def flush(self):
            nonlocal flushed
            flushed = True
            await super().flush()

    with pytest.raises(RuntimeError, match="test error"):
        async with data_context(oplog=TrackingCollector()):
            raise RuntimeError("test error")

    assert not flushed
