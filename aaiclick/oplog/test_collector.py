"""
Tests for OplogCollector: buffering, flushing, and oplog_context integration.
"""

from __future__ import annotations

import pytest

from aaiclick.data.data_context import data_context, create_object_from_value
from aaiclick.data.ch_client import create_ch_client
from aaiclick.oplog.collector import get_oplog_collector, OplogCollector
from aaiclick.oplog.triage import oplog_context


async def test_oplog_disabled_by_default():
    """No oplog_context produces no collector and no operation_log entries."""
    async with data_context():
        assert get_oplog_collector() is None
        obj = await create_object_from_value([1, 2, 3])
        assert get_oplog_collector() is None
        _ = obj


async def test_oplog_collector_created_when_enabled():
    """oplog_context installs an OplogCollector in the ContextVar."""
    async with data_context():
        async with oplog_context():
            collector = get_oplog_collector()
            assert collector is not None
            assert isinstance(collector, OplogCollector)


async def test_oplog_collector_removed_after_context():
    """Collector ContextVar is reset after oplog_context exits."""
    async with data_context():
        async with oplog_context():
            assert get_oplog_collector() is not None
        assert get_oplog_collector() is None


async def test_create_from_value_recorded():
    """create_object_from_value records a create_from_value entry."""
    async with data_context():
        async with oplog_context() as _:
            collector = get_oplog_collector()
            obj = await create_object_from_value([10, 20, 30])
            assert any(e.operation == "create_from_value" and e.result_table == obj.table
                       for e in collector._buffer)


async def test_table_registry_recorded_on_create():
    """Every non-persistent table created is added to table_registry buffer."""
    async with data_context():
        async with oplog_context():
            collector = get_oplog_collector()
            obj = await create_object_from_value([1, 2])
            assert obj.table in collector._table_buffer


async def test_operator_recorded():
    """Binary operators record left/right kwargs."""
    from aaiclick.data import create_object_from_value as cofv
    async with data_context():
        async with oplog_context():
            collector = get_oplog_collector()
            a = await cofv([1, 2, 3])
            b = await cofv([4, 5, 6])
            result = await (a + b)
            op_events = [e for e in collector._buffer if e.operation == "+"]
            assert op_events, "Expected '+' operation event"
            ev = op_events[-1]
            assert ev.kwargs.get("left") == a.table
            assert ev.kwargs.get("right") == b.table
            assert ev.result_table == result.table


async def test_concat_recorded():
    """concat records all source tables as args."""
    from aaiclick.data import create_object_from_value as cofv
    async with data_context():
        async with oplog_context():
            collector = get_oplog_collector()
            a = await cofv([1, 2])
            b = await cofv([3, 4])
            result = await a.concat(b)
            concat_events = [e for e in collector._buffer if e.operation == "concat"]
            assert concat_events, "Expected 'concat' operation event"
            ev = concat_events[-1]
            assert a.table in ev.args
            assert b.table in ev.args
            assert ev.result_table == result.table


async def test_copy_recorded():
    """Object.copy() records copy with source kwarg."""
    from aaiclick.data import create_object_from_value as cofv
    async with data_context():
        async with oplog_context():
            collector = get_oplog_collector()
            a = await cofv([7, 8, 9])
            result = await a.copy()
            copy_events = [e for e in collector._buffer if e.operation == "copy"]
            assert copy_events, "Expected 'copy' operation event"
            ev = copy_events[-1]
            assert ev.kwargs.get("source") == a.table
            assert ev.result_table == result.table


async def test_aggregation_recorded():
    """Aggregation records the source table."""
    from aaiclick.data import create_object_from_value as cofv
    async with data_context():
        async with oplog_context():
            collector = get_oplog_collector()
            a = await cofv([1, 2, 3, 4])
            _s = await a.sum()
            agg_events = [e for e in collector._buffer if e.operation == "sum"]
            assert agg_events, "Expected 'sum' operation event"
            ev = agg_events[-1]
            assert ev.kwargs.get("source") == a.table


async def test_buffer_discarded_on_exception():
    """On error, flush() is NOT called — no entries written to operation_log."""
    flushed = False

    class TrackingCollector(OplogCollector):
        async def flush(self):
            nonlocal flushed
            flushed = True
            await super().flush()

    with pytest.raises(RuntimeError, match="test error"):
        async with data_context():
            async with oplog_context(collector=TrackingCollector()):
                raise RuntimeError("test error")

    assert not flushed, "flush() should not be called on exception"


async def test_flush_writes_to_operation_log():
    """On clean exit, buffered events are written to operation_log."""
    from aaiclick.data import create_object_from_value as cofv

    async with data_context():
        async with oplog_context():
            obj = await cofv([100, 200, 300])
            table_name = obj.table

    ch = await create_ch_client()
    result = await ch.query(
        f"SELECT operation FROM operation_log WHERE result_table = '{table_name}' LIMIT 1"
    )
    assert result.result_rows, f"No oplog entry found for {table_name}"
    assert result.result_rows[0][0] == "create_from_value"


async def test_flush_writes_to_table_registry():
    """On clean exit, created tables are registered in table_registry."""
    from aaiclick.data import create_object_from_value as cofv

    async with data_context():
        async with oplog_context():
            obj = await cofv([1])
            table_name = obj.table

    ch = await create_ch_client()
    result = await ch.query(
        f"SELECT table_name FROM table_registry WHERE table_name = '{table_name}' LIMIT 1"
    )
    assert result.result_rows, f"No registry entry for {table_name}"


async def test_task_job_ids_stored():
    """task_id and job_id are stored in operation_log entries."""
    from aaiclick.data import create_object_from_value as cofv

    async with data_context():
        async with oplog_context(collector=OplogCollector(task_id=42, job_id=99)):
            obj = await cofv([5])
            table_name = obj.table

    ch = await create_ch_client()
    result = await ch.query(
        f"SELECT task_id, job_id FROM operation_log WHERE result_table = '{table_name}' LIMIT 1"
    )
    assert result.result_rows
    task_id_val, job_id_val = result.result_rows[0]
    assert task_id_val == 42
    assert job_id_val == 99
