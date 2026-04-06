"""
Tests for oplog recording via the lifecycle handler queue.
"""

from __future__ import annotations

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


async def test_oplog_writes_immediately(orch_ctx):
    """Operations are written to operation_log immediately (unbuffered)."""
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


async def test_concat_records_kwargs(orch_ctx):
    """Concat records source tables as kwargs (source_0, source_1)."""
    async with task_scope(task_id=1, job_id=1):
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([4, 5, 6])
        result = await a.concat(b)
        a_table, b_table, result_table = a.table, b.table, result.table

    ch = await create_ch_client()
    row = (await ch.query(
        f"SELECT operation, kwargs FROM operation_log "
        f"WHERE result_table = '{result_table}' LIMIT 1"
    )).result_rows
    assert row
    operation, kwargs_raw = row[0]
    assert operation == "concat"
    kwargs = dict(kwargs_raw) if not isinstance(kwargs_raw, dict) else kwargs_raw
    assert set(kwargs.values()) == {a_table, b_table}
