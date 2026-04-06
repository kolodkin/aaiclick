"""
Tests for oplog recording via the lifecycle handler queue.
"""

from __future__ import annotations

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.data_context.ch_client import create_ch_client
from aaiclick.orchestration.orch_context import task_scope


async def test_oplog_writes_on_operation(orch_ctx):
    """Operations are written to operation_log with correct metadata."""
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


async def test_binary_op_populates_lineage_aai_ids(orch_ctx):
    """Binary op (add) populates kwargs_aai_ids and result_aai_ids."""
    async with task_scope(task_id=1, job_id=1):
        a = await create_object_from_value([10, 20, 30])
        b = await create_object_from_value([1, 2, 3])
        result = await (a + b)
        result_table = result.table

    ch = await create_ch_client()
    all_ops = (await ch.query(
        f"SELECT operation, kwargs_aai_ids, result_aai_ids FROM operation_log "
        f"WHERE result_table = '{result_table}'"
    )).result_rows
    assert all_ops, f"No oplog entries found for {result_table}"

    row = [(k, r) for op, k, r in all_ops if op == "+"]
    assert row, f"No '+' entry; found: {[op for op, _, _ in all_ops]}"
    kwargs_aai_ids_raw, result_aai_ids = row[0]
    kwargs_aai_ids_raw, result_aai_ids = row[0]
    kwargs_aai_ids = dict(kwargs_aai_ids_raw) if not isinstance(kwargs_aai_ids_raw, dict) else kwargs_aai_ids_raw

    assert len(result_aai_ids) > 0, "result_aai_ids should be populated"
    assert "left" in kwargs_aai_ids, "kwargs_aai_ids should have 'left' key"
    assert "right" in kwargs_aai_ids, "kwargs_aai_ids should have 'right' key"
    assert len(kwargs_aai_ids["left"]) == len(result_aai_ids)
