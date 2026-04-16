"""
Tests for oplog recording via the lifecycle handler queue.
"""

from __future__ import annotations

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.data_context.ch_client import create_ch_client
from aaiclick.orchestration.orch_context import task_scope


async def test_oplog_writes_on_operation(orch_ctx):
    """Operations are written to operation_log with correct metadata."""
    async with task_scope(task_id=42, job_id=99, run_id=420):
        obj = await create_object_from_value([5])
        table_name = obj.table

    ch = await create_ch_client()

    row = (
        await ch.query(
            f"SELECT operation, task_id, job_id, run_id FROM operation_log WHERE result_table = '{table_name}' LIMIT 1"
        )
    ).result_rows
    assert row and row[0] == ("create_from_value", 42, 99, 420)

    reg = (
        await ch.query(f"SELECT table_name FROM table_registry WHERE table_name = '{table_name}' LIMIT 1")
    ).result_rows
    assert reg


async def test_concat_records_kwargs(orch_ctx):
    """Concat records source tables as kwargs (source_0, source_1)."""
    async with task_scope(task_id=1, job_id=1, run_id=100):
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([4, 5, 6])
        result = await a.concat(b)
        a_table, b_table, result_table = a.table, b.table, result.table

    ch = await create_ch_client()
    row = (
        await ch.query(f"SELECT operation, kwargs FROM operation_log WHERE result_table = '{result_table}' LIMIT 1")
    ).result_rows
    assert row
    operation, kwargs_raw = row[0]
    assert operation == "concat"
    kwargs = dict(kwargs_raw) if not isinstance(kwargs_raw, dict) else kwargs_raw
    assert set(kwargs.values()) == {a_table, b_table}


async def test_none_mode_leaves_lineage_aai_ids_empty(orch_ctx):
    """Under PreservationMode.NONE (default) lineage id arrays stay empty."""
    async with task_scope(task_id=1, job_id=1, run_id=100):
        a = await create_object_from_value([10, 20, 30])
        b = await create_object_from_value([1, 2, 3])
        result = await (a + b)
        result_table = result.table

    ch = await create_ch_client()
    rows = (
        await ch.query(
            f"SELECT kwargs_aai_ids, result_aai_ids FROM operation_log WHERE result_table = '{result_table}'"
        )
    ).result_rows
    assert rows, f"No oplog entries found for {result_table}"
    for kwargs_aai_ids_raw, result_aai_ids in rows:
        kwargs_aai_ids = dict(kwargs_aai_ids_raw) if not isinstance(kwargs_aai_ids_raw, dict) else kwargs_aai_ids_raw
        assert kwargs_aai_ids == {}, f"expected empty kwargs_aai_ids, got {kwargs_aai_ids}"
        assert list(result_aai_ids) == [], f"expected empty result_aai_ids, got {result_aai_ids}"


async def test_strategy_mode_populates_matching_rows(orch_ctx):
    """Under STRATEGY mode, the strategy's WHERE clause drives lineage tracking.

    Uses persistent (``p_*``) input tables so the strategy can reference them
    by name before the task runs.
    """
    left_table = "p_strat_left"
    right_table = "p_strat_right"

    async with task_scope(
        task_id=1,
        job_id=1,
        run_id=100,
        sampling_strategy={left_table: "value = 10"},
    ):
        a = await create_object_from_value([10, 20, 30], name="strat_left")
        b = await create_object_from_value([1, 2, 3], name="strat_right")
        assert a.table == left_table and b.table == right_table
        result = await (a + b)
        result_table = result.table

    ch = await create_ch_client()
    rows = (
        await ch.query(
            f"SELECT kwargs_aai_ids, result_aai_ids FROM operation_log "
            f"WHERE result_table = '{result_table}' AND operation = '+'"
        )
    ).result_rows
    assert rows, f"No '+' oplog entry for {result_table}"
    kwargs_aai_ids_raw, result_aai_ids_raw = rows[0]
    kwargs_aai_ids = dict(kwargs_aai_ids_raw) if not isinstance(kwargs_aai_ids_raw, dict) else kwargs_aai_ids_raw
    result_aai_ids = list(result_aai_ids_raw)
    # The strategy matched exactly one source row (value = 10); the result
    # array contains the positionally-aligned result row.
    assert len(result_aai_ids) == 1
    assert set(kwargs_aai_ids.keys()) == {"left", "right"}
    assert all(len(ids) == 1 for ids in kwargs_aai_ids.values())

    await ch.command(f"DROP TABLE IF EXISTS {left_table}")
    await ch.command(f"DROP TABLE IF EXISTS {right_table}")
