"""
Tests for ``aaiclick.oplog.sampling.apply_strategy``.
"""

from __future__ import annotations

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.data_context.ch_client import create_ch_client
from aaiclick.oplog.sampling import apply_strategy
from aaiclick.orchestration.orch_context import task_scope


async def test_empty_strategy_returns_empty(orch_ctx):
    """Empty strategy → empty arrays."""
    async with task_scope(task_id=1, job_id=1, run_id=100):
        a = await create_object_from_value([1, 2, 3], name="empty_left")
        b = await create_object_from_value([4, 5, 6], name="empty_right")
        a_table, b_table = a.table, b.table

    ch = await create_ch_client()
    kwargs_aai_ids, result_aai_ids = await apply_strategy(
        ch, a_table, {"left": b_table}, {},
    )
    assert kwargs_aai_ids == {}
    assert result_aai_ids == []

    await ch.command(f"DROP TABLE IF EXISTS {a_table}")
    await ch.command(f"DROP TABLE IF EXISTS {b_table}")


async def test_strategy_with_unrelated_tables_returns_empty(orch_ctx):
    """Strategy referencing tables not touched by the op → empty arrays."""
    async with task_scope(task_id=1, job_id=1, run_id=100):
        a = await create_object_from_value([1, 2, 3], name="unrel_left")
        b = await create_object_from_value([4, 5, 6], name="unrel_right")
        a_table, b_table = a.table, b.table

    ch = await create_ch_client()
    kwargs_aai_ids, result_aai_ids = await apply_strategy(
        ch, a_table, {"left": b_table}, {"p_something_else": "x = 1"},
    )
    assert kwargs_aai_ids == {}
    assert result_aai_ids == []

    await ch.command(f"DROP TABLE IF EXISTS {a_table}")
    await ch.command(f"DROP TABLE IF EXISTS {b_table}")


async def test_strategy_matches_result_table_nullary(orch_ctx):
    """Nullary op (no kwargs): only the result table's clause can drive matching."""
    async with task_scope(task_id=1, job_id=1, run_id=100):
        a = await create_object_from_value([10, 20, 30], name="nullary_res")
        a_table = a.table

    ch = await create_ch_client()
    kwargs_aai_ids, result_aai_ids = await apply_strategy(
        ch, a_table, {}, {a_table: "value = 20"},
    )
    assert kwargs_aai_ids == {}
    assert len(result_aai_ids) == 1

    await ch.command(f"DROP TABLE IF EXISTS {a_table}")


async def test_strategy_matches_source_in_unary(orch_ctx):
    """Unary op: matching a source row aligns to a result row."""
    async with task_scope(task_id=1, job_id=1, run_id=100):
        src = await create_object_from_value([10, 20, 30], name="unary_src")
        # Derive a result table with the same number of rows by copying src
        # (use a fresh object with the same values so positional join works).
        dst = await create_object_from_value([100, 200, 300], name="unary_dst")
        src_table, dst_table = src.table, dst.table

    ch = await create_ch_client()
    kwargs_aai_ids, result_aai_ids = await apply_strategy(
        ch, dst_table, {"source": src_table}, {src_table: "value = 20"},
    )
    assert list(kwargs_aai_ids.keys()) == ["source"]
    assert len(kwargs_aai_ids["source"]) == 1
    assert len(result_aai_ids) == 1

    await ch.command(f"DROP TABLE IF EXISTS {src_table}")
    await ch.command(f"DROP TABLE IF EXISTS {dst_table}")


async def test_strategy_matches_multi_source_op(orch_ctx):
    """N-ary op: strategy on one source yields positionally-aligned ids."""
    async with task_scope(task_id=1, job_id=1, run_id=100):
        left = await create_object_from_value([10, 20, 30], name="nary_left")
        right = await create_object_from_value([1, 2, 3], name="nary_right")
        result = await (left + right)
        left_table, right_table, result_table = left.table, right.table, result.table

    ch = await create_ch_client()
    kwargs_aai_ids, result_aai_ids = await apply_strategy(
        ch,
        result_table,
        {"left": left_table, "right": right_table},
        {left_table: "value = 20"},
    )
    assert set(kwargs_aai_ids.keys()) == {"left", "right"}
    assert len(kwargs_aai_ids["left"]) == 1
    assert len(kwargs_aai_ids["right"]) == 1
    assert len(result_aai_ids) == 1

    await ch.command(f"DROP TABLE IF EXISTS {left_table}")
    await ch.command(f"DROP TABLE IF EXISTS {right_table}")
