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


async def test_inherited_driver_uses_parameter_binding(orch_ctx):
    """Large inherited match sets travel as a typed Array(UInt64) parameter,
    not as an inlined SQL literal, so they bypass ``max_query_size`` and
    don't need the hand-rolled escaping fallback."""
    strategy_job_id = 1234

    async with task_scope(
        task_id=1,
        job_id=strategy_job_id,
        run_id=100,
        sampling_strategy={"p_big_prices": "value >= 0"},
    ):
        # 100 matched rows through multiply + add — enough to exercise the
        # propagation path without being slow in tests. The real-world
        # motivation was unbounded match sets (millions of rows); parameter
        # binding lifts the ceiling entirely, this just proves the wiring
        # exercises the typed path.
        values = list(range(100))
        prices = await create_object_from_value(
            [float(v) for v in values], name="big_prices"
        )
        quantities = await create_object_from_value(
            [2.0] * 100, name="big_quantities"
        )
        mul = await (prices * quantities)
        bonus = await create_object_from_value([1.0] * 100, name="big_bonus")
        add = await (mul + bonus)
        add_table = add.table

    ch = await create_ch_client()
    try:
        rows = (
            await ch.query(
                f"SELECT length(kwargs_aai_ids['left']), length(result_aai_ids) "
                f"FROM operation_log "
                f"WHERE job_id = {strategy_job_id} AND result_table = '{add_table}'"
            )
        ).result_rows
        assert rows, "add op should have an oplog row"
        left_len, result_len = rows[0]
        assert left_len == 100
        assert result_len == 100
    finally:
        await ch.command("DROP TABLE IF EXISTS p_big_prices")
        await ch.command("DROP TABLE IF EXISTS p_big_quantities")
        await ch.command("DROP TABLE IF EXISTS p_big_bonus")


async def test_strategy_propagates_through_multi_op_pipeline(orch_ctx):
    """A strategy on the first op's input propagates forward: the second op,
    whose kwargs don't mention any strategy key, still populates arrays
    via inheritance from its source's upstream oplog row."""
    strategy_job_id = 999

    async with task_scope(
        task_id=1,
        job_id=strategy_job_id,
        run_id=100,
        sampling_strategy={"p_prop_prices": "value >= 40"},
    ):
        prices = await create_object_from_value(
            [10.0, 20.0, 30.0, 40.0, 50.0], name="prop_prices"
        )
        quantities = await create_object_from_value(
            [2.0, 3.0, 1.0, 5.0, 4.0], name="prop_quantities"
        )
        mul = await (prices * quantities)
        bonus = await create_object_from_value(
            [5.0, 5.0, 5.0, 5.0, 5.0], name="prop_bonus"
        )
        add = await (mul + bonus)
        add_table = add.table
        mul_table = mul.table

    ch = await create_ch_client()
    try:
        # The * op has explicit strategy matches (prices is in strategy)
        mul_rows = (
            await ch.query(
                f"SELECT kwargs_aai_ids, result_aai_ids FROM operation_log "
                f"WHERE job_id = {strategy_job_id} AND result_table = '{mul_table}'"
            )
        ).result_rows
        assert mul_rows, "multiply op should have an oplog row"
        mul_kwargs, mul_results = mul_rows[0]
        assert len(list(mul_results)) == 2, "strategy matches prices >= 40 → 2 rows"

        # The + op has NO explicit strategy key (neither multiply_result nor
        # t_bonus nor add_result appear in the strategy). Without
        # propagation its arrays would be empty. With propagation, it
        # inherits the 2 matched rows from the multiply op's result.
        add_rows = (
            await ch.query(
                f"SELECT kwargs_aai_ids, result_aai_ids FROM operation_log "
                f"WHERE job_id = {strategy_job_id} AND result_table = '{add_table}'"
            )
        ).result_rows
        assert add_rows, "add op should have an oplog row"
        add_kwargs_raw, add_results_raw = add_rows[0]
        add_kwargs = dict(add_kwargs_raw) if not isinstance(add_kwargs_raw, dict) else add_kwargs_raw
        add_results = list(add_results_raw)
        assert len(add_results) == 2, (
            f"propagation should carry 2 matches into add, got {len(add_results)}"
        )
        assert set(add_kwargs.keys()) == {"left", "right"}
        assert len(add_kwargs["left"]) == 2
        assert len(add_kwargs["right"]) == 2
    finally:
        await ch.command("DROP TABLE IF EXISTS p_prop_prices")
        await ch.command("DROP TABLE IF EXISTS p_prop_quantities")
        await ch.command("DROP TABLE IF EXISTS p_prop_bonus")
