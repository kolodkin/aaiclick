"""
Tests for oplog recording via the lifecycle handler queue.
"""

from __future__ import annotations

from sqlalchemy import text

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.data_context.ch_client import create_ch_client
from aaiclick.orchestration.orch_context import task_scope
from aaiclick.orchestration.sql_context import get_sql_session


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

    async with get_sql_session() as session:
        result = await session.execute(
            text("SELECT table_name, job_id, task_id FROM table_registry WHERE table_name = :tn"),
            {"tn": table_name},
        )
        reg = result.fetchone()
    assert reg is not None
    assert reg[0] == table_name
    assert reg[1] == 99
    assert reg[2] == 42


async def test_job_scoped_named_object(orch_ctx):
    """Inside an orch task_scope a named object defaults to ``j_<job_id>_<name>``."""
    async with task_scope(task_id=7, job_id=1234, run_id=700):
        obj = await create_object_from_value([1, 2, 3], name="catalog_intermediate")
        assert obj.table == "j_1234_catalog_intermediate"
        assert obj.scope == "job"
        assert obj.persistent is True


async def test_global_scope_overrides_job_default(orch_ctx):
    """scope='global' in orch preserves the ``p_<name>`` behavior."""
    async with task_scope(task_id=8, job_id=1234, run_id=800):
        obj = await create_object_from_value(
            [1, 2, 3],
            name="cross_job_catalog",
            scope="global",
        )
        try:
            assert obj.table == "p_cross_job_catalog"
            assert obj.scope == "global"
        finally:
            ch = await create_ch_client()
            await ch.command("DROP TABLE IF EXISTS p_cross_job_catalog")


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
