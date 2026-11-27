"""
Tests for oplog recording via the lifecycle handler queue.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from aaiclick.backend import internal_table
from aaiclick.data.object import Object
from aaiclick.data.object_context import create_object_from_value
from aaiclick.data.object_context.ch_client import get_ch_client
from aaiclick.oplog.oplog_api import oplog_record, oplog_record_sample
from aaiclick.orchestration.orch_context import task_scope
from aaiclick.orchestration.sql_context import get_sql_session


# req: user_guide/lineage/operation-log, architecture/data_model/internal-databases, designs/oplog/recording, designs/oplog/instrumentation-points
async def test_oplog_writes_on_operation(orch_ctx):
    """Operations are written to operation_log with correct metadata."""
    async with task_scope(task_id=42, job_id=99, run_id=420):
        obj = await create_object_from_value([5])
        table_name = obj.table

    ch = get_ch_client()

    row = (
        await ch.query(
            f"SELECT operation, task_id, job_id, run_id FROM {internal_table('operation_log')} WHERE result_table = '{table_name}' LIMIT 1"
        )
    ).result_rows
    assert row and row[0] == ("create_from_value", 42, 99, 420)

    async with get_sql_session() as session:
        result = await session.execute(
            text("SELECT table_name, job_id, task_id, run_id FROM table_registry WHERE table_name = :tn"),
            {"tn": table_name},
        )
        reg = result.fetchone()
    assert reg is not None
    assert reg[0] == table_name
    assert reg[1] == 99
    assert reg[2] == 42
    assert reg[3] == 420


async def test_job_scoped_named_object(orch_ctx):
    """Inside an orch task_scope ``scope='job'`` yields ``j_<job_id>_<name>``."""
    async with task_scope(task_id=7, job_id=1234, run_id=700):
        obj = await create_object_from_value([1, 2, 3], name="catalog_intermediate", scope="job")
        assert obj.table == "j_1234_catalog_intermediate"
        assert obj.scope == "job"
        assert obj.persistent is True


async def test_global_scope_overrides_job_default(orch_ctx):
    """scope='global' in orch yields ``p_<name>``."""
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
            ch = get_ch_client()
            await ch.command("DROP TABLE IF EXISTS p_cross_job_catalog")


def test_oplog_record_rejects_non_string_kwarg():
    """Non-string kwarg values raise TypeError at the API boundary.

    Caught early because CH ``Map(String, String)`` would otherwise drop
    the row inside ``_write_oplog_row`` with only a swallowed log line.
    """
    with pytest.raises(TypeError, match="must be str"):
        oplog_record("t_x", "op", kwargs={"keys": ["a", "b"]})  # type: ignore[dict-item]
    with pytest.raises(TypeError, match="must be str"):
        oplog_record_sample("t_x", "op", kwargs={"limit": 100})  # type: ignore[dict-item]


# req: user_guide/lineage/operation-log, architecture/data_model/internal-databases, designs/oplog/instrumentation-points
async def test_concat_records_kwargs(orch_ctx):
    """Concat records source tables as kwargs (source_0, source_1)."""
    async with task_scope(task_id=1, job_id=1, run_id=100):
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([4, 5, 6])
        result = await a.concat(b)
        a_table, b_table, result_table = a.table, b.table, result.table

    ch = get_ch_client()
    row = (
        await ch.query(
            f"SELECT operation, kwargs FROM {internal_table('operation_log')} WHERE result_table = '{result_table}' LIMIT 1"
        )
    ).result_rows
    assert row
    operation, kwargs_raw = row[0]
    assert operation == "concat"
    kwargs = dict(kwargs_raw) if not isinstance(kwargs_raw, dict) else kwargs_raw
    assert set(kwargs.values()) == {a_table, b_table}


async def _insert_into(a: Object, b: Object) -> Object:
    await a.insert(b)
    return a


# req: designs/oplog/instrumentation-points
@pytest.mark.parametrize(
    "build, operation, expected",
    [
        pytest.param(lambda a, b: a.view(order_by="value") + b.view(order_by="value"), "+", {"left": "a", "right": "b"}, id="binary"),
        pytest.param(lambda a, b: a + 5, "+", {"left": "a"}, id="binary-literal-left-out"),
        pytest.param(lambda a, b: a.isin(b), "isin", {"source": "a", "other": "b"}, id="isin"),
        pytest.param(lambda a, b: a.copy(), "copy", {"source": "a"}, id="copy"),
        pytest.param(_insert_into, "insert", {"source": "b", "target": "a"}, id="insert"),
    ],
)
async def test_operation_records_its_input_tables(orch_ctx, build, operation, expected):
    async with task_scope(task_id=2, job_id=2, run_id=200):
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([2, 3, 4])
        result = await build(a, b)
        tables = {"a": a.table, "b": b.table}
        result_table = result.table

    rows = (
        await get_ch_client().query(
            f"SELECT kwargs FROM {internal_table('operation_log')}"
            f" WHERE result_table = '{result_table}' AND operation = '{operation}'"
        )
    ).result_rows
    assert [dict(r[0]) for r in rows] == [{key: tables[role] for key, role in expected.items()}]


# req: designs/oplog/registry-only-create
async def test_table_creation_records_one_operation(orch_ctx):
    """``create_object()`` registers the table; only the caller's operation is logged."""
    async with task_scope(task_id=3, job_id=3, run_id=300):
        table_name = (await create_object_from_value([1])).table

    rows = (
        await get_ch_client().query(
            f"SELECT operation FROM {internal_table('operation_log')} WHERE result_table = '{table_name}'"
        )
    ).result_rows
    assert [tuple(r) for r in rows] == [("create_from_value",)]
