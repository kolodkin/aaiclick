"""Tests for ``aaiclick.internal_api.viewer``."""

from __future__ import annotations

import pytest

from aaiclick.data.data_context import create_object_from_value
from aaiclick.internal_api import errors, viewer
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.orch_context import task_scope
from aaiclick.snowflake import get_snowflake_id
from aaiclick.tenancy import active_tenant
from aaiclick.viewer.view_models import ObjectQueryRequest, OrderBy, SavedQueryFilter, SavedQueryIn

pytestmark = pytest.mark.usefixtures("orch_ctx")

_SAMPLE_TASK = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"


async def _seed_orders():
    return await create_object_from_value(
        {"id": [1, 2, 3], "name": ["a", "b", "c"], "amount": [10, 20, 30]}, name="orders", scope="global"
    )


async def test_query_object_returns_json_compact_meta_and_data():
    await _seed_orders()
    res = await viewer.query_object(ObjectQueryRequest(object="orders", order_by=[OrderBy("id", "ASC")]))
    assert [(c.name, c.type) for c in res.meta] == [("id", "Int64"), ("name", "String"), ("amount", "Int64")]
    # 64-bit integers arrive quoted (output_format_json_quote_64bit_integers); the kernel parses by meta.type
    assert res.data == [["1", "a", "10"], ["2", "b", "20"], ["3", "c", "30"]]
    assert res.text is None


async def test_query_object_fields_where_order_limit_offset():
    await _seed_orders()
    res = await viewer.query_object(
        ObjectQueryRequest(
            object="orders",
            fields=["name"],
            where="amount >= 20",
            order_by=[OrderBy("amount", "DESC")],
            limit=1,
            offset=1,
        )
    )
    assert [c.name for c in res.meta] == ["name"]
    assert res.data == [["b"]]


async def test_query_object_csv():
    await _seed_orders()
    res = await viewer.query_object(
        ObjectQueryRequest(object="orders", fields=["id"], order_by=[OrderBy("id", "ASC")], fmt="csv")
    )
    assert res.text is not None and res.text.splitlines() == ['"id"', "1", "2", "3"]
    assert res.meta == [] and res.data == []


async def test_query_object_job_scope_by_id_and_name():
    job = await create_job("viewer_job", _SAMPLE_TASK)
    async with task_scope(task_id=get_snowflake_id(), job_id=job.id, run_id=get_snowflake_id()):
        await create_object_from_value([5, 6], name="result", scope="job")

    by_id = await viewer.query_object(ObjectQueryRequest(scope=f"job:{job.id}", object="result"))
    by_name = await viewer.query_object(ObjectQueryRequest(scope="job:viewer_job", object="result"))
    assert by_id.data == by_name.data == [["5"], ["6"]]


@pytest.mark.parametrize(
    "request_kwargs",
    [
        pytest.param({"object": "orders", "where": "1 = 1; DROP TABLE x"}, id="separator"),
        pytest.param({"object": "orders", "where": "id IN (SELECT 1)"}, id="subquery"),
        pytest.param({"object": "orders", "fields": ["nope"]}, id="unknown-field"),
        pytest.param({"object": "orders", "fields": ["aai_id"]}, id="aai_id-field"),
        pytest.param({"object": "orders", "order_by": [OrderBy("nope", "ASC")]}, id="unknown-order-col"),
        pytest.param({"object": "orders", "scope": "task:1"}, id="bad-scope"),
    ],
)
async def test_query_object_invalid(request_kwargs):
    await _seed_orders()
    with pytest.raises(errors.Invalid):
        await viewer.query_object(ObjectQueryRequest(**request_kwargs))


async def test_query_object_unknown_object_and_job():
    with pytest.raises(errors.NotFound):
        await viewer.query_object(ObjectQueryRequest(object="missing"))
    with pytest.raises(errors.NotFound):
        await viewer.query_object(ObjectQueryRequest(scope="job:no_such", object="x"))


async def test_save_query_round_trip_and_upsert():
    await _seed_orders()
    saved = await viewer.save_query(
        SavedQueryIn(
            name="big",
            object="orders",
            where="amount > 15",
            fields=["name"],
            order_by=[OrderBy("amount", "DESC")],
            cell_view="name:\n  type: link\n  value: https://x/{cell}\n",
        )
    )
    assert saved.where == "amount > 15" and saved.order_by == [OrderBy("amount", "DESC")]

    again = await viewer.save_query(SavedQueryIn(name="big", object="orders", where="amount > 25"))
    page = await viewer.list_saved_queries()
    assert [q.name for q in page.items] == ["big"] and page.items[0].where == "amount > 25"
    assert again.updated_at >= saved.updated_at


async def test_list_saved_queries_filters_scope_and_object():
    await viewer.save_query(SavedQueryIn(name="any", scope=None, object="orders"))
    await viewer.save_query(SavedQueryIn(name="job_only", scope="job:etl", object="result"))
    names = lambda page: sorted(q.name for q in page.items)  # noqa: E731
    assert names(await viewer.list_saved_queries(SavedQueryFilter(scope="job:etl"))) == ["any", "job_only"]
    assert names(await viewer.list_saved_queries(SavedQueryFilter(scope="persistent"))) == ["any"]
    assert names(await viewer.list_saved_queries(SavedQueryFilter(object="result"))) == ["job_only"]


async def test_save_query_validates_where_and_cell_view():
    with pytest.raises(errors.Invalid):
        await viewer.save_query(SavedQueryIn(name="x", object="orders", where="id IN (SELECT 1)"))
    with pytest.raises(errors.Invalid):
        await viewer.save_query(SavedQueryIn(name="x", object="orders", cell_view="col: [unclosed"))


async def test_saved_queries_are_tenant_scoped():
    await viewer.save_query(SavedQueryIn(name="mine", object="orders"))
    with active_tenant(2):
        assert (await viewer.list_saved_queries()).items == []
        with pytest.raises(errors.NotFound):
            await viewer.delete_saved_query("mine")
    assert (await viewer.delete_saved_query("mine")).name == "mine"
    assert (await viewer.list_saved_queries()).items == []
