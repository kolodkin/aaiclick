"""Tests for ``aaiclick.internal_api.viewer``."""

from __future__ import annotations

import pytest

from aaiclick.data.object_context import create_object_from_value
from aaiclick.internal_api import errors, viewer
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.orch_context import task_scope
from aaiclick.snowflake import get_snowflake_id
from aaiclick.viewer.view_models import (
    DashboardIn,
    ObjectQuery,
    ObjectQueryRequest,
    OrderBy,
    RunQueriesRequest,
)

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
    # A where routes through the parse-tree scope check, which must accept the job's own table.
    by_name = await viewer.query_object(ObjectQueryRequest(scope="job:viewer_job", object="result", where="1 = 1"))
    assert by_id.data == by_name.data == [["5"], ["6"]]


@pytest.mark.parametrize(
    "request_kwargs",
    [
        pytest.param({"object": "orders", "where": "1 = 1; DROP TABLE x"}, id="separator"),
        pytest.param({"object": "orders", "where": "id IN (SELECT 1)"}, id="subquery"),
        pytest.param({"object": "orders", "where": "1) --"}, id="paren-breakout"),
        pytest.param({"object": "orders", "where": "name IN p_secrets OR 1"}, id="in-other-table"),
        pytest.param({"object": "orders", "where": "id IN url('http://x/y', CSV, 'a Int64')"}, id="in-table-function"),
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


async def test_dashboard_round_trip_and_run():
    await _seed_orders()
    dash = await viewer.save_dashboard(
        DashboardIn(
            name="sales",
            html="<h1>x</h1>",
            queries={
                "top": ObjectQuery(object="orders", fields=["name", "amount"], order_by=[OrderBy("amount", "DESC")])
            },
            params=[{"name": "min", "options": ["15"]}],
        )
    )
    assert dash.queries["top"].fields == ["name", "amount"]
    assert (await viewer.get_dashboard("sales")).params == [{"name": "min", "options": ["15"]}]
    assert [d.name for d in (await viewer.list_dashboards()).items] == ["sales"]
    assert (await viewer.get_dashboard("sales")).html == "<h1>x</h1>"

    results = await viewer.run_dashboard("sales")
    assert results.results == {"top": {"name": ["c", "b", "a"], "amount": ["30", "20", "10"]}}
    assert [c.name for c in results.meta["top"]] == ["name", "amount"]

    # What the SPA sends once it has substituted the dashboard's params.
    filtered = await viewer.run_queries(
        RunQueriesRequest(queries={"top": ObjectQuery(object="orders", fields=["name"], where="amount > '15'")})
    )
    assert sorted(filtered.results["top"]["name"]) == ["b", "c"]

    assert (await viewer.delete_dashboard("sales")).name == "sales"
    with pytest.raises(errors.NotFound):
        await viewer.get_dashboard("sales")


async def test_save_dashboard_validates_panels():
    with pytest.raises(errors.Invalid):
        await viewer.save_dashboard(DashboardIn(name="bad", html="<p/>", queries={}))
    with pytest.raises(errors.Invalid):
        await viewer.save_dashboard(
            DashboardIn(name="bad", html="<p/>", queries={"p": ObjectQuery(object="orders", where="x IN (SELECT 1)")})
        )
