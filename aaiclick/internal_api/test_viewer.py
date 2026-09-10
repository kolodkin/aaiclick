"""Tests for ``aaiclick.internal_api.viewer``."""

from __future__ import annotations

import pytest

from aaiclick.data.data_context import create_object_from_value
from aaiclick.internal_api import errors, viewer
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.orch_context import task_scope
from aaiclick.snowflake import get_snowflake_id
from aaiclick.viewer.view_models import ObjectQueryRequest, OrderBy

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
