"""Tests for ``aaiclick.internal_api.objects``."""

from __future__ import annotations

import pytest

from aaiclick.data.object_context import (
    create_object_from_value,
    list_persistent_objects,
)
from aaiclick.data.view_models import ObjectDetail, ObjectView
from aaiclick.datetime_utils import utc_now
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.orch_context import task_scope
from aaiclick.snowflake import get_snowflake_id
from aaiclick.view_models import ObjectFilter, Page, PurgeObjectsRequest

from . import errors, objects

_SAMPLE_TASK = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"

# orch supplies the registry read path; its per-test reset drops every CH
# table and SQL row — every registered table — so no extra sweep is needed.
pytestmark = pytest.mark.usefixtures("orch_ctx")


async def test_list_objects_returns_page():
    await create_object_from_value([1, 2, 3], name="list_a", scope="global")
    await create_object_from_value([4, 5], name="list_b", scope="global")

    page = await objects.list_objects()

    assert isinstance(page, Page)
    assert page.total == 2
    assert all(isinstance(o, ObjectView) for o in page.items)
    names = sorted(o.name for o in page.items)
    assert names == ["list_a", "list_b"]
    assert all(o.scope == "global" and o.persistent for o in page.items)


async def test_list_objects_populates_row_count_and_size():
    await create_object_from_value([1, 2, 3, 4], name="metrics_target", scope="global")

    page = await objects.list_objects()

    [view] = [o for o in page.items if o.name == "metrics_target"]
    assert view.row_count == 4
    assert view.size_bytes is not None and view.size_bytes > 0


async def test_list_objects_created_at_is_registry_time():
    """Memory tables report epoch 0 in system.tables; created_at must come from the registry."""
    before = utc_now()
    await create_object_from_value([1], name="created_target", scope="global")

    page = await objects.list_objects()
    detail = await objects.get_object("created_target")

    [view] = [o for o in page.items if o.name == "created_target"]
    assert view.created_at is not None and view.created_at >= before
    assert detail.created_at == view.created_at


async def test_list_objects_prefix_filter():
    await create_object_from_value([1], name="alpha_one", scope="global")
    await create_object_from_value([2], name="alpha_two", scope="global")
    await create_object_from_value([3], name="beta_one", scope="global")

    page = await objects.list_objects(ObjectFilter(prefix="alpha_"))

    assert page.total == 2
    assert sorted(o.name for o in page.items) == ["alpha_one", "alpha_two"]


async def test_list_objects_limit_paginates_but_keeps_total():
    for i in range(5):
        await create_object_from_value([i], name=f"page_{i}", scope="global")

    page = await objects.list_objects(ObjectFilter(limit=2))

    assert page.total == 5
    assert len(page.items) == 2


@pytest.mark.parametrize(
    "object_filter",
    [
        pytest.param(ObjectFilter(scope="temp"), id="non_global_scope"),
        pytest.param(ObjectFilter(scope="job"), id="job_scope_without_job"),
    ],
)
async def test_list_objects_invalid_filter_raises(object_filter):
    with pytest.raises(errors.Invalid):
        await objects.list_objects(object_filter)


async def test_get_object_returns_detail_with_schema():
    await create_object_from_value([10, 20], name="detail_target", scope="global")

    detail = await objects.get_object("detail_target")

    assert isinstance(detail, ObjectDetail)
    assert detail.name == "detail_target"
    assert detail.table == "p_detail_target"
    assert detail.scope == "global"
    col_names = list(detail.table_schema.columns)
    assert "value" in col_names
    assert "aai_id" not in col_names


async def test_get_object_not_found_raises():
    with pytest.raises(errors.NotFound):
        await objects.get_object("does_not_exist_xyz")


async def test_delete_object_drops_table():
    await create_object_from_value([1], name="to_delete", scope="global")

    view = await objects.delete_object("to_delete")

    assert view.name == "to_delete"
    assert "to_delete" not in await list_persistent_objects()


async def test_delete_object_missing_is_idempotent():
    view = await objects.delete_object("never_existed")
    assert view.name == "never_existed"


async def test_delete_object_invalid_name_raises_invalid():
    with pytest.raises(errors.Invalid):
        await objects.delete_object("bad name;")


async def test_purge_objects_requires_time_filter():
    with pytest.raises(errors.Invalid):
        await objects.purge_objects(PurgeObjectsRequest())


async def test_list_objects_job_scope_by_ref():
    job = await create_job("objs_job", _SAMPLE_TASK)
    async with task_scope(task_id=get_snowflake_id(), job_id=job.id, run_id=get_snowflake_id()):
        await create_object_from_value([1, 2], name="result", scope="job")
    await create_object_from_value([3], name="persist", scope="global")

    page = await objects.list_objects(ObjectFilter(scope="job", job=job.id))
    assert [(o.name, o.scope, o.table, o.job_id, o.job_name) for o in page.items] == [
        ("result", "job", f"j_{job.id}_result", job.id, "objs_job")
    ]

    everything = await objects.list_objects(ObjectFilter(scope="all"))
    assert [(o.name, o.scope, o.job_name) for o in everything.items] == [
        ("persist", "global", None),
        ("result", "job", "objs_job"),
    ]

    by_name = await objects.list_objects(ObjectFilter(job="objs_job"))
    assert [o.name for o in by_name.items] == ["result"]

    detail = await objects.get_object("result", job="objs_job")
    assert detail.table == f"j_{job.id}_result"
    assert (detail.job_id, detail.job_name) == (job.id, "objs_job")
    with pytest.raises(errors.NotFound):
        await objects.get_object("persist", job=job.id)

    # The SQL-side name strips "j_<id>_" (not just "j_"), so prefix and order match both tiers.
    async with task_scope(task_id=get_snowflake_id(), job_id=job.id, run_id=get_snowflake_id()):
        await create_object_from_value([4], name="persist_job", scope="job")
    filtered = await objects.list_objects(ObjectFilter(scope="all", prefix="persist", limit=1))
    assert ([(o.name, o.scope) for o in filtered.items], filtered.total) == ([("persist", "global")], 2)
