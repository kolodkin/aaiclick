"""Tests for ``aaiclick.internal_api.objects``."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from aaiclick.data.data_context import (
    create_object_from_value,
    list_persistent_objects,
)
from aaiclick.data.view_models import ObjectDetail, ObjectView
from aaiclick.view_models import ObjectFilter, Page, PurgeObjectsRequest

from . import errors, objects


@pytest.fixture(autouse=True)
async def _object_data_ctx(orch_ctx) -> AsyncIterator[None]:
    """Drop any leftover persistent objects around each test.

    Reuses the shared ``orch_ctx`` fixture (orch_context + synthetic
    task_scope) so ``create_object`` writes ``table_registry.schema_doc``
    via the OrchLifecycleHandler — Phase 2's registry-backed read path
    requires it.
    """
    for name in await list_persistent_objects():
        await objects.delete_object(name)
    yield
    for name in await list_persistent_objects():
        await objects.delete_object(name)


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


async def test_list_objects_rejects_non_global_scope():
    with pytest.raises(errors.Invalid):
        await objects.list_objects(ObjectFilter(scope="temp"))


async def test_get_object_returns_detail_with_schema():
    await create_object_from_value([10, 20], name="detail_target", scope="global")

    detail = await objects.get_object("detail_target")

    assert isinstance(detail, ObjectDetail)
    assert detail.name == "detail_target"
    assert detail.table == "p_detail_target"
    assert detail.scope == "global"
    col_names = [c.name for c in detail.table_schema.columns]
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


async def test_purge_objects_requires_time_filter():
    with pytest.raises(errors.Invalid):
        await objects.purge_objects(PurgeObjectsRequest())
