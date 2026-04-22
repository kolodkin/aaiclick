"""Tests for ``aaiclick.internal_api.objects``."""

from __future__ import annotations

import pytest

from aaiclick.data.data_context import (
    create_object_from_value,
    data_context,
    list_persistent_objects,
)
from aaiclick.data.view_models import ObjectDetail, ObjectView
from aaiclick.view_models import ObjectFilter, Page, PurgeObjectsRequest

from . import errors, objects


async def _cleanup_all():
    """Drop every persistent object so tests don't see each other's tables."""
    for name in await list_persistent_objects():
        await objects.delete_object(name)


async def test_list_objects_returns_page():
    async with data_context():
        await _cleanup_all()
        await create_object_from_value([1, 2, 3], name="list_a")
        await create_object_from_value([4, 5], name="list_b")

        page = await objects.list_objects()

        assert isinstance(page, Page)
        assert page.total == 2
        assert all(isinstance(o, ObjectView) for o in page.items)
        names = sorted(o.name for o in page.items)
        assert names == ["list_a", "list_b"]
        assert all(o.scope == "global" and o.persistent for o in page.items)
        await _cleanup_all()


async def test_list_objects_prefix_filter():
    async with data_context():
        await _cleanup_all()
        await create_object_from_value([1], name="alpha_one")
        await create_object_from_value([2], name="alpha_two")
        await create_object_from_value([3], name="beta_one")

        page = await objects.list_objects(ObjectFilter(prefix="alpha_"))

        assert page.total == 2
        assert sorted(o.name for o in page.items) == ["alpha_one", "alpha_two"]
        await _cleanup_all()


async def test_list_objects_limit_paginates_but_keeps_total():
    async with data_context():
        await _cleanup_all()
        for i in range(5):
            await create_object_from_value([i], name=f"page_{i}")

        page = await objects.list_objects(ObjectFilter(limit=2))

        assert page.total == 5
        assert len(page.items) == 2
        await _cleanup_all()


async def test_list_objects_rejects_non_global_scope():
    async with data_context():
        with pytest.raises(errors.Invalid):
            await objects.list_objects(ObjectFilter(scope="temp"))


async def test_get_object_returns_detail_with_schema():
    async with data_context():
        await _cleanup_all()
        await create_object_from_value([10, 20], name="detail_target")

        detail = await objects.get_object("detail_target")

        assert isinstance(detail, ObjectDetail)
        assert detail.name == "detail_target"
        assert detail.table == "p_detail_target"
        assert detail.scope == "global"
        col_names = [c.name for c in detail.table_schema.columns]
        assert "aai_id" in col_names
        await _cleanup_all()


async def test_get_object_not_found_raises():
    async with data_context():
        with pytest.raises(errors.NotFound):
            await objects.get_object("does_not_exist_xyz")


async def test_delete_object_drops_table():
    async with data_context():
        await _cleanup_all()
        await create_object_from_value([1], name="to_delete")

        await objects.delete_object("to_delete")

        assert "to_delete" not in await list_persistent_objects()


async def test_delete_object_missing_is_idempotent():
    async with data_context():
        # No existence check — matches DROP TABLE IF EXISTS semantics.
        await objects.delete_object("never_existed")


async def test_purge_objects_requires_time_filter():
    async with data_context():
        with pytest.raises(errors.Invalid):
            await objects.purge_objects(PurgeObjectsRequest())
