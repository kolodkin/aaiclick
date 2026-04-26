"""Persistence tests that verify behavior **across** ``data_context()`` exits.

The default ``ctx`` fixture keeps the data context active for the whole
test body, so a "persistent survives context exit" assertion can never
actually run after exit. These tests open multiple ``async with
data_context()`` blocks so we can verify that ``p_<name>`` tables (and
their ``table_registry.schema_doc`` rows) really do survive between
contexts.

Lives in its own module (no ``orch_ctx`` alongside) so the module-scoped
chdb session boundary doesn't collide with neighbouring data tests.
"""

from __future__ import annotations

from aaiclick import create_object_from_value
from aaiclick.data.data_context import (
    data_context,
    delete_persistent_object,
    list_persistent_objects,
    open_object,
)


async def test_persistent_survives_context_exit():
    """Data written in one context is readable in a fresh context."""
    async with data_context():
        await create_object_from_value(
            [100, 200], name="extra_persist_survive", scope="global"
        )

    try:
        async with data_context():
            obj = await open_object("extra_persist_survive")
            assert obj.persistent is True
            assert await obj.data() == [100, 200]
    finally:
        async with data_context():
            await delete_persistent_object("extra_persist_survive")


async def test_persistent_dict_survives_context_exit():
    """Dict-of-arrays persists across contexts and re-hydrates with full schema."""
    async with data_context():
        await create_object_from_value(
            {"x": [1, 2, 3], "y": [4, 5, 6]},
            name="extra_persist_dict",
            scope="global",
        )

    try:
        async with data_context():
            obj = await open_object("extra_persist_dict")
            data = await obj.data()
            assert data["x"] == [1, 2, 3]
            assert data["y"] == [4, 5, 6]
    finally:
        async with data_context():
            await delete_persistent_object("extra_persist_dict")


async def test_persistent_scalar_survives_context_exit():
    """Scalar persists across contexts."""
    async with data_context():
        await create_object_from_value(42, name="extra_persist_scalar", scope="global")

    try:
        async with data_context():
            obj = await open_object("extra_persist_scalar")
            assert await obj.data() == 42
    finally:
        async with data_context():
            await delete_persistent_object("extra_persist_scalar")


async def test_persistent_append_across_contexts():
    """Re-creating with the same name in a new context appends."""
    async with data_context():
        await create_object_from_value(
            [1, 2], name="extra_persist_append", scope="global"
        )

    try:
        async with data_context():
            await create_object_from_value(
                [3, 4], name="extra_persist_append", scope="global"
            )

        async with data_context():
            obj = await open_object("extra_persist_append")
            assert sorted(await obj.data()) == [1, 2, 3, 4]
    finally:
        async with data_context():
            await delete_persistent_object("extra_persist_append")


async def test_delete_persistent_actually_removes_across_contexts():
    """delete_persistent_object in one context makes open_object fail in the next."""
    async with data_context():
        await create_object_from_value(
            [1], name="extra_persist_delete", scope="global"
        )

    async with data_context():
        await delete_persistent_object("extra_persist_delete")

    async with data_context():
        names = await list_persistent_objects()
        assert "extra_persist_delete" not in names


async def test_persistent_object_is_stale_after_context_exit():
    """The ``Object`` handle becomes stale on exit even though its table persists."""
    async with data_context():
        obj = await create_object_from_value(
            [1, 2, 3], name="extra_persist_stale", scope="global"
        )
        assert not obj.stale

    assert obj.stale  # handle is stale

    # But the underlying table is intact — re-openable:
    try:
        async with data_context():
            reopened = await open_object("extra_persist_stale")
            assert await reopened.data() == [1, 2, 3]
    finally:
        async with data_context():
            await delete_persistent_object("extra_persist_stale")
