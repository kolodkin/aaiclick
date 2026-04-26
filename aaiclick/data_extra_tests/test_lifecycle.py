"""Lifecycle tests that need explicit ``data_context()`` enter/exit.

The default ``ctx`` / ``orch_ctx`` fixtures keep the data context active
for the whole test body, so assertions like ``assert obj.stale`` after
context exit can't run inline. These tests open their own
``async with data_context():`` blocks so we can verify both in-context
state (``not obj.stale``) and post-exit state (``obj.stale``).

Lives in its own module (no ``orch_ctx`` alongside) so the module-scoped
chdb session boundary doesn't collide with neighbouring data tests.
"""

from __future__ import annotations

import pytest

from aaiclick import (
    FIELDTYPE_SCALAR,
    ColumnInfo,
    Schema,
    create_object,
    create_object_from_value,
)
from aaiclick.data.data_context import (
    data_context,
    delete_object,
    get_ch_client,
    get_data_lifecycle,
)
from aaiclick.data.data_context.lifecycle import LocalLifecycleHandler


async def test_context_basic_usage():
    """Object is live inside ``data_context()`` and stale after exit."""
    async with data_context():
        obj = await create_object_from_value([1, 2, 3])
        data = await obj.data()
        assert data == [1, 2, 3]
        assert not obj.stale
    assert obj.stale


async def test_context_multiple_objects():
    """Every Object created in the block becomes stale on exit."""
    async with data_context():
        obj1 = await create_object_from_value([1, 2, 3])
        obj2 = await create_object_from_value([4, 5, 6])
        obj3 = await create_object_from_value(42)

        assert await obj1.data() == [1, 2, 3]
        assert await obj2.data() == [4, 5, 6]
        assert await obj3.data() == 42
        assert not obj1.stale
        assert not obj2.stale
        assert not obj3.stale

    assert obj1.stale
    assert obj2.stale
    assert obj3.stale


async def test_context_with_operations():
    """Operator results are tracked and become stale on context exit.

    Both operands carry ``aai_id`` so the cross-table contract is
    satisfied via the auto ``order_by="aai_id"`` fallback.
    """
    async with data_context():
        a = await create_object_from_value([1, 2, 3], aai_id=True)
        b = await create_object_from_value([4, 5, 6], aai_id=True)

        result = await (a + b)
        assert sorted(await result.data()) == [5, 7, 9]
        assert not a.stale
        assert not b.stale
        assert not result.stale

    assert a.stale
    assert b.stale
    assert result.stale


async def test_context_create_object_with_schema():
    """Object created via explicit Schema is stale-marked on context exit."""
    async with data_context():
        schema = Schema(fieldtype=FIELDTYPE_SCALAR, columns={"value": ColumnInfo("Float64")})
        obj = await create_object(schema)
        ch = get_ch_client()
        await ch.command(f"INSERT INTO {obj.table} VALUES (3.14)")
        result = await ch.query(f"SELECT * FROM {obj.table}")
        assert len(result.result_rows) == 1
        assert abs(result.result_rows[0][0] - 3.14) < 1e-5
        assert not obj.stale

    assert obj.stale


async def test_context_factory_methods():
    """Both factory paths register Objects for stale-on-exit cleanup."""
    async with data_context():
        obj1 = await create_object_from_value([1, 2, 3])
        schema = Schema(fieldtype=FIELDTYPE_SCALAR, columns={"value": ColumnInfo("Int64")})
        obj2 = await create_object(schema)
        assert not obj1.stale
        assert not obj2.stale

    assert obj1.stale
    assert obj2.stale


async def test_context_dict_values():
    """Dict-of-scalars and dict-of-arrays both stale-mark on exit."""
    async with data_context():
        obj1 = await create_object_from_value({"name": "Alice", "age": 30})
        assert await obj1.data() == {"name": "Alice", "age": 30}

        obj2 = await create_object_from_value({"x": [1, 2], "y": [3, 4]})
        assert await obj2.data() == {"x": [1, 2], "y": [3, 4]}
        assert not obj1.stale
        assert not obj2.stale

    assert obj1.stale
    assert obj2.stale


async def test_context_concat_operation():
    """``concat`` results inherit the lifecycle and stale-mark on exit."""
    async with data_context():
        obj1 = await create_object_from_value([1, 2, 3])
        obj2 = await create_object_from_value([4, 5, 6])
        result = await obj1.concat(obj2)
        assert sorted(await result.data()) == [1, 2, 3, 4, 5, 6]
        assert not result.stale

    assert obj1.stale
    assert obj2.stale
    assert result.stale


async def test_context_stale_error_messages():
    """``data()`` on a stale Object raises with the table name in the message."""
    async with data_context():
        obj = await create_object_from_value([1, 2, 3])
        table_name = obj.table

    assert obj.stale
    with pytest.raises(RuntimeError, match=table_name):
        await obj.data()


async def test_delete_object_marks_stale_inline():
    """``delete_object`` flips the flag immediately, no context-exit needed."""
    async with data_context():
        obj = await create_object_from_value([1, 2, 3])
        assert not obj.stale
        await delete_object(obj)
        assert obj.stale


async def test_data_context_always_creates_local_lifecycle():
    """``data_context()`` (vs ``orch_context()``) creates a LocalLifecycleHandler."""
    async with data_context():
        assert isinstance(get_data_lifecycle(), LocalLifecycleHandler)
        obj = await create_object_from_value([1, 2, 3])
        assert await obj.data() == [1, 2, 3]
