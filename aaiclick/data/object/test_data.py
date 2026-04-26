"""Tests for ``Object.data()`` keyword-only kwargs (Phase 5).

Covers:
- ``limit=1000`` safety default for array reads
- ``order_by``, ``offset``, ``limit`` keyword-only args
- View kwargs override ``View._order_by`` / ``_offset`` / ``_limit`` only
  when the caller explicitly passes them.
"""

from aaiclick import create_object_from_value


async def test_data_limit_default_caps_at_1000(ctx):
    obj = await create_object_from_value(list(range(2500)))
    rows = await obj.data()
    assert len(rows) == 1000


async def test_data_limit_none_returns_all(ctx):
    obj = await create_object_from_value(list(range(2500)))
    rows = await obj.data(limit=None)
    assert len(rows) == 2500


async def test_data_order_by_returns_deterministic_rows(ctx):
    obj = await create_object_from_value([3, 1, 2])
    assert await obj.data(order_by="value") == [1, 2, 3]


async def test_data_offset_and_limit(ctx):
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    assert await obj.data(order_by="value", offset=1, limit=2) == [2, 3]


async def test_data_without_order_by_does_not_raise(ctx):
    """Spec: .data() does not raise on missing order_by — limit=1000 is the safety cap."""
    obj = await create_object_from_value([1, 2, 3])
    rows = await obj.data()
    assert sorted(rows) == [1, 2, 3]


async def test_scalar_data_ignores_kwargs(ctx):
    s = await create_object_from_value(42)
    assert await s.data(order_by="value", offset=5, limit=3) == 42


async def test_view_kwargs_override_view_attrs(ctx):
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    v = obj.view(order_by="value", limit=2)
    assert await v.data() == [1, 2]
    assert await v.data(limit=3) == [1, 2, 3]


async def test_view_attrs_used_when_kwargs_absent(ctx):
    obj = await create_object_from_value([3, 1, 2])
    v = obj.view(order_by="value")
    assert await v.data() == [1, 2, 3]
