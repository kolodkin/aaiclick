"""Tests for the **without-aai_id** path on ``create_object_from_value``.

Mirrors ``test_with_aai_id.py`` but covers the default behavior when
``aai_id=True`` is omitted: no ``aai_id`` column, no implicit row order,
and the cross-table contract enforced via ``ValueError`` unless callers
opt into an explicit ``view(order_by=...)`` for both operands.

Most happy-path operator tests rely on ``aai_id=True`` (see
``test_with_aai_id.py``); this module is the dedicated coverage of the
fallback contract so we don't silently lose it.
"""

import pytest

from aaiclick import create_object_from_value


async def test_no_aai_id_column_by_default(ctx):
    """Default ``create_object_from_value`` does NOT add an aai_id column."""
    obj = await create_object_from_value([1, 2, 3])
    assert "aai_id" not in obj.schema.columns


async def test_no_implicit_order_by_without_aai_id(ctx):
    """``Object.order_by`` stays None when the schema has no aai_id."""
    obj = await create_object_from_value([3, 1, 2])
    assert obj.order_by is None


async def test_cross_table_op_without_aai_id_raises(ctx):
    """Array+array between two tables with neither side carrying aai_id and
    no explicit ``order_by`` is rejected — pair stability would be
    undefined."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20, 30])
    with pytest.raises(TypeError, match="order_by"):
        await (a + b)


async def test_cross_table_op_without_aai_id_works_with_explicit_order_by(ctx):
    """Wrapping both sides in ``view(order_by="value")`` satisfies the
    cross-table contract without aai_id."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20, 30])
    result = await (a.view(order_by="value") + b.view(order_by="value"))
    assert sorted(await result.data(order_by="value")) == [11, 22, 33]


async def test_partial_aai_id_one_sided_still_requires_order_by(ctx):
    """Only one side has aai_id — the other still needs an explicit
    ``order_by`` so we never silently mis-pair rows."""
    a = await create_object_from_value([1, 2, 3])  # no aai_id
    b = await create_object_from_value([10, 20, 30], aai_id=True)
    with pytest.raises(TypeError, match="order_by"):
        await (a + b)
    # Workaround: explicit order_by on the no-aai_id side.
    result = await (a.view(order_by="value") + b)
    assert "aai_id" in result.schema.columns


async def test_view_order_by_value_recovers_value_order(ctx):
    """Without aai_id, callers can still recover sorted-by-value order."""
    obj = await create_object_from_value([3, 1, 2])
    assert await obj.view(order_by="value").data() == [1, 2, 3]


async def test_data_without_constraints_is_unordered(ctx):
    """Without aai_id and without an explicit order_by, ``data()`` makes no
    ordering guarantee — but the multiset of values is preserved."""
    obj = await create_object_from_value([3, 1, 2])
    assert sorted(await obj.data()) == [1, 2, 3]


async def test_aggregation_works_without_aai_id(ctx):
    """Aggregations don't need pair-stability — they reduce to a scalar."""
    obj = await create_object_from_value([10, 20, 30])
    assert await (await obj.sum()).data() == 60
    assert await (await obj.mean()).data() == 20
