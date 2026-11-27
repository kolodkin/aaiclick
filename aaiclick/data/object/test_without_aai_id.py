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
from aaiclick.data.models import Computed


async def test_no_aai_id_column_by_default(ctx):
    """Default ``create_object_from_value`` does NOT add an aai_id column."""
    obj = await create_object_from_value([1, 2, 3])
    assert "aai_id" not in obj.schema.columns


async def test_no_implicit_order_by_without_aai_id(ctx):
    """``Object.order_by`` stays None when the schema has no aai_id."""
    obj = await create_object_from_value([3, 1, 2])
    assert obj.order_by is None


# req: user_guide/ingest/cross-table-order-by
async def test_cross_table_op_without_aai_id_raises(ctx):
    """Array+array between two tables with neither side carrying aai_id and
    no explicit ``order_by`` is rejected — pair stability would be
    undefined."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20, 30])
    with pytest.raises(TypeError, match="order_by"):
        _ = a + b


# req: user_guide/ingest/cross-table-order-by
async def test_cross_table_op_without_aai_id_works_with_explicit_order_by(ctx):
    """Wrapping both sides in ``view(order_by="value")`` satisfies the
    cross-table contract without aai_id."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20, 30])
    result = a.view(order_by="value") + b.view(order_by="value")
    assert sorted(await result.data(order_by="value")) == [11, 22, 33]


# req: user_guide/ingest/cross-table-order-by
async def test_partial_aai_id_one_sided_still_requires_order_by(ctx):
    """Only one side has aai_id — the other still needs an explicit
    ``order_by`` so we never silently mis-pair rows."""
    a = await create_object_from_value([1, 2, 3])  # no aai_id
    b = await create_object_from_value([10, 20, 30], aai_id=True)
    with pytest.raises(TypeError, match="order_by"):
        _ = a + b
    # Workaround: explicit order_by on the no-aai_id side.
    result = a.view(order_by="value") + b
    assert "aai_id" in result.schema.columns


# req: user_guide/ingest/row-order
async def test_view_order_by_value_recovers_value_order(ctx):
    """Without aai_id, callers can still recover sorted-by-value order."""
    obj = await create_object_from_value([3, 1, 2])
    assert await obj.view(order_by="value").data() == [1, 2, 3]


# req: user_guide/ingest/row-order
async def test_data_without_constraints_is_unordered(ctx):
    """Without aai_id and without an explicit order_by, ``data()`` makes no
    ordering guarantee — but the multiset of values is preserved."""
    obj = await create_object_from_value([3, 1, 2])
    assert sorted(await obj.data()) == [1, 2, 3]


# req: user_guide/ingest/aligned-ops-skip-order
async def test_aggregation_works_without_aai_id(ctx):
    """Aggregations don't need pair-stability — they reduce to a scalar."""
    obj = await create_object_from_value([10, 20, 30])
    assert await obj.sum().data() == 60
    assert await obj.mean().data() == 20


# req: user_guide/ingest/aligned-ops-skip-order
async def test_same_rows_ops_skip_order_contract(ctx):
    """``a - a`` and ``v['y'] - v['x']`` read one set of rows, so they pair without ``order_by``."""
    a = await create_object_from_value([1, 2, 3])
    assert await (a - a).data() == [0, 0, 0]

    v = await create_object_from_value({"x": [1, 2, 3], "y": [10, 20, 30]})
    assert sorted(await (v["y"] - v["x"]).data()) == [9, 18, 27]


# req: user_guide/ingest/aligned-ops-skip-order
async def test_scalar_broadcast_skips_order_contract(ctx):
    """A Python scalar, or a scalar Object from another table, broadcasts without ``order_by``."""
    a = await create_object_from_value([1, 2, 3])
    ten = await create_object_from_value(10)
    assert sorted(await (a + 10).data()) == [11, 12, 13]
    assert sorted(await (a + ten).data()) == [11, 12, 13]


# req: user_guide/ingest/aligned-ops-skip-order
async def test_fields_of_two_views_of_one_table_need_order_by(ctx):
    """A ``with_columns()`` View and its source are two row sets, even over one table."""
    obj = await create_object_from_value({"x": [1, 2, 3]})
    doubled = obj.with_columns({"z": Computed("Int64", "x * 2")})
    with pytest.raises(TypeError, match="order_by"):
        _ = doubled["z"] + obj["x"]
