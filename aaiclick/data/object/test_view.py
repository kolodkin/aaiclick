"""
Tests for Object view functionality.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.models import Computed


@pytest.mark.parametrize(
    "value, view_kwargs, expected",
    [
        pytest.param(
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            {"where": "value > 5", "limit": 3},
            [6, 7, 8],
            id="where-limit",
        ),
        pytest.param([10, 20, 30, 40, 50], {"offset": 2, "limit": 2}, [30, 40], id="offset"),
        pytest.param([3, 1, 4, 1, 5], {"order_by": "value DESC", "limit": 3}, [5, 4, 3], id="order-by"),
    ],
)
async def test_view_constraints(ctx, value, view_kwargs, expected):
    """view() applies WHERE, LIMIT, OFFSET and ORDER BY constraints."""
    obj = await create_object_from_value(value, aai_id=True)
    view = obj.view(**view_kwargs)
    assert await view.data() == expected


async def test_view_insert_blocked(ctx):
    """Test that insert() is blocked on views."""
    obj = await create_object_from_value([1, 2, 3], aai_id=True)
    view = obj.view(limit=2)
    with pytest.raises(RuntimeError, match="Cannot insert into a view"):
        await view.insert(4)


async def test_view_operator_addition(ctx):
    """Test that operators work with views — length must match."""
    obj_a = await create_object_from_value([10, 20, 30, 40, 50], aai_id=True)
    obj_b = await create_object_from_value([1, 2, 3], aai_id=True)
    view = obj_a.view(where="value > 20")  # [30, 40, 50] — 3 elements
    result = view + obj_b
    data = await result.data()
    assert data == [31, 42, 53]


async def test_view_operator_with_limit(ctx):
    """Test operators with view having LIMIT — length must match."""
    obj_a = await create_object_from_value([100, 200, 300, 400], aai_id=True)
    obj_b = await create_object_from_value([1, 2], aai_id=True)
    view = obj_a.view(limit=2)  # [100, 200] — 2 elements
    result = view * obj_b
    data = await result.data()
    assert data == [100, 400]


async def test_view_both_sides(ctx):
    """Test operators when both operands are views — length must match."""
    obj_a = await create_object_from_value([5, 10, 15, 20, 25], aai_id=True)
    obj_b = await create_object_from_value([1, 2, 3, 4, 5], aai_id=True)
    view_a = obj_a.view(where="value >= 10")  # [10, 15, 20, 25] — 4 elements
    view_b = obj_b.view(limit=4)  # [1, 2, 3, 4] — 4 elements
    result = view_a + view_b
    data = await result.data()
    assert data == [11, 17, 23, 29]


# =============================================================================
# Filtered Views keep their WHERE through column selection and rename
# =============================================================================


async def test_view_getitem_preserves_where(ctx):
    """__getitem__ on a filtered View preserves the WHERE clause."""
    obj = await create_object_from_value(
        {
            "category": ["A", "B", "C", "A", "B"],
            "amount": [10, 20, 30, 40, 50],
        },
        aai_id=True,
    )
    filtered = obj.where("amount > 25")
    # Without the fix, ["category"] would create a fresh View losing the WHERE
    col_view = filtered["category"]
    result = await col_view.data()
    assert result == ["C", "A", "B"]


async def test_view_getitem_preserves_computed_columns(ctx):
    """__getitem__ on a View with computed columns preserves them."""
    obj = await create_object_from_value(
        {
            "x": [1, 2, 3, 4, 5],
            "y": [10, 20, 30, 40, 50],
        },
        aai_id=True,
    )
    tagged = obj.with_columns({"big": Computed("UInt8", "y > 25")})
    filtered = tagged.where("big")
    col = filtered["x"]
    result = await col.data()
    assert result == [3, 4, 5]


async def test_computed_field_combines_with_source_field(ctx):
    """A computed field and a plain field of the same View pair row-for-row."""
    obj = await create_object_from_value({"x": [1, 2], "y": [10, 20]})
    v = obj.with_columns({"d": Computed("Int64", "x * 100")})

    assert await (v["d"] + v["x"]).data() == [101, 202]


async def test_computed_field_with_other_projection_needs_row_order(ctx):
    """A computed field and a field of the un-projected source read different
    projections, so pairing them needs an explicit row order."""
    obj = await create_object_from_value({"x": [1, 2], "y": [10, 20]})
    v = obj.with_columns({"d": Computed("Int64", "x * 100")})

    with pytest.raises(TypeError, match="explicit row order"):
        v["d"] + obj["y"]


async def test_view_rename_preserves_where(ctx):
    """rename() on a filtered View preserves the WHERE clause."""
    obj = await create_object_from_value(
        {
            "category": ["A", "B", "C"],
            "amount": [10, 20, 30],
        },
        aai_id=True,
    )
    filtered = obj.where("amount > 15")
    renamed = filtered.rename({"category": "cat"})
    result = await renamed.data()
    assert result["cat"] == ["B", "C"]
    assert result["amount"] == [20, 30]


async def test_views_differing_only_in_order_pair_by_position(ctx):
    """Views that differ only in order_by read rows in different orders, so
    they pair by position rather than row-for-row."""
    obj = await create_object_from_value([1, 2, 3])

    result = obj.view(order_by="value ASC") + obj.view(order_by="value DESC")

    assert await result.data() == [4, 4, 4]
