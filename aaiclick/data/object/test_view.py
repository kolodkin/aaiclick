"""
Tests for Object view functionality.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.models import Computed


async def test_view_where_limit(ctx):
    """Test creating a view with WHERE and LIMIT constraints."""
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], aai_id=True)
    view = obj.view(where="value > 5", limit=3)
    result = await view.data()
    assert result == [6, 7, 8]


async def test_view_offset(ctx):
    """Test creating a view with OFFSET constraint."""
    obj = await create_object_from_value([10, 20, 30, 40, 50], aai_id=True)
    view = obj.view(offset=2, limit=2)
    result = await view.data()
    assert result == [30, 40]


async def test_view_order_by(ctx):
    """Test creating a view with ORDER BY constraint."""
    obj = await create_object_from_value([3, 1, 4, 1, 5], aai_id=True)
    view = obj.view(order_by="value DESC", limit=3)
    result = await view.data()
    assert result == [5, 4, 3]


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
    result = await (view + obj_b)
    data = await result.data()
    assert data == [31, 42, 53]


async def test_view_operator_with_limit(ctx):
    """Test operators with view having LIMIT — length must match."""
    obj_a = await create_object_from_value([100, 200, 300, 400], aai_id=True)
    obj_b = await create_object_from_value([1, 2], aai_id=True)
    view = obj_a.view(limit=2)  # [100, 200] — 2 elements
    result = await (view * obj_b)
    data = await result.data()
    assert data == [100, 400]


async def test_view_both_sides(ctx):
    """Test operators when both operands are views — length must match."""
    obj_a = await create_object_from_value([5, 10, 15, 20, 25], aai_id=True)
    obj_b = await create_object_from_value([1, 2, 3, 4, 5], aai_id=True)
    view_a = obj_a.view(where="value >= 10")  # [10, 15, 20, 25] — 4 elements
    view_b = obj_b.view(limit=4)  # [1, 2, 3, 4] — 4 elements
    result = await (view_a + view_b)
    data = await result.data()
    assert data == [11, 17, 23, 29]


# =============================================================================
# Chained WHERE tests (where / or_where)
# =============================================================================


async def test_view_where_single(ctx):
    """where() on Object creates a View with WHERE condition."""
    obj = await create_object_from_value([1, 2, 3, 4, 5], aai_id=True)
    view = obj.where("value > 3")
    result = await view.data()
    assert result == [4, 5]


async def test_view_where_chained_and(ctx):
    """Multiple where() calls chain with AND."""
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], aai_id=True)
    view = obj.view(where="value > 2").where("value < 8")
    result = await view.data()
    assert result == [3, 4, 5, 6, 7]


async def test_view_or_where(ctx):
    """or_where() chains with OR."""
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], aai_id=True)
    view = obj.view(where="value <= 2").or_where("value >= 9")
    result = await view.data()
    assert result == [1, 2, 9, 10]


async def test_view_where_returns_new_view(ctx):
    """where() returns a new View, original is unchanged."""
    obj = await create_object_from_value([1, 2, 3, 4, 5], aai_id=True)
    view1 = obj.where("value > 1")
    view2 = view1.where("value < 5")
    assert view1 is not view2
    assert await view1.data() == [2, 3, 4, 5]
    assert await view2.data() == [2, 3, 4]


async def test_view_where_and_or_mixed(ctx):
    """Mixed where() and or_where() chaining."""
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], aai_id=True)
    view = obj.where("value > 3").where("value < 7").or_where("value = 10")
    result = await view.data()
    assert result == [4, 5, 6, 10]


async def test_view_or_where_without_where_raises(ctx):
    """or_where() without prior where() raises ValueError."""
    obj = await create_object_from_value([1, 2, 3], aai_id=True)
    view = obj.view(limit=2)
    with pytest.raises(ValueError, match="prior where"):
        view.or_where("value > 1")


async def test_view_where_empty_string_raises(ctx):
    """Empty string raises ValueError."""
    obj = await create_object_from_value([1, 2, 3], aai_id=True)
    view = obj.view(where="value > 1")
    with pytest.raises(ValueError, match="non-empty"):
        view.where("")


async def test_view_or_where_empty_string_raises(ctx):
    """or_where() with empty string raises ValueError."""
    obj = await create_object_from_value([1, 2, 3], aai_id=True)
    view = obj.view(where="value > 1")
    with pytest.raises(ValueError, match="non-empty"):
        view.or_where("")


async def test_object_or_where_raises(ctx):
    """or_where() on Object raises ValueError (no prior where)."""
    obj = await create_object_from_value([1, 2, 3], aai_id=True)
    with pytest.raises(ValueError, match="prior where"):
        obj.or_where("value > 1")


async def test_view_where_with_dict_object(ctx):
    """where() works with dict objects."""
    obj = await create_object_from_value(
        {
            "category": ["A", "B", "C", "A", "B"],
            "amount": [10, 20, 30, 40, 50],
        },
        aai_id=True,
    )
    view = obj.where("amount > 15").where("amount < 45")
    result = await view.data()
    assert result["category"] == ["B", "C", "A"]
    assert result["amount"] == [20, 30, 40]


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


async def test_view_where_getitem_count(ctx):
    """Chaining where() + __getitem__ + count() returns filtered count."""
    obj = await create_object_from_value(
        {
            "name": ["a", "b", "c", "d", "e"],
            "score": [10, 20, 30, 40, 50],
        },
        aai_id=True,
    )
    high = obj.where("score >= 30")
    count = await (await high["name"].count()).data()
    assert count == 3


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


async def test_view_or_where_with_group_by(ctx):
    """where/or_where views work with group_by."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B", "C"],
            "amount": [5, 15, 10, 20, 100],
        },
        aai_id=True,
    )
    view = obj.where("amount > 10").or_where("category = 'C'")
    result = await view.group_by("category").sum("amount")
    data = await result.data()
    pairs = dict(zip(data["category"], data["amount"], strict=False))
    assert pairs["A"] == 15
    assert pairs["B"] == 20
    assert pairs["C"] == 100
