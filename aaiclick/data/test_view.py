"""
Tests for Object view functionality.
"""

import pytest

from aaiclick import create_object_from_value


async def test_view_where_limit(ctx):
    """Test creating a view with WHERE and LIMIT constraints."""
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    view = obj.view(where="value > 5", limit=3)
    result = await view.data()
    assert result == [6, 7, 8]


async def test_view_offset(ctx):
    """Test creating a view with OFFSET constraint."""
    obj = await create_object_from_value([10, 20, 30, 40, 50])
    view = obj.view(offset=2, limit=2)
    result = await view.data()
    assert result == [30, 40]


async def test_view_order_by(ctx):
    """Test creating a view with ORDER BY constraint."""
    obj = await create_object_from_value([3, 1, 4, 1, 5])
    view = obj.view(order_by="value DESC", limit=3)
    result = await view.data()
    assert result == [5, 4, 3]


async def test_view_insert_blocked(ctx):
    """Test that insert() is blocked on views."""
    obj = await create_object_from_value([1, 2, 3])
    view = obj.view(limit=2)
    with pytest.raises(RuntimeError, match="Cannot insert into a view"):
        await view.insert(4)


async def test_view_operator_addition(ctx):
    """Test that operators work with views."""
    obj_a = await create_object_from_value([10, 20, 30, 40, 50])
    obj_b = await create_object_from_value([1, 2, 3, 4, 5])
    view = obj_a.view(where="value > 20")  # [30, 40, 50]
    result = await (view + obj_b)
    data = await result.data()
    assert data == [31, 42, 53]


async def test_view_operator_with_limit(ctx):
    """Test operators with view having LIMIT."""
    obj_a = await create_object_from_value([100, 200, 300, 400])
    obj_b = await create_object_from_value([1, 2, 3, 4])
    view = obj_a.view(limit=2)  # [100, 200]
    result = await (view * obj_b)
    data = await result.data()
    assert data == [100, 400]


async def test_view_both_sides(ctx):
    """Test operators when both operands are views."""
    obj_a = await create_object_from_value([5, 10, 15, 20, 25])
    obj_b = await create_object_from_value([1, 2, 3, 4, 5])
    view_a = obj_a.view(where="value >= 10")  # [10, 15, 20, 25]
    view_b = obj_b.view(limit=3)  # [1, 2, 3]
    result = await (view_a + view_b)
    data = await result.data()
    assert data == [11, 17, 23]


# =============================================================================
# Chained WHERE tests (where / or_where)
# =============================================================================


async def test_view_where_single(ctx):
    """where() on Object creates a View with WHERE condition."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    view = obj.where("value > 3")
    result = await view.data()
    assert result == [4, 5]


async def test_view_where_chained_and(ctx):
    """Multiple where() calls chain with AND."""
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    view = obj.view(where="value > 2").where("value < 8")
    result = await view.data()
    assert result == [3, 4, 5, 6, 7]


async def test_view_or_where(ctx):
    """or_where() chains with OR."""
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    view = obj.view(where="value <= 2").or_where("value >= 9")
    result = await view.data()
    assert result == [1, 2, 9, 10]


async def test_view_where_and_or_mixed(ctx):
    """Mixed where() and or_where() chaining."""
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    view = obj.where("value > 3").where("value < 7").or_where("value = 10")
    result = await view.data()
    assert result == [4, 5, 6, 10]


async def test_view_or_where_without_where_raises(ctx):
    """or_where() without prior where() raises ValueError."""
    obj = await create_object_from_value([1, 2, 3])
    view = obj.view(limit=2)
    with pytest.raises(ValueError, match="prior where"):
        view.or_where("value > 1")


async def test_view_where_empty_string_raises(ctx):
    """Empty string raises ValueError."""
    obj = await create_object_from_value([1, 2, 3])
    view = obj.view(where="value > 1")
    with pytest.raises(ValueError, match="non-empty"):
        view.where("")


async def test_view_or_where_empty_string_raises(ctx):
    """or_where() with empty string raises ValueError."""
    obj = await create_object_from_value([1, 2, 3])
    view = obj.view(where="value > 1")
    with pytest.raises(ValueError, match="non-empty"):
        view.or_where("")


async def test_object_or_where_raises(ctx):
    """or_where() on Object raises ValueError (no prior where)."""
    obj = await create_object_from_value([1, 2, 3])
    with pytest.raises(ValueError, match="prior where"):
        obj.or_where("value > 1")


async def test_view_where_with_dict_object(ctx):
    """where() works with dict objects."""
    obj = await create_object_from_value({
        "category": ["A", "B", "C", "A", "B"],
        "amount": [10, 20, 30, 40, 50],
    })
    view = obj.where("amount > 15").where("amount < 45")
    result = await view.data()
    assert result["category"] == ["B", "C", "A"]
    assert result["amount"] == [20, 30, 40]


async def test_view_or_where_with_group_by(ctx):
    """where/or_where views work with group_by."""
    obj = await create_object_from_value({
        "category": ["A", "A", "B", "B", "C"],
        "amount": [5, 15, 10, 20, 100],
    })
    view = obj.where("amount > 10").or_where("category = 'C'")
    result = await view.group_by("category").sum("amount")
    data = await result.data()
    pairs = dict(zip(data["category"], data["amount"]))
    assert pairs["A"] == 15
    assert pairs["B"] == 20
    assert pairs["C"] == 100
