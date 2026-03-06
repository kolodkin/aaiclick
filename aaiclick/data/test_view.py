"""
Tests for Object view functionality.
"""

import pytest

from aaiclick import create_object_from_value, create_object
from aaiclick import DataContext


async def test_view_where_limit():
    """Test creating a view with WHERE and LIMIT constraints."""
    async with DataContext() as ctx:
        # Create an object with array data
        obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

        # Create a view with WHERE and LIMIT
        view = obj.view(where="value > 5", limit=3)

        # Get data from view
        result = await view.data()

        # Should return [6, 7, 8] (values > 5, limited to 3)
        assert result == [6, 7, 8]


async def test_view_offset():
    """Test creating a view with OFFSET constraint."""
    async with DataContext() as ctx:
        # Create an object with array data
        obj = await create_object_from_value([10, 20, 30, 40, 50])

        # Create a view with OFFSET
        view = obj.view(offset=2, limit=2)

        # Get data from view
        result = await view.data()

        # Should return [30, 40] (skip first 2, take next 2)
        assert result == [30, 40]


async def test_view_order_by():
    """Test creating a view with ORDER BY constraint."""
    async with DataContext() as ctx:
        # Create an object with array data
        obj = await create_object_from_value([3, 1, 4, 1, 5])

        # Create a view with ORDER BY descending
        view = obj.view(order_by="value DESC", limit=3)

        # Get data from view
        result = await view.data()

        # Should return [5, 4, 3] (top 3 values in descending order)
        assert result == [5, 4, 3]


async def test_view_insert_blocked():
    """Test that insert() is blocked on views."""
    async with DataContext() as ctx:
        # Create an object with array data
        obj = await create_object_from_value([1, 2, 3])

        # Create a view
        view = obj.view(limit=2)

        # Try to insert - should raise RuntimeError
        try:
            await view.insert(4)
            assert False, "Expected RuntimeError"
        except RuntimeError as e:
            assert "Cannot insert into a view" in str(e)


async def test_view_operator_addition():
    """Test that operators work with views."""
    async with DataContext() as ctx:
        # Create two objects
        obj_a = await create_object_from_value([10, 20, 30, 40, 50])
        obj_b = await create_object_from_value([1, 2, 3, 4, 5])

        # Create a view with filter on obj_a
        view = obj_a.view(where="value > 20")  # Should get [30, 40, 50]

        # Add view and obj_b
        result = await (view + obj_b)
        data = await result.data()

        # Should add first 3 elements: [30+1, 40+2, 50+3] = [31, 42, 53]
        assert data == [31, 42, 53]


async def test_view_operator_with_limit():
    """Test operators with view having LIMIT."""
    async with DataContext() as ctx:
        # Create two objects
        obj_a = await create_object_from_value([100, 200, 300, 400])
        obj_b = await create_object_from_value([1, 2, 3, 4])

        # Create view with limit
        view = obj_a.view(limit=2)  # Should get [100, 200]

        # Multiply view and obj_b
        result = await (view * obj_b)
        data = await result.data()

        # Should multiply first 2 elements: [100*1, 200*2] = [100, 400]
        assert data == [100, 400]


async def test_view_both_sides():
    """Test operators when both operands are views."""
    async with DataContext() as ctx:
        # Create two objects
        obj_a = await create_object_from_value([5, 10, 15, 20, 25])
        obj_b = await create_object_from_value([1, 2, 3, 4, 5])

        # Create views on both
        view_a = obj_a.view(where="value >= 10")  # [10, 15, 20, 25]
        view_b = obj_b.view(limit=3)  # [1, 2, 3]

        # Add both views
        result = await (view_a + view_b)
        data = await result.data()

        # Should add first 3 elements: [10+1, 15+2, 20+3] = [11, 17, 23]
        assert data == [11, 17, 23]


# =============================================================================
# Chained WHERE tests (add_where / or_where)
# =============================================================================


async def test_view_add_where_single():
    """add_where() on Object creates a View with WHERE condition."""
    async with DataContext():
        obj = await create_object_from_value([1, 2, 3, 4, 5])
        view = obj.add_where("value > 3")
        result = await view.data()
        assert result == [4, 5]


async def test_view_add_where_chained_and():
    """Multiple add_where() calls chain with AND."""
    async with DataContext():
        obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        view = obj.view(where="value > 2").add_where("value < 8")
        result = await view.data()
        # value > 2 AND value < 8 → [3, 4, 5, 6, 7]
        assert result == [3, 4, 5, 6, 7]


async def test_view_or_where():
    """or_where() chains with OR."""
    async with DataContext():
        obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        view = obj.view(where="value <= 2").or_where("value >= 9")
        result = await view.data()
        # value <= 2 OR value >= 9 → [1, 2, 9, 10]
        assert result == [1, 2, 9, 10]


async def test_view_add_where_and_or_mixed():
    """Mixed add_where() and or_where() chaining."""
    async with DataContext():
        obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        # WHERE (value > 3) AND (value < 7) OR (value = 10)
        view = obj.add_where("value > 3").add_where("value < 7").or_where("value = 10")
        result = await view.data()
        # (value > 3 AND value < 7) OR (value = 10) → [4, 5, 6, 10]
        assert result == [4, 5, 6, 10]


async def test_view_or_where_without_where_raises():
    """or_where() without prior where() raises ValueError."""
    async with DataContext():
        obj = await create_object_from_value([1, 2, 3])
        view = obj.view(limit=2)
        with pytest.raises(ValueError, match="prior where"):
            view.or_where("value > 1")


async def test_view_add_where_empty_string_raises():
    """Empty string raises ValueError."""
    async with DataContext():
        obj = await create_object_from_value([1, 2, 3])
        view = obj.view(where="value > 1")
        with pytest.raises(ValueError, match="non-empty"):
            view.add_where("")


async def test_view_or_where_empty_string_raises():
    """or_where() with empty string raises ValueError."""
    async with DataContext():
        obj = await create_object_from_value([1, 2, 3])
        view = obj.view(where="value > 1")
        with pytest.raises(ValueError, match="non-empty"):
            view.or_where("")


async def test_object_or_where_raises():
    """or_where() on Object raises ValueError (no prior where)."""
    async with DataContext():
        obj = await create_object_from_value([1, 2, 3])
        with pytest.raises(ValueError, match="prior where"):
            obj.or_where("value > 1")


async def test_view_add_where_with_dict_object():
    """add_where() works with dict objects."""
    async with DataContext():
        obj = await create_object_from_value({
            "category": ["A", "B", "C", "A", "B"],
            "amount": [10, 20, 30, 40, 50],
        })
        view = obj.add_where("amount > 15").add_where("amount < 45")
        result = await view.data()
        assert result["category"] == ["B", "C", "A"]
        assert result["amount"] == [20, 30, 40]


async def test_view_or_where_with_group_by():
    """add_where/or_where views work with group_by."""
    async with DataContext():
        obj = await create_object_from_value({
            "category": ["A", "A", "B", "B", "C"],
            "amount": [5, 15, 10, 20, 100],
        })
        # WHERE (amount > 10) OR (category = 'C')
        view = obj.add_where("amount > 10").or_where("category = 'C'")
        result = await view.group_by("category").sum("amount")
        data = await result.data()
        pairs = dict(zip(data["category"], data["amount"]))
        # A: amount=15 (only 15 passes), B: amount=20 (only 20 passes), C: amount=100
        assert pairs["A"] == 15
        assert pairs["B"] == 20
        assert pairs["C"] == 100


