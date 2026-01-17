"""
Tests for Object view functionality.
"""

from aaiclick import create_object_from_value, create_object
from aaiclick import Context


async def test_view_where_limit():
    """Test creating a view with WHERE and LIMIT constraints."""
    async with Context() as ctx:
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
    async with Context() as ctx:
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
    async with Context() as ctx:
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
    async with Context() as ctx:
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
    async with Context() as ctx:
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
    async with Context() as ctx:
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
    async with Context() as ctx:
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


