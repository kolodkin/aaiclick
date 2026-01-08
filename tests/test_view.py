"""
Tests for Object view functionality.
"""

from aaiclick import Context


async def test_view_where_limit():
    """Test creating a view with WHERE and LIMIT constraints."""
    async with Context() as ctx:
        # Create an object with array data
        obj = await ctx.create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

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
        obj = await ctx.create_object_from_value([10, 20, 30, 40, 50])

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
        obj = await ctx.create_object_from_value([3, 1, 4, 1, 5])

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
        obj = await ctx.create_object_from_value([1, 2, 3])

        # Create a view
        view = obj.view(limit=2)

        # Try to insert - should raise RuntimeError
        try:
            await view.insert(4)
            assert False, "Expected RuntimeError"
        except RuntimeError as e:
            assert "Cannot insert into a view" in str(e)
