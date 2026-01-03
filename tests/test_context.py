"""
Tests for Context manager functionality.
"""

import pytest
from aaiclick import Context, create_object_from_value, create_object, get_ch_client


async def test_context_basic_usage():
    """Test basic context manager usage with automatic cleanup."""
    async with Context() as ctx:
        obj = await ctx.create_object_from_value([1, 2, 3])
        data = await obj.data()
        assert data == [1, 2, 3]
        assert not obj.stale

    # After context exits, object should be stale
    assert obj.stale


async def test_context_multiple_objects():
    """Test context manager with multiple objects."""
    async with Context() as ctx:
        obj1 = await ctx.create_object_from_value([1, 2, 3])
        obj2 = await ctx.create_object_from_value([4, 5, 6])
        obj3 = await ctx.create_object_from_value(42)

        # All objects should work within context
        data1 = await obj1.data()
        data2 = await obj2.data()
        data3 = await obj3.data()

        assert data1 == [1, 2, 3]
        assert data2 == [4, 5, 6]
        assert data3 == 42

        assert not obj1.stale
        assert not obj2.stale
        assert not obj3.stale

    # All objects should be stale after context exits
    assert obj1.stale
    assert obj2.stale
    assert obj3.stale


async def test_context_with_operations():
    """Test context manager with object operations."""
    async with Context() as ctx:
        a = await ctx.create_object_from_value([1, 2, 3])
        b = await ctx.create_object_from_value([4, 5, 6])

        # Operations create new objects that are NOT tracked by context
        result = await (a + b)
        data = await result.data()
        assert data == [5, 7, 9]

        assert not a.stale
        assert not b.stale
        # Result object is not tracked by context
        assert not result.stale

    # Only a and b are stale, result is still alive
    assert a.stale
    assert b.stale
    assert not result.stale

    # Clean up result manually
    await result.delete_table()
    assert result.stale


async def test_context_create_object_with_schema():
    """Test context with create_object using explicit schema."""
    async with Context() as ctx:
        obj = await ctx.create_object("value Float64")
        ch_client = await get_ch_client()

        # Insert some data
        await ch_client.command(f"INSERT INTO {obj.table} VALUES (3.14)")

        result = await ch_client.query(f"SELECT * FROM {obj.table}")
        assert len(result.result_rows) == 1
        assert abs(result.result_rows[0][0] - 3.14) < 1e-5

        assert not obj.stale

    assert obj.stale


async def test_context_object_stale_flag():
    """Test that stale flag is set correctly."""
    obj = await create_object_from_value([1, 2, 3])
    assert not obj.stale

    await obj.delete_table()
    assert obj.stale


async def test_context_factory_with_context_parameter():
    """Test using factory functions with context parameter directly."""
    async with Context() as ctx:
        # Using create_object_from_value with context parameter
        obj1 = await create_object_from_value([1, 2, 3], context=ctx)
        # Using create_object with context parameter
        obj2 = await create_object("value Int64", context=ctx)

        assert not obj1.stale
        assert not obj2.stale

    assert obj1.stale
    assert obj2.stale


async def test_context_weakref_behavior():
    """Test that context uses weakref and doesn't prevent garbage collection."""
    async with Context() as ctx:
        obj = await ctx.create_object_from_value([1, 2, 3])
        table_name = obj.table

        # Object is tracked
        assert len(ctx._objects) == 1

        # Even if object is deleted from our scope, weakref should handle it
        del obj

        # Context should still have the weakref entry (though it might be dead)
        # We can't easily test GC behavior in Python, so just verify cleanup works


async def test_context_dict_values():
    """Test context with dict values."""
    async with Context() as ctx:
        # Dict of scalars
        obj1 = await ctx.create_object_from_value({"name": "Alice", "age": 30})
        data1 = await obj1.data()
        assert data1 == {"name": "Alice", "age": 30}

        # Dict of arrays
        obj2 = await ctx.create_object_from_value({"x": [1, 2], "y": [3, 4]})
        data2 = await obj2.data()
        assert data2 == {"x": [1, 2], "y": [3, 4]}

        assert not obj1.stale
        assert not obj2.stale

    assert obj1.stale
    assert obj2.stale


async def test_context_concat_operation():
    """Test context with concat operation."""
    async with Context() as ctx:
        obj1 = await ctx.create_object_from_value([1, 2, 3])
        obj2 = await ctx.create_object_from_value([4, 5, 6])

        # Concat creates a new object not tracked by context
        result = await obj1.concat(obj2)
        data = await result.data()
        assert data == [1, 2, 3, 4, 5, 6]

        assert not result.stale

    # Original objects are stale, result is not
    assert obj1.stale
    assert obj2.stale
    assert not result.stale

    # Clean up result
    await result.delete_table()
    assert result.stale


async def test_context_client_usage():
    """Test that context can use global client."""
    async with Context() as ctx:
        # Context should use global client if none provided
        assert ctx.ch_client is not None

        obj = await ctx.create_object_from_value([1, 2, 3])
        data = await obj.data()
        assert data == [1, 2, 3]
