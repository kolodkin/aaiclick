"""
Tests for integer (Int64) data type - scalars, arrays, operators, and statistics.
"""

import numpy as np
# Removed: from aaiclick import create_object_from_value

THRESHOLD = 1e-5


# =============================================================================
# Scalar Tests
# =============================================================================

async def test_int_scalar_creation(ctx):
    """Test creating an integer scalar object."""
    obj = await ctx.create_object_from_value(42)
    data = await obj.data()
    assert data == 42


async def test_int_scalar_add(ctx):
    """Test addition of integer scalars."""
    a = await ctx.create_object_from_value(100)
    b = await ctx.create_object_from_value(50)

    result = await (a + b)
    data = await result.data()

    assert data == 150

    await result.delete_table()


async def test_int_scalar_sub(ctx):
    """Test subtraction of integer scalars."""
    a = await ctx.create_object_from_value(100)
    b = await ctx.create_object_from_value(30)

    result = await (a - b)
    data = await result.data()

    assert data == 70

    await result.delete_table()


# =============================================================================
# Array Tests
# =============================================================================

async def test_int_array_creation(ctx):
    """Test creating an integer array object."""
    obj = await ctx.create_object_from_value([1, 2, 3, 4, 5])
    data = await obj.data()
    assert data == [1, 2, 3, 4, 5]


async def test_int_array_add(ctx):
    """Test element-wise addition of integer arrays."""
    a = await ctx.create_object_from_value([1, 2, 3])
    b = await ctx.create_object_from_value([10, 20, 30])

    result = await (a + b)
    data = await result.data()

    assert data == [11, 22, 33]

    await result.delete_table()


async def test_int_array_sub(ctx):
    """Test element-wise subtraction of integer arrays."""
    a = await ctx.create_object_from_value([100, 200, 300])
    b = await ctx.create_object_from_value([10, 20, 30])

    result = await (a - b)
    data = await result.data()

    assert data == [90, 180, 270]

    await result.delete_table()


async def test_int_array_chained_operations(ctx):
    """Test chaining multiple operations on integer arrays."""
    a = await ctx.create_object_from_value([10, 20, 30])
    b = await ctx.create_object_from_value([1, 2, 3])
    c = await ctx.create_object_from_value([5, 10, 15])

    # (a + b) - c
    temp = await (a + b)
    result = await (temp - c)
    data = await result.data()

    assert data == [6, 12, 18]

    await temp.delete_table()
    await result.delete_table()


async def test_int_array_concat(ctx):
    """Test concatenating integer arrays."""
    a = await ctx.create_object_from_value([1, 2, 3])
    b = await ctx.create_object_from_value([4, 5, 6])

    result = await a.concat(b)
    data = await result.data()

    assert data == [1, 2, 3, 4, 5, 6]

    await result.delete_table()


async def test_int_scalar_concat_fails(ctx):
    """Test that concat method on scalar fails."""
    import pytest

    a = await ctx.create_object_from_value(42)
    b = await ctx.create_object_from_value([1, 2, 3])

    with pytest.raises(ValueError, match="concat requires obj_a to have array fieldtype"):
        await a.concat(b)



# =============================================================================
# Statistics Tests
# =============================================================================

async def test_int_array_min(ctx):
    """Test min() on integer array."""
    values = [5, 2, 8, 1, 9]
    obj = await ctx.create_object_from_value(values)

    result = await obj.min()
    expected = np.min(values)

    assert abs(result - expected) < THRESHOLD



async def test_int_array_max(ctx):
    """Test max() on integer array."""
    values = [5, 2, 8, 1, 9]
    obj = await ctx.create_object_from_value(values)

    result = await obj.max()
    expected = np.max(values)

    assert abs(result - expected) < THRESHOLD



async def test_int_array_sum(ctx):
    """Test sum() on integer array."""
    values = [1, 2, 3, 4, 5]
    obj = await ctx.create_object_from_value(values)

    result = await obj.sum()
    expected = np.sum(values)

    assert abs(result - expected) < THRESHOLD



async def test_int_array_mean(ctx):
    """Test mean() on integer array."""
    values = [10, 20, 30, 40]
    obj = await ctx.create_object_from_value(values)

    result = await obj.mean()
    expected = np.mean(values)

    assert abs(result - expected) < THRESHOLD



async def test_int_array_std(ctx):
    """Test std() on integer array."""
    values = [2, 4, 6, 8]
    obj = await ctx.create_object_from_value(values)

    result = await obj.std()
    expected = np.std(values, ddof=0)

    assert abs(result - expected) < THRESHOLD



async def test_int_statistics_after_operation(ctx):
    """Test statistics on result of integer operations."""
    a = await ctx.create_object_from_value([10, 20, 30])
    b = await ctx.create_object_from_value([5, 10, 15])

    result = await (a + b)

    expected_values = np.array([15, 30, 45])

    assert abs(await result.min() - np.min(expected_values)) < THRESHOLD
    assert abs(await result.max() - np.max(expected_values)) < THRESHOLD
    assert abs(await result.sum() - np.sum(expected_values)) < THRESHOLD
    assert abs(await result.mean() - np.mean(expected_values)) < THRESHOLD

    await result.delete_table()
