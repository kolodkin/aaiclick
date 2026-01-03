"""
Tests for integer (Int64) data type - scalars, arrays, operators, and statistics.
"""

import numpy as np
from aaiclick import create_object_from_value

THRESHOLD = 1e-5


# =============================================================================
# Scalar Tests
# =============================================================================

async def test_int_scalar_creation():
    """Test creating an integer scalar object."""
    obj = await create_object_from_value(42)
    data = await obj.data()
    assert data == 42
    await obj.delete_table()


async def test_int_scalar_add():
    """Test addition of integer scalars."""
    a = await create_object_from_value(100)
    b = await create_object_from_value(50)

    result = await (a + b)
    data = await result.data()

    assert data == 150

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


async def test_int_scalar_sub():
    """Test subtraction of integer scalars."""
    a = await create_object_from_value(100)
    b = await create_object_from_value(30)

    result = await (a - b)
    data = await result.data()

    assert data == 70

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


# =============================================================================
# Array Tests
# =============================================================================

async def test_int_array_creation():
    """Test creating an integer array object."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    data = await obj.data()
    assert data == [1, 2, 3, 4, 5]
    await obj.delete_table()


async def test_int_array_add():
    """Test element-wise addition of integer arrays."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20, 30])

    result = await (a + b)
    data = await result.data()

    assert data == [11, 22, 33]

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


async def test_int_array_sub():
    """Test element-wise subtraction of integer arrays."""
    a = await create_object_from_value([100, 200, 300])
    b = await create_object_from_value([10, 20, 30])

    result = await (a - b)
    data = await result.data()

    assert data == [90, 180, 270]

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


async def test_int_array_chained_operations():
    """Test chaining multiple operations on integer arrays."""
    a = await create_object_from_value([10, 20, 30])
    b = await create_object_from_value([1, 2, 3])
    c = await create_object_from_value([5, 10, 15])

    # (a + b) - c
    temp = await (a + b)
    result = await (temp - c)
    data = await result.data()

    assert data == [6, 12, 18]

    await a.delete_table()
    await b.delete_table()
    await c.delete_table()
    await temp.delete_table()
    await result.delete_table()


async def test_int_array_concat():
    """Test concatenating integer arrays."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([4, 5, 6])

    result = await a.concat(b)
    data = await result.data()

    assert data == [1, 2, 3, 4, 5, 6]

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


async def test_int_scalar_concat_fails():
    """Test that concat method on scalar fails."""
    import pytest

    a = await create_object_from_value(42)
    b = await create_object_from_value([1, 2, 3])

    with pytest.raises(ValueError, match="concat requires obj_a to have array fieldtype"):
        await a.concat(b)

    await a.delete_table()
    await b.delete_table()


# =============================================================================
# Statistics Tests
# =============================================================================

async def test_int_array_min():
    """Test min() on integer array."""
    values = [5, 2, 8, 1, 9]
    obj = await create_object_from_value(values)

    result = await obj.min()
    expected = np.min(values)

    assert abs(result - expected) < THRESHOLD

    await obj.delete_table()


async def test_int_array_max():
    """Test max() on integer array."""
    values = [5, 2, 8, 1, 9]
    obj = await create_object_from_value(values)

    result = await obj.max()
    expected = np.max(values)

    assert abs(result - expected) < THRESHOLD

    await obj.delete_table()


async def test_int_array_sum():
    """Test sum() on integer array."""
    values = [1, 2, 3, 4, 5]
    obj = await create_object_from_value(values)

    result = await obj.sum()
    expected = np.sum(values)

    assert abs(result - expected) < THRESHOLD

    await obj.delete_table()


async def test_int_array_mean():
    """Test mean() on integer array."""
    values = [10, 20, 30, 40]
    obj = await create_object_from_value(values)

    result = await obj.mean()
    expected = np.mean(values)

    assert abs(result - expected) < THRESHOLD

    await obj.delete_table()


async def test_int_array_std():
    """Test std() on integer array."""
    values = [2, 4, 6, 8]
    obj = await create_object_from_value(values)

    result = await obj.std()
    expected = np.std(values, ddof=0)

    assert abs(result - expected) < THRESHOLD

    await obj.delete_table()


async def test_int_statistics_after_operation():
    """Test statistics on result of integer operations."""
    a = await create_object_from_value([10, 20, 30])
    b = await create_object_from_value([5, 10, 15])

    result = await (a + b)

    expected_values = np.array([15, 30, 45])

    assert abs(await result.min() - np.min(expected_values)) < THRESHOLD
    assert abs(await result.max() - np.max(expected_values)) < THRESHOLD
    assert abs(await result.sum() - np.sum(expected_values)) < THRESHOLD
    assert abs(await result.mean() - np.mean(expected_values)) < THRESHOLD

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()
