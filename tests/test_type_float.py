"""
Tests for float (Float64) data type - scalars, arrays, operators, and statistics.
"""

import numpy as np
from aaiclick import create_object_from_value

THRESHOLD = 1e-5


# =============================================================================
# Scalar Tests
# =============================================================================

async def test_float_scalar_creation():
    """Test creating a float scalar object."""
    obj = await create_object_from_value(3.14159)
    data = await obj.data()
    assert abs(data - 3.14159) < THRESHOLD
    await obj.delete_table()


async def test_float_scalar_add():
    """Test addition of float scalars."""
    a = await create_object_from_value(100.5)
    b = await create_object_from_value(50.25)

    result = await (a + b)
    data = await result.data()

    assert abs(data - 150.75) < THRESHOLD

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


async def test_float_scalar_sub():
    """Test subtraction of float scalars."""
    a = await create_object_from_value(100.5)
    b = await create_object_from_value(30.25)

    result = await (a - b)
    data = await result.data()

    assert abs(data - 70.25) < THRESHOLD

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


# =============================================================================
# Array Tests
# =============================================================================

async def test_float_array_creation():
    """Test creating a float array object."""
    obj = await create_object_from_value([1.5, 2.5, 3.5])
    data = await obj.data()
    assert data == [1.5, 2.5, 3.5]
    await obj.delete_table()


async def test_float_array_add():
    """Test element-wise addition of float arrays."""
    a = await create_object_from_value([10.0, 20.0, 30.0])
    b = await create_object_from_value([5.0, 10.0, 15.0])

    result = await (a + b)
    data = await result.data()

    assert data == [15.0, 30.0, 45.0]

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


async def test_float_array_sub():
    """Test element-wise subtraction of float arrays."""
    a = await create_object_from_value([100.5, 200.5, 300.5])
    b = await create_object_from_value([10.5, 20.5, 30.5])

    result = await (a - b)
    data = await result.data()

    assert data == [90.0, 180.0, 270.0]

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


async def test_float_array_chained_operations():
    """Test chaining multiple operations on float arrays."""
    a = await create_object_from_value([10.5, 20.5, 30.5])
    b = await create_object_from_value([1.0, 2.0, 3.0])
    c = await create_object_from_value([5.0, 10.0, 15.0])

    # (a + b) - c
    temp = await (a + b)
    result = await (temp - c)
    data = await result.data()

    expected = [6.5, 12.5, 18.5]
    for i, val in enumerate(data):
        assert abs(val - expected[i]) < THRESHOLD

    await a.delete_table()
    await b.delete_table()
    await c.delete_table()
    await temp.delete_table()
    await result.delete_table()


async def test_float_array_concat():
    """Test concatenating float arrays."""
    from aaiclick import concat

    a = await create_object_from_value([1.5, 2.5])
    b = await create_object_from_value([3.5, 4.5, 5.5])

    result = await concat(a, b)
    data = await result.data()

    assert data == [1.5, 2.5, 3.5, 4.5, 5.5]

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


async def test_float_array_concat_method():
    """Test concatenating float arrays using concat method."""
    a = await create_object_from_value([1.5, 2.5])
    b = await create_object_from_value([3.5, 4.5])

    result = await a.concat(b)
    data = await result.data()

    assert data == [1.5, 2.5, 3.5, 4.5]

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


# =============================================================================
# Statistics Tests
# =============================================================================

async def test_float_array_min():
    """Test min() on float array."""
    values = [5.5, 2.2, 8.8, 1.1, 9.9]
    obj = await create_object_from_value(values)

    result = await obj.min()
    expected = np.min(values)

    assert abs(result - expected) < THRESHOLD

    await obj.delete_table()


async def test_float_array_max():
    """Test max() on float array."""
    values = [5.5, 2.2, 8.8, 1.1, 9.9]
    obj = await create_object_from_value(values)

    result = await obj.max()
    expected = np.max(values)

    assert abs(result - expected) < THRESHOLD

    await obj.delete_table()


async def test_float_array_sum():
    """Test sum() on float array."""
    values = [1.1, 2.2, 3.3, 4.4, 5.5]
    obj = await create_object_from_value(values)

    result = await obj.sum()
    expected = np.sum(values)

    assert abs(result - expected) < THRESHOLD

    await obj.delete_table()


async def test_float_array_mean():
    """Test mean() on float array."""
    values = [10.5, 20.5, 30.5, 40.5]
    obj = await create_object_from_value(values)

    result = await obj.mean()
    expected = np.mean(values)

    assert abs(result - expected) < THRESHOLD

    await obj.delete_table()


async def test_float_array_std():
    """Test std() on float array."""
    values = [2.5, 4.5, 6.5, 8.5]
    obj = await create_object_from_value(values)

    result = await obj.std()
    expected = np.std(values, ddof=0)

    assert abs(result - expected) < THRESHOLD

    await obj.delete_table()


async def test_float_statistics_after_operation():
    """Test statistics on result of float operations."""
    a = await create_object_from_value([10.0, 20.0, 30.0])
    b = await create_object_from_value([5.0, 10.0, 15.0])

    result = await (a + b)

    expected_values = np.array([15.0, 30.0, 45.0])

    assert abs(await result.min() - np.min(expected_values)) < THRESHOLD
    assert abs(await result.max() - np.max(expected_values)) < THRESHOLD
    assert abs(await result.sum() - np.sum(expected_values)) < THRESHOLD
    assert abs(await result.mean() - np.mean(expected_values)) < THRESHOLD
    assert abs(await result.std() - np.std(expected_values, ddof=0)) < THRESHOLD

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


async def test_float_single_value_statistics():
    """Test statistics on a single float value."""
    values = [42.5]
    obj = await create_object_from_value(values)

    assert abs(await obj.min() - 42.5) < THRESHOLD
    assert abs(await obj.max() - 42.5) < THRESHOLD
    assert abs(await obj.sum() - 42.5) < THRESHOLD
    assert abs(await obj.mean() - 42.5) < THRESHOLD
    assert abs(await obj.std() - 0.0) < THRESHOLD

    await obj.delete_table()
