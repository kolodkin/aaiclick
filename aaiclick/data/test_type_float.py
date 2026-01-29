"""
Tests for float (Float64) data type - scalars, arrays, operators, and statistics.
"""

from aaiclick import create_object_from_value, create_object
import numpy as np

THRESHOLD = 1e-5


# =============================================================================
# Scalar Tests
# =============================================================================

async def test_float_scalar_creation(ctx):
    """Test creating a float scalar object."""
    obj = await create_object_from_value(3.14159)
    data = await obj.data()
    assert abs(data - 3.14159) < THRESHOLD


async def test_float_scalar_add(ctx):
    """Test addition of float scalars."""
    a = await create_object_from_value(100.5)
    b = await create_object_from_value(50.25)

    result = await (a + b)
    data = await result.data()

    assert abs(data - 150.75) < THRESHOLD



async def test_float_scalar_sub(ctx):
    """Test subtraction of float scalars."""
    a = await create_object_from_value(100.5)
    b = await create_object_from_value(30.25)

    result = await (a - b)
    data = await result.data()

    assert abs(data - 70.25) < THRESHOLD



# =============================================================================
# Array Tests
# =============================================================================

async def test_float_array_creation(ctx):
    """Test creating a float array object."""
    obj = await create_object_from_value([1.5, 2.5, 3.5])
    data = await obj.data()
    assert data == [1.5, 2.5, 3.5]


async def test_float_array_add(ctx):
    """Test element-wise addition of float arrays."""
    a = await create_object_from_value([10.0, 20.0, 30.0])
    b = await create_object_from_value([5.0, 10.0, 15.0])

    result = await (a + b)
    data = await result.data()

    assert data == [15.0, 30.0, 45.0]



async def test_float_array_sub(ctx):
    """Test element-wise subtraction of float arrays."""
    a = await create_object_from_value([100.5, 200.5, 300.5])
    b = await create_object_from_value([10.5, 20.5, 30.5])

    result = await (a - b)
    data = await result.data()

    assert data == [90.0, 180.0, 270.0]



async def test_float_array_chained_operations(ctx):
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



async def test_float_array_concat(ctx):
    """Test concatenating float arrays."""
    a = await create_object_from_value([1.5, 2.5])
    b = await create_object_from_value([3.5, 4.5, 5.5])

    result = await a.concat(b)
    data = await result.data()

    assert data == [1.5, 2.5, 3.5, 4.5, 5.5]



# =============================================================================
# Statistics Tests
# =============================================================================

async def test_float_array_min(ctx):
    """Test min() on float array. Returns Object, use .data() to extract value."""
    values = [5.5, 2.2, 8.8, 1.1, 9.9]
    obj = await create_object_from_value(values)

    result_obj = await obj.min()
    result = await result_obj.data()
    expected = np.min(values)

    assert abs(result - expected) < THRESHOLD



async def test_float_array_max(ctx):
    """Test max() on float array. Returns Object, use .data() to extract value."""
    values = [5.5, 2.2, 8.8, 1.1, 9.9]
    obj = await create_object_from_value(values)

    result_obj = await obj.max()
    result = await result_obj.data()
    expected = np.max(values)

    assert abs(result - expected) < THRESHOLD



async def test_float_array_sum(ctx):
    """Test sum() on float array. Returns Object, use .data() to extract value."""
    values = [1.1, 2.2, 3.3, 4.4, 5.5]
    obj = await create_object_from_value(values)

    result_obj = await obj.sum()
    result = await result_obj.data()
    expected = np.sum(values)

    assert abs(result - expected) < THRESHOLD



async def test_float_array_mean(ctx):
    """Test mean() on float array. Returns Object, use .data() to extract value."""
    values = [10.5, 20.5, 30.5, 40.5]
    obj = await create_object_from_value(values)

    result_obj = await obj.mean()
    result = await result_obj.data()
    expected = np.mean(values)

    assert abs(result - expected) < THRESHOLD



async def test_float_array_std(ctx):
    """Test std() on float array. Returns Object, use .data() to extract value."""
    values = [2.5, 4.5, 6.5, 8.5]
    obj = await create_object_from_value(values)

    result_obj = await obj.std()
    result = await result_obj.data()
    expected = np.std(values, ddof=0)

    assert abs(result - expected) < THRESHOLD



async def test_float_statistics_after_operation(ctx):
    """Test statistics on result of float operations. Returns Objects, use .data() to extract values."""
    a = await create_object_from_value([10.0, 20.0, 30.0])
    b = await create_object_from_value([5.0, 10.0, 15.0])

    result = await (a + b)

    expected_values = np.array([15.0, 30.0, 45.0])

    assert abs(await (await result.min()).data() - np.min(expected_values)) < THRESHOLD
    assert abs(await (await result.max()).data() - np.max(expected_values)) < THRESHOLD
    assert abs(await (await result.sum()).data() - np.sum(expected_values)) < THRESHOLD
    assert abs(await (await result.mean()).data() - np.mean(expected_values)) < THRESHOLD
    assert abs(await (await result.std()).data() - np.std(expected_values, ddof=0)) < THRESHOLD



async def test_float_single_value_statistics(ctx):
    """Test statistics on a single float value. Returns Objects, use .data() to extract values."""
    values = [42.5]
    obj = await create_object_from_value(values)

    assert abs(await (await obj.min()).data() - 42.5) < THRESHOLD
    assert abs(await (await obj.max()).data() - 42.5) < THRESHOLD
    assert abs(await (await obj.sum()).data() - 42.5) < THRESHOLD
    assert abs(await (await obj.mean()).data() - 42.5) < THRESHOLD
    assert abs(await (await obj.std()).data() - 0.0) < THRESHOLD

