"""
Tests for Object statistical methods (min, max, sum, mean, std).
"""

import pytest
import numpy as np
from aaiclick import create_object_from_value

# Threshold for comparing floating point results
THRESHOLD = 1e-5


@pytest.mark.asyncio
async def test_object_min():
    """Test min method on an object."""
    values = [10.0, 20.0, 5.0, 30.0]
    obj = await create_object_from_value(values)

    result = await obj.min()
    expected = np.min(values)

    assert abs(result - expected) < THRESHOLD, f"Expected min to be {expected}, got {result}"

    # Cleanup
    await obj.delete_table()


@pytest.mark.asyncio
async def test_object_max():
    """Test max method on an object."""
    values = [10.0, 20.0, 5.0, 30.0]
    obj = await create_object_from_value(values)

    result = await obj.max()
    expected = np.max(values)

    assert abs(result - expected) < THRESHOLD, f"Expected max to be {expected}, got {result}"

    # Cleanup
    await obj.delete_table()


@pytest.mark.asyncio
async def test_object_sum():
    """Test sum method on an object."""
    values = [10.0, 20.0, 5.0, 15.0]
    obj = await create_object_from_value(values)

    result = await obj.sum()
    expected = np.sum(values)

    assert abs(result - expected) < THRESHOLD, f"Expected sum to be {expected}, got {result}"

    # Cleanup
    await obj.delete_table()


@pytest.mark.asyncio
async def test_object_mean():
    """Test mean method on an object."""
    values = [10.0, 20.0, 30.0, 40.0]
    obj = await create_object_from_value(values)

    result = await obj.mean()
    expected = np.mean(values)

    assert abs(result - expected) < THRESHOLD, f"Expected mean to be {expected}, got {result}"

    # Cleanup
    await obj.delete_table()


@pytest.mark.asyncio
async def test_object_std():
    """Test std method on an object."""
    # Using values with known standard deviation
    # stddevPop uses population std (ddof=0)
    values = [2.0, 4.0, 6.0, 8.0]
    obj = await create_object_from_value(values)

    result = await obj.std()
    expected = np.std(values, ddof=0)  # Population standard deviation

    assert abs(result - expected) < THRESHOLD, f"Expected std to be {expected}, got {result}"

    # Cleanup
    await obj.delete_table()


@pytest.mark.asyncio
async def test_statistics_with_integers():
    """Test statistical methods with integer values."""
    values = [1, 2, 3, 4, 5]
    obj = await create_object_from_value(values)

    min_result = await obj.min()
    max_result = await obj.max()
    sum_result = await obj.sum()
    mean_result = await obj.mean()

    assert abs(min_result - np.min(values)) < THRESHOLD
    assert abs(max_result - np.max(values)) < THRESHOLD
    assert abs(sum_result - np.sum(values)) < THRESHOLD
    assert abs(mean_result - np.mean(values)) < THRESHOLD

    # Cleanup
    await obj.delete_table()


@pytest.mark.asyncio
async def test_statistics_single_value():
    """Test statistical methods with a single value."""
    values = [42.0]
    obj = await create_object_from_value(values)

    min_result = await obj.min()
    max_result = await obj.max()
    sum_result = await obj.sum()
    mean_result = await obj.mean()
    std_result = await obj.std()

    assert abs(min_result - np.min(values)) < THRESHOLD
    assert abs(max_result - np.max(values)) < THRESHOLD
    assert abs(sum_result - np.sum(values)) < THRESHOLD
    assert abs(mean_result - np.mean(values)) < THRESHOLD
    assert abs(std_result - np.std(values, ddof=0)) < THRESHOLD

    # Cleanup
    await obj.delete_table()


@pytest.mark.asyncio
async def test_statistics_on_result_object():
    """Test statistical methods on an object created from operation results."""
    values_a = [10.0, 20.0, 30.0]
    values_b = [5.0, 10.0, 15.0]
    obj_a = await create_object_from_value(values_a)
    obj_b = await create_object_from_value(values_b)

    # Add two objects
    result = await (obj_a + obj_b)

    # Calculate expected values using numpy
    expected_values = np.array(values_a) + np.array(values_b)

    min_val = await result.min()
    max_val = await result.max()
    sum_val = await result.sum()
    mean_val = await result.mean()

    assert abs(min_val - np.min(expected_values)) < THRESHOLD
    assert abs(max_val - np.max(expected_values)) < THRESHOLD
    assert abs(sum_val - np.sum(expected_values)) < THRESHOLD
    assert abs(mean_val - np.mean(expected_values)) < THRESHOLD

    # Cleanup
    await obj_a.delete_table()
    await obj_b.delete_table()
    await result.delete_table()
