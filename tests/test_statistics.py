"""
Tests for Object statistical methods (min, max, sum, mean, std).
"""

import numpy as np
from aaiclick import create_object_from_value

# Threshold for comparing floating point results
THRESHOLD = 1e-5


async def test_object_min():
    """Test min method on an object."""
    values = [10.0, 20.0, 5.0, 30.0]
    obj = await create_object_from_value(values)

    result = await obj.min()
    expected = np.min(values)
    diff = abs(result - expected)

    assert diff < THRESHOLD, f"Expected min to be {expected}, got {result}, diff={diff}"

    # Cleanup
    await obj.delete_table()


async def test_object_max():
    """Test max method on an object."""
    values = [10.0, 20.0, 5.0, 30.0]
    obj = await create_object_from_value(values)

    result = await obj.max()
    expected = np.max(values)
    diff = abs(result - expected)

    assert diff < THRESHOLD, f"Expected max to be {expected}, got {result}, diff={diff}"

    # Cleanup
    await obj.delete_table()


async def test_object_sum():
    """Test sum method on an object."""
    values = [10.0, 20.0, 5.0, 15.0]
    obj = await create_object_from_value(values)

    result = await obj.sum()
    expected = np.sum(values)
    diff = abs(result - expected)

    assert diff < THRESHOLD, f"Expected sum to be {expected}, got {result}, diff={diff}"

    # Cleanup
    await obj.delete_table()


async def test_object_mean():
    """Test mean method on an object."""
    values = [10.0, 20.0, 30.0, 40.0]
    obj = await create_object_from_value(values)

    result = await obj.mean()
    expected = np.mean(values)
    diff = abs(result - expected)

    assert diff < THRESHOLD, f"Expected mean to be {expected}, got {result}, diff={diff}"

    # Cleanup
    await obj.delete_table()


async def test_object_std():
    """Test std method on an object."""
    # Using values with known standard deviation
    # stddevPop uses population std (ddof=0)
    values = [2.0, 4.0, 6.0, 8.0]
    obj = await create_object_from_value(values)

    result = await obj.std()
    expected = np.std(values, ddof=0)  # Population standard deviation
    diff = abs(result - expected)

    assert diff < THRESHOLD, f"Expected std to be {expected}, got {result}, diff={diff}"

    # Cleanup
    await obj.delete_table()


async def test_statistics_with_integers():
    """Test statistical methods with integer values."""
    values = [1, 2, 3, 4, 5]
    obj = await create_object_from_value(values)

    min_result = await obj.min()
    max_result = await obj.max()
    sum_result = await obj.sum()
    mean_result = await obj.mean()

    min_expected = np.min(values)
    max_expected = np.max(values)
    sum_expected = np.sum(values)
    mean_expected = np.mean(values)

    min_diff = abs(min_result - min_expected)
    max_diff = abs(max_result - max_expected)
    sum_diff = abs(sum_result - sum_expected)
    mean_diff = abs(mean_result - mean_expected)

    assert min_diff < THRESHOLD, f"Expected min {min_expected}, got {min_result}, diff={min_diff}"
    assert max_diff < THRESHOLD, f"Expected max {max_expected}, got {max_result}, diff={max_diff}"
    assert sum_diff < THRESHOLD, f"Expected sum {sum_expected}, got {sum_result}, diff={sum_diff}"
    assert mean_diff < THRESHOLD, f"Expected mean {mean_expected}, got {mean_result}, diff={mean_diff}"

    # Cleanup
    await obj.delete_table()


async def test_statistics_single_value():
    """Test statistical methods with a single value."""
    values = [42.0]
    obj = await create_object_from_value(values)

    min_result = await obj.min()
    max_result = await obj.max()
    sum_result = await obj.sum()
    mean_result = await obj.mean()
    std_result = await obj.std()

    min_expected = np.min(values)
    max_expected = np.max(values)
    sum_expected = np.sum(values)
    mean_expected = np.mean(values)
    std_expected = np.std(values, ddof=0)

    min_diff = abs(min_result - min_expected)
    max_diff = abs(max_result - max_expected)
    sum_diff = abs(sum_result - sum_expected)
    mean_diff = abs(mean_result - mean_expected)
    std_diff = abs(std_result - std_expected)

    assert min_diff < THRESHOLD, f"Expected min {min_expected}, got {min_result}, diff={min_diff}"
    assert max_diff < THRESHOLD, f"Expected max {max_expected}, got {max_result}, diff={max_diff}"
    assert sum_diff < THRESHOLD, f"Expected sum {sum_expected}, got {sum_result}, diff={sum_diff}"
    assert mean_diff < THRESHOLD, f"Expected mean {mean_expected}, got {mean_result}, diff={mean_diff}"
    assert std_diff < THRESHOLD, f"Expected std {std_expected}, got {std_result}, diff={std_diff}"

    # Cleanup
    await obj.delete_table()


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

    min_expected = np.min(expected_values)
    max_expected = np.max(expected_values)
    sum_expected = np.sum(expected_values)
    mean_expected = np.mean(expected_values)

    min_diff = abs(min_val - min_expected)
    max_diff = abs(max_val - max_expected)
    sum_diff = abs(sum_val - sum_expected)
    mean_diff = abs(mean_val - mean_expected)

    assert min_diff < THRESHOLD, f"Expected min {min_expected}, got {min_val}, diff={min_diff}"
    assert max_diff < THRESHOLD, f"Expected max {max_expected}, got {max_val}, diff={max_diff}"
    assert sum_diff < THRESHOLD, f"Expected sum {sum_expected}, got {sum_val}, diff={sum_diff}"
    assert mean_diff < THRESHOLD, f"Expected mean {mean_expected}, got {mean_val}, diff={mean_diff}"

    # Cleanup
    await obj_a.delete_table()
    await obj_b.delete_table()
    await result.delete_table()
