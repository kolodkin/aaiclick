"""
Tests for Object statistical methods (min, max, sum, mean, std).
"""

import pytest
from aaiclick import create_object_from_value


@pytest.mark.asyncio
async def test_object_min():
    """Test min method on an object."""
    obj = await create_object_from_value([10.0, 20.0, 5.0, 30.0])

    result = await obj.min()

    assert result == 5.0, f"Expected min to be 5.0, got {result}"

    # Cleanup
    await obj.delete_table()


@pytest.mark.asyncio
async def test_object_max():
    """Test max method on an object."""
    obj = await create_object_from_value([10.0, 20.0, 5.0, 30.0])

    result = await obj.max()

    assert result == 30.0, f"Expected max to be 30.0, got {result}"

    # Cleanup
    await obj.delete_table()


@pytest.mark.asyncio
async def test_object_sum():
    """Test sum method on an object."""
    obj = await create_object_from_value([10.0, 20.0, 5.0, 15.0])

    result = await obj.sum()

    assert result == 50.0, f"Expected sum to be 50.0, got {result}"

    # Cleanup
    await obj.delete_table()


@pytest.mark.asyncio
async def test_object_mean():
    """Test mean method on an object."""
    obj = await create_object_from_value([10.0, 20.0, 30.0, 40.0])

    result = await obj.mean()

    assert result == 25.0, f"Expected mean to be 25.0, got {result}"

    # Cleanup
    await obj.delete_table()


@pytest.mark.asyncio
async def test_object_std():
    """Test std method on an object."""
    # Using values with known standard deviation
    # Values: [2, 4, 6, 8]
    # Mean = 5
    # Variance = ((2-5)^2 + (4-5)^2 + (6-5)^2 + (8-5)^2) / 4 = (9 + 1 + 1 + 9) / 4 = 5
    # Std = sqrt(5) ≈ 2.236
    obj = await create_object_from_value([2.0, 4.0, 6.0, 8.0])

    result = await obj.std()

    # Check with reasonable precision (ClickHouse might have slight floating point differences)
    expected = 2.23606797749979  # sqrt(5)
    assert abs(result - expected) < 0.0001, f"Expected std to be ~{expected}, got {result}"

    # Cleanup
    await obj.delete_table()


@pytest.mark.asyncio
async def test_statistics_with_integers():
    """Test statistical methods with integer values."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])

    min_result = await obj.min()
    max_result = await obj.max()
    sum_result = await obj.sum()
    mean_result = await obj.mean()

    assert min_result == 1
    assert max_result == 5
    assert sum_result == 15
    assert mean_result == 3.0

    # Cleanup
    await obj.delete_table()


@pytest.mark.asyncio
async def test_statistics_single_value():
    """Test statistical methods with a single value."""
    obj = await create_object_from_value([42.0])

    min_result = await obj.min()
    max_result = await obj.max()
    sum_result = await obj.sum()
    mean_result = await obj.mean()
    std_result = await obj.std()

    assert min_result == 42.0
    assert max_result == 42.0
    assert sum_result == 42.0
    assert mean_result == 42.0
    assert std_result == 0.0  # Single value has no variation

    # Cleanup
    await obj.delete_table()


@pytest.mark.asyncio
async def test_statistics_on_result_object():
    """Test statistical methods on an object created from operation results."""
    obj_a = await create_object_from_value([10.0, 20.0, 30.0])
    obj_b = await create_object_from_value([5.0, 10.0, 15.0])

    # Add two objects
    result = await (obj_a + obj_b)

    # Calculate statistics on result (15, 30, 45)
    min_val = await result.min()
    max_val = await result.max()
    sum_val = await result.sum()
    mean_val = await result.mean()

    assert min_val == 15.0
    assert max_val == 45.0
    assert sum_val == 90.0
    assert mean_val == 30.0

    # Cleanup
    await obj_a.delete_table()
    await obj_b.delete_table()
    await result.delete_table()
