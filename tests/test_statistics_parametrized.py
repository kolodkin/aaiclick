"""
Parametrized tests for statistics operations across numeric data types.

Tests min, max, sum, mean, and std operations for int, float, and bool types.
String type does not support statistics.
"""

import numpy as np
import pytest

THRESHOLD = 1e-5


# =============================================================================
# Min Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,values,expected_result",
    [
        # Integer arrays
        pytest.param("int", [5, 2, 8, 1, 9], 1, id="int-mixed"),
        pytest.param("int", [10, 20, 30], 10, id="int-ascending"),
        pytest.param("int", [-5, -10, -15], -15, id="int-negative"),
        pytest.param("int", [42], 42, id="int-single"),
        pytest.param("int", [0, 0, 0], 0, id="int-zeros"),
        # Float arrays
        pytest.param("float", [5.5, 2.2, 8.8, 1.1, 9.9], 1.1, id="float-mixed"),
        pytest.param("float", [10.0, 20.0, 30.0], 10.0, id="float-ascending"),
        pytest.param("float", [-5.5, -10.5], -10.5, id="float-negative"),
        pytest.param("float", [3.14], 3.14, id="float-single"),
        # Boolean arrays (as UInt8)
        pytest.param("bool", [True, False, True], 0, id="bool-mixed"),
        pytest.param("bool", [True, True, True], 1, id="bool-all-true"),
        pytest.param("bool", [False, False, False], 0, id="bool-all-false"),
    ],
)
async def test_array_min(ctx, data_type, values, expected_result):
    """Test min() on arrays across numeric types."""
    obj = await ctx.create_object_from_value(values)

    result = await obj.min()

    if isinstance(expected_result, float):
        assert abs(result - expected_result) < THRESHOLD
    else:
        assert result == expected_result


# =============================================================================
# Max Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,values,expected_result",
    [
        # Integer arrays
        pytest.param("int", [5, 2, 8, 1, 9], 9, id="int-mixed"),
        pytest.param("int", [10, 20, 30], 30, id="int-ascending"),
        pytest.param("int", [-5, -10, -15], -5, id="int-negative"),
        pytest.param("int", [42], 42, id="int-single"),
        pytest.param("int", [0, 0, 0], 0, id="int-zeros"),
        # Float arrays
        pytest.param("float", [5.5, 2.2, 8.8, 1.1, 9.9], 9.9, id="float-mixed"),
        pytest.param("float", [10.0, 20.0, 30.0], 30.0, id="float-ascending"),
        pytest.param("float", [-5.5, -10.5], -5.5, id="float-negative"),
        pytest.param("float", [3.14], 3.14, id="float-single"),
        # Boolean arrays (as UInt8)
        pytest.param("bool", [True, False, True], 1, id="bool-mixed"),
        pytest.param("bool", [True, True, True], 1, id="bool-all-true"),
        pytest.param("bool", [False, False, False], 0, id="bool-all-false"),
    ],
)
async def test_array_max(ctx, data_type, values, expected_result):
    """Test max() on arrays across numeric types."""
    obj = await ctx.create_object_from_value(values)

    result = await obj.max()

    if isinstance(expected_result, float):
        assert abs(result - expected_result) < THRESHOLD
    else:
        assert result == expected_result


# =============================================================================
# Sum Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,values,expected_result",
    [
        # Integer arrays
        pytest.param("int", [1, 2, 3, 4, 5], 15, id="int-sequential"),
        pytest.param("int", [10, 20, 30], 60, id="int-multiples"),
        pytest.param("int", [-5, -10, 5, 10], 0, id="int-canceling"),
        pytest.param("int", [42], 42, id="int-single"),
        pytest.param("int", [0, 0, 0], 0, id="int-zeros"),
        # Float arrays
        pytest.param("float", [1.1, 2.2, 3.3, 4.4, 5.5], 16.5, id="float-sequential"),
        pytest.param("float", [10.0, 20.0, 30.0], 60.0, id="float-multiples"),
        pytest.param("float", [-5.5, 5.5], 0.0, id="float-canceling"),
        pytest.param("float", [3.14], 3.14, id="float-single"),
        # Boolean arrays (counts True values)
        pytest.param("bool", [True, False, True, True, False], 3, id="bool-mixed"),
        pytest.param("bool", [True, True, True], 3, id="bool-all-true"),
        pytest.param("bool", [False, False, False], 0, id="bool-all-false"),
    ],
)
async def test_array_sum(ctx, data_type, values, expected_result):
    """Test sum() on arrays across numeric types."""
    obj = await ctx.create_object_from_value(values)

    result = await obj.sum()

    if isinstance(expected_result, float):
        assert abs(result - expected_result) < THRESHOLD
    else:
        assert result == expected_result


# =============================================================================
# Mean Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,values,expected_result",
    [
        # Integer arrays
        pytest.param("int", [10, 20, 30, 40], 25.0, id="int-multiples"),
        pytest.param("int", [1, 2, 3, 4, 5], 3.0, id="int-sequential"),
        pytest.param("int", [0, 0, 0], 0.0, id="int-zeros"),
        pytest.param("int", [42], 42.0, id="int-single"),
        # Float arrays
        pytest.param("float", [10.5, 20.5, 30.5, 40.5], 25.5, id="float-multiples"),
        pytest.param("float", [1.0, 2.0, 3.0], 2.0, id="float-sequential"),
        pytest.param("float", [3.14], 3.14, id="float-single"),
        # Boolean arrays (proportion of True values)
        pytest.param("bool", [True, False, True, False], 0.5, id="bool-half"),
        pytest.param("bool", [True, True, True], 1.0, id="bool-all-true"),
        pytest.param("bool", [False, False, False], 0.0, id="bool-all-false"),
    ],
)
async def test_array_mean(ctx, data_type, values, expected_result):
    """Test mean() on arrays across numeric types."""
    obj = await ctx.create_object_from_value(values)

    result = await obj.mean()

    assert abs(result - expected_result) < THRESHOLD


# =============================================================================
# Std Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,values",
    [
        # Integer arrays
        [2, 4, 6, 8],
        [10, 20, 30, 40],
        [1, 2, 3, 4, 5],
        [0, 0, 0],
        # Float arrays
        [2.5, 4.5, 6.5, 8.5],
        [1.0, 2.0, 3.0],
        [5.5, 5.5, 5.5],
        # Boolean arrays
        [True, False, True, False],
        [True, True, True],
        [False, False, False],
    ],
)
async def test_array_std(ctx, data_type, values):
    """Test std() on arrays across numeric types."""
    obj = await ctx.create_object_from_value(values)

    result = await obj.std()
    expected = np.std(values, ddof=0)

    assert abs(result - expected) < THRESHOLD


# =============================================================================
# Statistics After Operations Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,array_a,array_b,operator",
    [
        # Integer operations
        ([10, 20, 30], [5, 10, 15], "+"),
        ([100, 200, 300], [10, 20, 30], "-"),
        ([1, 2, 3], [10, 20, 30], "+"),
        # Float operations
        ([10.0, 20.0, 30.0], [5.0, 10.0, 15.0], "+"),
        ([100.5, 200.5, 300.5], [10.5, 20.5, 30.5], "-"),
        ([1.5, 2.5], [3.5, 4.5], "+"),
    ],
)
async def test_statistics_after_operation(ctx, data_type, array_a, array_b, operator):
    """Test statistics on result of arithmetic operations."""
    obj_a = await ctx.create_object_from_value(array_a)
    obj_b = await ctx.create_object_from_value(array_b)

    # Apply operation
    if operator == "+":
        result = await (obj_a + obj_b)
    elif operator == "-":
        result = await (obj_a - obj_b)
    else:
        raise ValueError(f"Unsupported operator: {operator}")

    # Calculate expected values
    if operator == "+":
        expected_values = np.array(array_a) + np.array(array_b)
    else:
        expected_values = np.array(array_a) - np.array(array_b)

    # Test all statistics
    assert abs(await result.min() - np.min(expected_values)) < THRESHOLD
    assert abs(await result.max() - np.max(expected_values)) < THRESHOLD
    assert abs(await result.sum() - np.sum(expected_values)) < THRESHOLD
    assert abs(await result.mean() - np.mean(expected_values)) < THRESHOLD
    assert abs(await result.std() - np.std(expected_values, ddof=0)) < THRESHOLD


# =============================================================================
# Single Value Statistics Tests
# =============================================================================


@pytest.mark.parametrize(
    "value",
    [
        # Single element arrays
        [42],
        [42.5],
        [0],
        [0.0],
        [-100],
        [-100.5],
    ],
)
async def test_single_value_statistics(ctx, value):
    """Test statistics on single-element arrays."""
    obj = await ctx.create_object_from_value(value)

    expected_val = float(value[0])

    assert abs(await obj.min() - expected_val) < THRESHOLD
    assert abs(await obj.max() - expected_val) < THRESHOLD
    assert abs(await obj.sum() - expected_val) < THRESHOLD
    assert abs(await obj.mean() - expected_val) < THRESHOLD
    assert abs(await obj.std() - 0.0) < THRESHOLD


# =============================================================================
# Special Cases Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,values,expected_min,expected_max,expected_sum,expected_mean,expected_std",
    [
        # All same values (std should be 0)
        ([5, 5, 5, 5], 5, 5, 20, 5.0, 0.0),
        ([10.5, 10.5, 10.5], 10.5, 10.5, 31.5, 10.5, 0.0),
        # All True boolean array
        ([True, True, True], 1, 1, 3, 1.0, 0.0),
        # All False boolean array
        ([False, False, False], 0, 0, 0, 0.0, 0.0),
        # Mixed zeros and non-zeros
        ([0, 5, 0, 5], 0, 5, 10, 2.5, 2.5),
    ],
)
async def test_special_cases(ctx, data_type, values, expected_min, expected_max, expected_sum, expected_mean, expected_std):
    """Test statistics on special case arrays."""
    obj = await ctx.create_object_from_value(values)

    assert abs(await obj.min() - expected_min) < THRESHOLD
    assert abs(await obj.max() - expected_max) < THRESHOLD
    assert abs(await obj.sum() - expected_sum) < THRESHOLD
    assert abs(await obj.mean() - expected_mean) < THRESHOLD
    assert abs(await obj.std() - expected_std) < THRESHOLD


# =============================================================================
# Negative Numbers Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,values",
    [
        # All negative integers
        [-10, -20, -30, -40],
        # All negative floats
        [-1.5, -2.5, -3.5],
        # Mixed positive and negative
        [-5, 5, -10, 10],
        [-2.5, 2.5, -5.0, 5.0],
    ],
)
async def test_negative_numbers_statistics(ctx, data_type, values):
    """Test statistics with negative numbers."""
    obj = await ctx.create_object_from_value(values)

    expected_min = np.min(values)
    expected_max = np.max(values)
    expected_sum = np.sum(values)
    expected_mean = np.mean(values)
    expected_std = np.std(values, ddof=0)

    assert abs(await obj.min() - expected_min) < THRESHOLD
    assert abs(await obj.max() - expected_max) < THRESHOLD
    assert abs(await obj.sum() - expected_sum) < THRESHOLD
    assert abs(await obj.mean() - expected_mean) < THRESHOLD
    assert abs(await obj.std() - expected_std) < THRESHOLD
