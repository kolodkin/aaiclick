"""
Parametrized tests for statistics operations across numeric data types.

Tests min, max, sum, mean, std, var, count, and quantile operations for int, float, and bool types.
String type does not support statistics.
"""

import numpy as np
import pytest
from aaiclick import create_object_from_value, create_object

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
    """Test min() on arrays across numeric types. Returns Object, use .data() to extract value."""
    obj = await create_object_from_value(values)

    result_obj = await obj.min()
    result = await result_obj.data()

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
    """Test max() on arrays across numeric types. Returns Object, use .data() to extract value."""
    obj = await create_object_from_value(values)

    result_obj = await obj.max()
    result = await result_obj.data()

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
    """Test sum() on arrays across numeric types. Returns Object, use .data() to extract value."""
    obj = await create_object_from_value(values)

    result_obj = await obj.sum()
    result = await result_obj.data()

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
    """Test mean() on arrays across numeric types. Returns Object, use .data() to extract value."""
    obj = await create_object_from_value(values)

    result_obj = await obj.mean()
    result = await result_obj.data()

    assert abs(result - expected_result) < THRESHOLD


# =============================================================================
# Std Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,values",
    [
        # Integer arrays
        pytest.param("int", [2, 4, 6, 8], id="int-even"),
        pytest.param("int", [10, 20, 30, 40], id="int-multiples"),
        pytest.param("int", [1, 2, 3, 4, 5], id="int-sequential"),
        pytest.param("int", [0, 0, 0], id="int-zeros"),
        # Float arrays
        pytest.param("float", [2.5, 4.5, 6.5, 8.5], id="float-even"),
        pytest.param("float", [1.0, 2.0, 3.0], id="float-sequential"),
        pytest.param("float", [5.5, 5.5, 5.5], id="float-same"),
        # Boolean arrays
        pytest.param("bool", [True, False, True, False], id="bool-mixed"),
        pytest.param("bool", [True, True, True], id="bool-all-true"),
        pytest.param("bool", [False, False, False], id="bool-all-false"),
    ],
)
async def test_array_std(ctx, data_type, values):
    """Test std() on arrays across numeric types. Returns Object, use .data() to extract value."""
    obj = await create_object_from_value(values)

    result_obj = await obj.std()
    result = await result_obj.data()
    expected = np.std(values, ddof=0)

    assert abs(result - expected) < THRESHOLD


# =============================================================================
# Statistics After Operations Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,array_a,array_b,operator",
    [
        # Integer operations
        pytest.param("int", [10, 20, 30], [5, 10, 15], "+", id="int-add"),
        pytest.param("int", [100, 200, 300], [10, 20, 30], "-", id="int-sub"),
        pytest.param("int", [1, 2, 3], [10, 20, 30], "+", id="int-add-small"),
        # Float operations
        pytest.param("float", [10.0, 20.0, 30.0], [5.0, 10.0, 15.0], "+", id="float-add"),
        pytest.param("float", [100.5, 200.5, 300.5], [10.5, 20.5, 30.5], "-", id="float-sub"),
        pytest.param("float", [1.5, 2.5], [3.5, 4.5], "+", id="float-add-small"),
    ],
)
async def test_statistics_after_operation(ctx, data_type, array_a, array_b, operator):
    """Test statistics on result of arithmetic operations. Returns Objects, use .data() to extract values."""
    obj_a = await create_object_from_value(array_a)
    obj_b = await create_object_from_value(array_b)

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

    # Test all statistics (now return Objects, use .data() to extract values)
    assert abs(await (await result.min()).data() - np.min(expected_values)) < THRESHOLD
    assert abs(await (await result.max()).data() - np.max(expected_values)) < THRESHOLD
    assert abs(await (await result.sum()).data() - np.sum(expected_values)) < THRESHOLD
    assert abs(await (await result.mean()).data() - np.mean(expected_values)) < THRESHOLD
    assert abs(await (await result.std()).data() - np.std(expected_values, ddof=0)) < THRESHOLD


# =============================================================================
# Single Value Statistics Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,value",
    [
        # Single element arrays
        pytest.param("int", [42], id="int-positive"),
        pytest.param("float", [42.5], id="float-positive"),
        pytest.param("int", [0], id="int-zero"),
        pytest.param("float", [0.0], id="float-zero"),
        pytest.param("int", [-100], id="int-negative"),
        pytest.param("float", [-100.5], id="float-negative"),
    ],
)
async def test_single_value_statistics(ctx, data_type, value):
    """Test statistics on single-element arrays. Returns Objects, use .data() to extract values."""
    obj = await create_object_from_value(value)

    expected_val = float(value[0])

    assert abs(await (await obj.min()).data() - expected_val) < THRESHOLD
    assert abs(await (await obj.max()).data() - expected_val) < THRESHOLD
    assert abs(await (await obj.sum()).data() - expected_val) < THRESHOLD
    assert abs(await (await obj.mean()).data() - expected_val) < THRESHOLD
    assert abs(await (await obj.std()).data() - 0.0) < THRESHOLD


# =============================================================================
# Special Cases Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,values,expected_min,expected_max,expected_sum,expected_mean,expected_std",
    [
        # All same values (std should be 0)
        pytest.param("int", [5, 5, 5, 5], 5, 5, 20, 5.0, 0.0, id="int-all-same"),
        pytest.param("float", [10.5, 10.5, 10.5], 10.5, 10.5, 31.5, 10.5, 0.0, id="float-all-same"),
        # All True boolean array
        pytest.param("bool", [True, True, True], 1, 1, 3, 1.0, 0.0, id="bool-all-true"),
        # All False boolean array
        pytest.param("bool", [False, False, False], 0, 0, 0, 0.0, 0.0, id="bool-all-false"),
        # Mixed zeros and non-zeros
        pytest.param("int", [0, 5, 0, 5], 0, 5, 10, 2.5, 2.5, id="int-mixed-zeros"),
    ],
)
async def test_special_cases(ctx, data_type, values, expected_min, expected_max, expected_sum, expected_mean, expected_std):
    """Test statistics on special case arrays. Returns Objects, use .data() to extract values."""
    obj = await create_object_from_value(values)

    assert abs(await (await obj.min()).data() - expected_min) < THRESHOLD
    assert abs(await (await obj.max()).data() - expected_max) < THRESHOLD
    assert abs(await (await obj.sum()).data() - expected_sum) < THRESHOLD
    assert abs(await (await obj.mean()).data() - expected_mean) < THRESHOLD
    assert abs(await (await obj.std()).data() - expected_std) < THRESHOLD


# =============================================================================
# Negative Numbers Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,values",
    [
        # All negative integers
        pytest.param("int", [-10, -20, -30, -40], id="int-all-negative"),
        # All negative floats
        pytest.param("float", [-1.5, -2.5, -3.5], id="float-all-negative"),
        # Mixed positive and negative
        pytest.param("int", [-5, 5, -10, 10], id="int-mixed"),
        pytest.param("float", [-2.5, 2.5, -5.0, 5.0], id="float-mixed"),
    ],
)
async def test_negative_numbers_statistics(ctx, data_type, values):
    """Test statistics with negative numbers. Returns Objects, use .data() to extract values."""
    obj = await create_object_from_value(values)

    expected_min = np.min(values)
    expected_max = np.max(values)
    expected_sum = np.sum(values)
    expected_mean = np.mean(values)
    expected_std = np.std(values, ddof=0)

    assert abs(await (await obj.min()).data() - expected_min) < THRESHOLD
    assert abs(await (await obj.max()).data() - expected_max) < THRESHOLD
    assert abs(await (await obj.sum()).data() - expected_sum) < THRESHOLD
    assert abs(await (await obj.mean()).data() - expected_mean) < THRESHOLD
    assert abs(await (await obj.std()).data() - expected_std) < THRESHOLD


# =============================================================================
# Count Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,values,expected_count",
    [
        # Integer arrays
        pytest.param("int", [1, 2, 3, 4, 5], 5, id="int-five"),
        pytest.param("int", [42], 1, id="int-single"),
        pytest.param("int", [0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 10, id="int-ten-zeros"),
        # Float arrays
        pytest.param("float", [1.1, 2.2, 3.3], 3, id="float-three"),
        pytest.param("float", [3.14], 1, id="float-single"),
        pytest.param("float", [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0], 7, id="float-seven"),
        # Boolean arrays
        pytest.param("bool", [True, False, True], 3, id="bool-three"),
        pytest.param("bool", [True], 1, id="bool-single"),
        pytest.param("bool", [False, False, False, False], 4, id="bool-four-false"),
    ],
)
async def test_array_count(ctx, data_type, values, expected_count):
    """Test count() on arrays across numeric types. Returns Object, use .data() to extract value."""
    obj = await create_object_from_value(values)

    result_obj = await obj.count()
    result = await result_obj.data()

    assert result == expected_count


# =============================================================================
# Var Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,values",
    [
        # Integer arrays
        pytest.param("int", [2, 4, 6, 8], id="int-even"),
        pytest.param("int", [10, 20, 30, 40], id="int-multiples"),
        pytest.param("int", [1, 2, 3, 4, 5], id="int-sequential"),
        pytest.param("int", [0, 0, 0], id="int-zeros"),
        # Float arrays
        pytest.param("float", [2.5, 4.5, 6.5, 8.5], id="float-even"),
        pytest.param("float", [1.0, 2.0, 3.0], id="float-sequential"),
        pytest.param("float", [5.5, 5.5, 5.5], id="float-same"),
        # Boolean arrays
        pytest.param("bool", [True, False, True, False], id="bool-mixed"),
        pytest.param("bool", [True, True, True], id="bool-all-true"),
        pytest.param("bool", [False, False, False], id="bool-all-false"),
    ],
)
async def test_array_var(ctx, data_type, values):
    """Test var() on arrays across numeric types. Returns Object, use .data() to extract value."""
    obj = await create_object_from_value(values)

    result_obj = await obj.var()
    result = await result_obj.data()
    expected = np.var(values, ddof=0)

    assert abs(result - expected) < THRESHOLD


async def test_var_equals_std_squared(ctx):
    """Test that var() equals std()^2."""
    values = [2, 4, 6, 8, 10]
    obj = await create_object_from_value(values)

    std_result = await (await obj.std()).data()
    var_result = await (await obj.var()).data()

    assert abs(var_result - std_result**2) < THRESHOLD


# =============================================================================
# Quantile Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,values,quantile_level,expected_approx",
    [
        # Median tests (q=0.5)
        pytest.param("int", [1, 2, 3, 4, 5], 0.5, 3.0, id="int-median-odd"),
        pytest.param("int", [1, 2, 3, 4, 5, 6], 0.5, 3.5, id="int-median-even"),
        pytest.param("float", [1.0, 2.0, 3.0, 4.0, 5.0], 0.5, 3.0, id="float-median"),
        # Min/Max quantiles
        pytest.param("int", [1, 2, 3, 4, 5], 0.0, 1.0, id="int-q0"),
        pytest.param("int", [1, 2, 3, 4, 5], 1.0, 5.0, id="int-q1"),
        # Quartiles
        pytest.param("int", [1, 2, 3, 4, 5, 6, 7, 8], 0.25, 2.5, id="int-q25"),
        pytest.param("int", [1, 2, 3, 4, 5, 6, 7, 8], 0.75, 6.5, id="int-q75"),
        # Single element
        pytest.param("int", [42], 0.5, 42.0, id="int-single-median"),
        pytest.param("float", [3.14], 0.5, 3.14, id="float-single-median"),
    ],
)
async def test_array_quantile(ctx, data_type, values, quantile_level, expected_approx):
    """Test quantile() on arrays. Note: ClickHouse quantile uses approximate algorithm."""
    obj = await create_object_from_value(values)

    result_obj = await obj.quantile(quantile_level)
    result = await result_obj.data()

    # ClickHouse quantile uses approximate algorithm, allow larger threshold
    assert abs(result - expected_approx) < 1.0


async def test_quantile_invalid_level(ctx):
    """Test that quantile() raises ValueError for invalid quantile levels."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])

    with pytest.raises(ValueError):
        await obj.quantile(-0.1)

    with pytest.raises(ValueError):
        await obj.quantile(1.1)
