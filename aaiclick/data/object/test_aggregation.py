"""
Parametrized tests for aggregation operations across numeric data types.

Tests min, max, sum, mean, std, var, count, and quantile aggregation operators.
These operators reduce arrays to scalar values.
String type does not support aggregation.
"""

import math
import operator

import numpy as np
import pytest

from aaiclick import Object, create_object_from_value

THRESHOLD = 1e-5

NUM_ITEMS = 10000


# =============================================================================
# Min Tests
# =============================================================================


@pytest.mark.parametrize(
    "values,expected_result",
    [
        # Integer arrays
        pytest.param([5, 2, 8, 1, 9], 1, id="int-mixed"),
        pytest.param([10, 20, 30], 10, id="int-ascending"),
        pytest.param([-5, -10, -15], -15, id="int-negative"),
        pytest.param([42], 42, id="int-single"),
        pytest.param([0, 0, 0], 0, id="int-zeros"),
        # Float arrays
        pytest.param([5.5, 2.2, 8.8, 1.1, 9.9], 1.1, id="float-mixed"),
        pytest.param([10.0, 20.0, 30.0], 10.0, id="float-ascending"),
        pytest.param([-5.5, -10.5], -10.5, id="float-negative"),
        pytest.param([3.14], 3.14, id="float-single"),
        # Boolean arrays (as UInt8)
        pytest.param([True, False, True], 0, id="bool-mixed"),
        pytest.param([True, True, True], 1, id="bool-all-true"),
        pytest.param([False, False, False], 0, id="bool-all-false"),
    ],
)
async def test_array_min(ctx, values, expected_result):
    """Test min() on arrays across numeric types. Returns Object, use .data() to extract value."""
    obj = await create_object_from_value(values)

    result_obj = await obj.min()
    result = await result_obj.data()

    assert result == pytest.approx(expected_result, abs=THRESHOLD)


# =============================================================================
# Max Tests
# =============================================================================


@pytest.mark.parametrize(
    "values,expected_result",
    [
        # Integer arrays
        pytest.param([5, 2, 8, 1, 9], 9, id="int-mixed"),
        pytest.param([10, 20, 30], 30, id="int-ascending"),
        pytest.param([-5, -10, -15], -5, id="int-negative"),
        pytest.param([42], 42, id="int-single"),
        pytest.param([0, 0, 0], 0, id="int-zeros"),
        # Float arrays
        pytest.param([5.5, 2.2, 8.8, 1.1, 9.9], 9.9, id="float-mixed"),
        pytest.param([10.0, 20.0, 30.0], 30.0, id="float-ascending"),
        pytest.param([-5.5, -10.5], -5.5, id="float-negative"),
        pytest.param([3.14], 3.14, id="float-single"),
        # Boolean arrays (as UInt8)
        pytest.param([True, False, True], 1, id="bool-mixed"),
        pytest.param([True, True, True], 1, id="bool-all-true"),
        pytest.param([False, False, False], 0, id="bool-all-false"),
    ],
)
async def test_array_max(ctx, values, expected_result):
    """Test max() on arrays across numeric types. Returns Object, use .data() to extract value."""
    obj = await create_object_from_value(values)

    result_obj = await obj.max()
    result = await result_obj.data()

    assert result == pytest.approx(expected_result, abs=THRESHOLD)


# =============================================================================
# Sum Tests
# =============================================================================


@pytest.mark.parametrize(
    "values,expected_result",
    [
        # Integer arrays
        pytest.param([1, 2, 3, 4, 5], 15, id="int-sequential"),
        pytest.param([10, 20, 30], 60, id="int-multiples"),
        pytest.param([-5, -10, 5, 10], 0, id="int-canceling"),
        pytest.param([42], 42, id="int-single"),
        pytest.param([0, 0, 0], 0, id="int-zeros"),
        # Float arrays
        pytest.param([1.1, 2.2, 3.3, 4.4, 5.5], 16.5, id="float-sequential"),
        pytest.param([10.0, 20.0, 30.0], 60.0, id="float-multiples"),
        pytest.param([-5.5, 5.5], 0.0, id="float-canceling"),
        pytest.param([3.14], 3.14, id="float-single"),
        # Boolean arrays (counts True values)
        pytest.param([True, False, True, True, False], 3, id="bool-mixed"),
        pytest.param([True, True, True], 3, id="bool-all-true"),
        pytest.param([False, False, False], 0, id="bool-all-false"),
    ],
)
async def test_array_sum(ctx, values, expected_result):
    """Test sum() on arrays across numeric types. Returns Object, use .data() to extract value."""
    obj = await create_object_from_value(values)

    result_obj = await obj.sum()
    result = await result_obj.data()

    assert result == pytest.approx(expected_result, abs=THRESHOLD)


# =============================================================================
# Mean Tests
# =============================================================================


@pytest.mark.parametrize(
    "values,expected_result",
    [
        # Integer arrays
        pytest.param([10, 20, 30, 40], 25.0, id="int-multiples"),
        pytest.param([1, 2, 3, 4, 5], 3.0, id="int-sequential"),
        pytest.param([0, 0, 0], 0.0, id="int-zeros"),
        pytest.param([42], 42.0, id="int-single"),
        # Float arrays
        pytest.param([10.5, 20.5, 30.5, 40.5], 25.5, id="float-multiples"),
        pytest.param([1.0, 2.0, 3.0], 2.0, id="float-sequential"),
        pytest.param([3.14], 3.14, id="float-single"),
        # Boolean arrays (proportion of True values)
        pytest.param([True, False, True, False], 0.5, id="bool-half"),
        pytest.param([True, True, True], 1.0, id="bool-all-true"),
        pytest.param([False, False, False], 0.0, id="bool-all-false"),
    ],
)
async def test_array_mean(ctx, values, expected_result):
    """Test mean() on arrays across numeric types. Returns Object, use .data() to extract value."""
    obj = await create_object_from_value(values)

    result_obj = await obj.mean()
    result = await result_obj.data()

    assert abs(result - expected_result) < THRESHOLD


# =============================================================================
# Std Tests
# =============================================================================


@pytest.mark.parametrize(
    "values",
    [
        # Integer arrays
        pytest.param([2, 4, 6, 8], id="int-even"),
        pytest.param([10, 20, 30, 40], id="int-multiples"),
        pytest.param([1, 2, 3, 4, 5], id="int-sequential"),
        pytest.param([0, 0, 0], id="int-zeros"),
        # Float arrays
        pytest.param([2.5, 4.5, 6.5, 8.5], id="float-even"),
        pytest.param([1.0, 2.0, 3.0], id="float-sequential"),
        pytest.param([5.5, 5.5, 5.5], id="float-same"),
        # Boolean arrays
        pytest.param([True, False, True, False], id="bool-mixed"),
        pytest.param([True, True, True], id="bool-all-true"),
        pytest.param([False, False, False], id="bool-all-false"),
    ],
)
async def test_array_std(ctx, values):
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
    "array_a,array_b,op",
    [
        # Integer operations
        pytest.param([10, 20, 30], [5, 10, 15], operator.add, id="int-add"),
        pytest.param([100, 200, 300], [10, 20, 30], operator.sub, id="int-sub"),
        pytest.param([1, 2, 3], [10, 20, 30], operator.add, id="int-add-small"),
        # Float operations
        pytest.param([10.0, 20.0, 30.0], [5.0, 10.0, 15.0], operator.add, id="float-add"),
        pytest.param([100.5, 200.5, 300.5], [10.5, 20.5, 30.5], operator.sub, id="float-sub"),
        pytest.param([1.5, 2.5], [3.5, 4.5], operator.add, id="float-add-small"),
    ],
)
async def test_statistics_after_operation(ctx, array_a, array_b, op):
    """Test statistics on result of arithmetic operations. Returns Objects, use .data() to extract values."""
    obj_a = await create_object_from_value(array_a, aai_id=True)
    obj_b = await create_object_from_value(array_b, aai_id=True)

    result = op(obj_a, obj_b)
    expected_values = op(np.array(array_a), np.array(array_b))

    assert await result.min().data() == pytest.approx(np.min(expected_values), abs=THRESHOLD)
    assert await result.max().data() == pytest.approx(np.max(expected_values), abs=THRESHOLD)
    assert await result.sum().data() == pytest.approx(np.sum(expected_values), abs=THRESHOLD)
    assert await result.mean().data() == pytest.approx(np.mean(expected_values), abs=THRESHOLD)
    assert await result.std().data() == pytest.approx(np.std(expected_values, ddof=0), abs=THRESHOLD)


# =============================================================================
# Statistics Tests
# =============================================================================


@pytest.mark.parametrize(
    "values",
    [
        pytest.param([0, 5, 0, 5], id="int-mixed-zeros"),
        pytest.param([-10, -20, -30, -40], id="int-all-negative"),
        pytest.param([-1.5, -2.5, -3.5], id="float-all-negative"),
        # Mixed positive and negative
        pytest.param([-5, 5, -10, 10], id="int-mixed-sign"),
        pytest.param([-2.5, 2.5, -5.0, 5.0], id="float-mixed-sign"),
    ],
)
async def test_statistics(ctx, values):
    """min/max/sum/mean/std together against numpy for sign mixes the per-aggregation tests skip."""
    obj = await create_object_from_value(values)
    expected = np.array(values, dtype=float)

    assert await obj.min().data() == pytest.approx(np.min(expected), abs=THRESHOLD)
    assert await obj.max().data() == pytest.approx(np.max(expected), abs=THRESHOLD)
    assert await obj.sum().data() == pytest.approx(np.sum(expected), abs=THRESHOLD)
    assert await obj.mean().data() == pytest.approx(np.mean(expected), abs=THRESHOLD)
    assert await obj.std().data() == pytest.approx(np.std(expected, ddof=0), abs=THRESHOLD)


# =============================================================================
# Count Tests
# =============================================================================


@pytest.mark.parametrize(
    "values,expected_count",
    [
        # Integer arrays
        pytest.param([1, 2, 3, 4, 5], 5, id="int-five"),
        pytest.param([42], 1, id="int-single"),
        pytest.param([0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 10, id="int-ten-zeros"),
        # Float arrays
        pytest.param([1.1, 2.2, 3.3], 3, id="float-three"),
        pytest.param([3.14], 1, id="float-single"),
        pytest.param([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0], 7, id="float-seven"),
        # Boolean arrays
        pytest.param([True, False, True], 3, id="bool-three"),
        pytest.param([True], 1, id="bool-single"),
        pytest.param([False, False, False, False], 4, id="bool-four-false"),
    ],
)
async def test_array_count(ctx, values, expected_count):
    """Test count() on arrays across numeric types. Returns Object, use .data() to extract value."""
    obj = await create_object_from_value(values)

    result_obj = await obj.count()
    result = await result_obj.data()

    assert result == expected_count


# =============================================================================
# Var Tests
# =============================================================================


@pytest.mark.parametrize(
    "values",
    [
        # Integer arrays
        pytest.param([2, 4, 6, 8], id="int-even"),
        pytest.param([10, 20, 30, 40], id="int-multiples"),
        pytest.param([1, 2, 3, 4, 5], id="int-sequential"),
        pytest.param([0, 0, 0], id="int-zeros"),
        # Float arrays
        pytest.param([2.5, 4.5, 6.5, 8.5], id="float-even"),
        pytest.param([1.0, 2.0, 3.0], id="float-sequential"),
        pytest.param([5.5, 5.5, 5.5], id="float-same"),
        # Boolean arrays
        pytest.param([True, False, True, False], id="bool-mixed"),
        pytest.param([True, True, True], id="bool-all-true"),
        pytest.param([False, False, False], id="bool-all-false"),
    ],
)
async def test_array_var(ctx, values):
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

    std_result = await obj.std().data()
    var_result = await obj.var().data()

    assert abs(var_result - std_result**2) < THRESHOLD


# =============================================================================
# Quantile Tests
# =============================================================================


@pytest.mark.parametrize(
    "values,quantile_level,expected_approx",
    [
        # Median tests (q=0.5)
        pytest.param([1, 2, 3, 4, 5], 0.5, 3.0, id="int-median-odd"),
        pytest.param([1, 2, 3, 4, 5, 6], 0.5, 3.5, id="int-median-even"),
        pytest.param([1.0, 2.0, 3.0, 4.0, 5.0], 0.5, 3.0, id="float-median"),
        # Min/Max quantiles
        pytest.param([1, 2, 3, 4, 5], 0.0, 1.0, id="int-q0"),
        pytest.param([1, 2, 3, 4, 5], 1.0, 5.0, id="int-q1"),
        # Quartiles
        pytest.param([1, 2, 3, 4, 5, 6, 7, 8], 0.25, 2.5, id="int-q25"),
        pytest.param([1, 2, 3, 4, 5, 6, 7, 8], 0.75, 6.5, id="int-q75"),
        # Single element
        pytest.param([42], 0.5, 42.0, id="int-single-median"),
        pytest.param([3.14], 0.5, 3.14, id="float-single-median"),
    ],
)
async def test_array_quantile(ctx, values, quantile_level, expected_approx):
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


async def test_sum_of_comparison_does_not_wrap(ctx):
    """``(a == b).sum()`` counts every match: the UInt8 comparison result must
    widen to UInt64 before summing, or 300 matches wrap to 44."""
    values = list(range(300))
    obj_a = await create_object_from_value(values, aai_id=True)
    obj_b = await create_object_from_value(values, aai_id=True)

    assert await (await (obj_a == obj_b).sum()).data() == 300


# =============================================================================
# Large-array aggregation tests (NUM_ITEMS rows)
# =============================================================================


@pytest.mark.parametrize(
    "values, agg, expected",
    [
        pytest.param(list(range(100, NUM_ITEMS + 100)), Object.min, 100, id="min-int"),
        pytest.param([float(i) * 0.1 for i in range(NUM_ITEMS)], Object.max, 999.9, id="max-float"),
        pytest.param([1.5] * NUM_ITEMS, Object.sum, 1.5 * NUM_ITEMS, id="sum-float"),
        pytest.param(list(range(NUM_ITEMS)), Object.mean, (NUM_ITEMS - 1) / 2.0, id="mean-int"),
        # Population std of 0..N-1 is sqrt((N^2 - 1) / 12).
        pytest.param(
            [float(i) for i in range(NUM_ITEMS)], Object.std, math.sqrt((NUM_ITEMS**2 - 1) / 12.0), id="std-float"
        ),
    ],
)
async def test_large_array_aggregation(ctx, values, agg, expected):
    """Aggregations over NUM_ITEMS rows."""
    obj = await create_object_from_value(values, aai_id=True)

    result = await agg(obj)

    assert await result.data() == pytest.approx(expected)
