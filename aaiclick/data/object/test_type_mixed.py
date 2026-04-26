"""
Tests for mixed data types (int + float combinations) - scalars, arrays, and operators.

This file tests operations between different numeric types to ensure proper
type coercion and result accuracy.
"""

import numpy as np
import pytest

from aaiclick import create_object_from_value

THRESHOLD = 1e-5


# =============================================================================
# Scalar Tests - Int + Float
# =============================================================================


@pytest.mark.parametrize(
    "val_a,val_b,operator,expected",
    [
        pytest.param(100, 50.5, "+", 150.5, id="int-plus-float"),
        pytest.param(100.5, 50, "+", 150.5, id="float-plus-int"),
        pytest.param(100, 30.5, "-", 69.5, id="int-minus-float"),
        pytest.param(100.5, 30, "-", 70.5, id="float-minus-int"),
        pytest.param(0, 3.14159, "+", 3.14159, id="int-zero-plus-float"),
        pytest.param(0.0, 42, "+", 42.0, id="float-zero-plus-int"),
    ],
)
async def test_mixed_scalar_ops(ctx, val_a, val_b, operator, expected):
    """Test mixed int/float scalar arithmetic."""
    a = await create_object_from_value(val_a, aai_id=True)
    b = await create_object_from_value(val_b, aai_id=True)

    va, vb = a.view(order_by="value"), b.view(order_by="value")
    result = await (va + vb) if operator == "+" else await (va - vb)
    data = sorted(await result.data(order_by="value"))

    assert abs(data - expected) < THRESHOLD


# =============================================================================
# Array Tests - Int + Float
# =============================================================================


@pytest.mark.parametrize(
    "arr_a,arr_b,operator,expected",
    [
        pytest.param([1, 2, 3], [0.5, 1.5, 2.5], "+", [1.5, 3.5, 5.5], id="int-plus-float"),
        pytest.param([10.0, 20.0, 30.0], [1, 2, 3], "+", [11.0, 22.0, 33.0], id="float-plus-int"),
        pytest.param([100, 200, 300], [10.5, 20.5, 30.5], "-", [89.5, 179.5, 269.5], id="int-minus-float"),
        pytest.param([100.5, 200.5, 300.5], [10, 20, 30], "-", [90.5, 180.5, 270.5], id="float-minus-int"),
        pytest.param(
            [1000000, 2000000, 3000000],
            [0.001, 0.002, 0.003],
            "+",
            [1000000.001, 2000000.002, 3000000.003],
            id="large-int-small-float",
        ),
        pytest.param([-10, -20, -30], [5.5, 10.5, 15.5], "+", [-4.5, -9.5, -14.5], id="negative-int-plus-float"),
    ],
)
async def test_mixed_array_ops(ctx, arr_a, arr_b, operator, expected):
    """Test element-wise mixed int/float array arithmetic."""
    a = await create_object_from_value(arr_a, aai_id=True)
    b = await create_object_from_value(arr_b, aai_id=True)

    va, vb = a.view(order_by="value"), b.view(order_by="value")
    result = await (va + vb) if operator == "+" else await (va - vb)
    data = sorted(await result.data(order_by="value"))
    for i, val in enumerate(sorted(data)):
        assert abs(val - sorted(expected)[i]) < THRESHOLD


# =============================================================================
# Chained Operations with Mixed Types
# =============================================================================


@pytest.mark.parametrize(
    "arr_a,arr_b,arr_c,op1,op2,expected",
    [
        pytest.param(
            [10, 20, 30], [5.5, 10.5, 15.5], [3, 6, 9], "+", "-", [12.5, 24.5, 36.5], id="int-plus-float-minus-int"
        ),
        pytest.param(
            [100.5, 200.5], [10, 20], [5.25, 10.25], "-", "+", [95.75, 190.75], id="float-minus-int-plus-float"
        ),
        pytest.param(
            [1, 2, 3], [0.5, 1.0, 1.5], [10, 20, 30], "+", "+", [11.5, 23.0, 34.5], id="int-plus-float-plus-int"
        ),
    ],
)
async def test_mixed_chained_ops(ctx, arr_a, arr_b, arr_c, op1, op2, expected):
    """Test chained mixed int/float operations."""
    a = await create_object_from_value(arr_a, aai_id=True)
    b = await create_object_from_value(arr_b, aai_id=True)
    c = await create_object_from_value(arr_c, aai_id=True)

    va, vb, vc = a.view(order_by="value"), b.view(order_by="value"), c.view(order_by="value")
    temp = await (va + vb) if op1 == "+" else await (va - vb)
    # result is same-table (temp) + cross-table (c), so wrap temp too
    tv = temp.view(order_by="value")
    result = await (tv + vc) if op2 == "+" else await (tv - vc)
    data = sorted(await result.data(order_by="value"))

    for i, val in enumerate(data):
        assert abs(val - expected[i]) < THRESHOLD


# =============================================================================
# Statistics Tests on Mixed Type Results
# =============================================================================


async def test_mixed_statistics_after_operation(ctx):
    """Test statistics on result of mixed type operations. Returns Objects, use .data() to extract values."""
    a = await create_object_from_value([10, 20, 30, 40], aai_id=True)
    b = await create_object_from_value([0.5, 1.5, 2.5, 3.5], aai_id=True)

    result = await (a.view(order_by="value") + b.view(order_by="value"))

    expected_values = np.array([10.5, 21.5, 32.5, 43.5])

    assert abs(await (await result.min()).data() - np.min(expected_values)) < THRESHOLD
    assert abs(await (await result.max()).data() - np.max(expected_values)) < THRESHOLD
    assert abs(await (await result.sum()).data() - np.sum(expected_values)) < THRESHOLD
    assert abs(await (await result.mean()).data() - np.mean(expected_values)) < THRESHOLD
    assert abs(await (await result.std()).data() - np.std(expected_values, ddof=0)) < THRESHOLD


async def test_mixed_min_max_after_subtraction(ctx):
    """Test min/max on result of mixed type subtraction. Returns Objects, use .data() to extract values."""
    a = await create_object_from_value([100, 200, 300], aai_id=True)
    b = await create_object_from_value([0.1, 0.2, 0.3], aai_id=True)

    result = await (a.view(order_by="value") - b.view(order_by="value"))

    assert abs(await (await result.min()).data() - 99.9) < THRESHOLD
    assert abs(await (await result.max()).data() - 299.7) < THRESHOLD


async def test_mixed_sum_mean_precision(ctx):
    """Test sum and mean with mixed types requiring precision. Returns Objects, use .data() to extract values."""
    # Create arrays where int + float requires precision
    a = await create_object_from_value([1, 2, 3, 4, 5], aai_id=True)
    b = await create_object_from_value([0.1, 0.2, 0.3, 0.4, 0.5], aai_id=True)

    result = await (a.view(order_by="value") + b.view(order_by="value"))

    expected_values = np.array([1.1, 2.2, 3.3, 4.4, 5.5])
    expected_sum = np.sum(expected_values)
    expected_mean = np.mean(expected_values)

    assert abs(await (await result.sum()).data() - expected_sum) < THRESHOLD
    assert abs(await (await result.mean()).data() - expected_mean) < THRESHOLD


# =============================================================================
# Edge Cases with Mixed Types
# =============================================================================


async def test_mixed_single_element_arrays(ctx):
    """Test mixed type operations on single element arrays."""
    a = await create_object_from_value([42], aai_id=True)
    b = await create_object_from_value([0.5], aai_id=True)

    result = await (a.view(order_by="value") + b.view(order_by="value"))
    data = await result.data()

    assert abs(data[0] - 42.5) < THRESHOLD


async def test_mixed_very_small_float_with_large_int(ctx):
    """Test operations with very small float and large int."""
    a = await create_object_from_value([1000000], aai_id=True)
    b = await create_object_from_value([1e-10], aai_id=True)

    result = await (a.view(order_by="value") + b.view(order_by="value"))
    data = await result.data()

    # Result should be very close to 1000000 due to float precision
    assert abs(data[0] - 1000000.0000000001) < 1e-9


async def test_mixed_boundary_values(ctx):
    """Test mixed operations with boundary values."""
    # Test with zero and negative transitions
    a = await create_object_from_value([-1, 0, 1], aai_id=True)
    b = await create_object_from_value([0.5, 0.5, 0.5], aai_id=True)

    result = await (a.view(order_by="value") + b.view(order_by="value"))
    data = await result.data()

    expected = [-0.5, 0.5, 1.5]
    for i, val in enumerate(data):
        assert abs(val - expected[i]) < THRESHOLD


async def test_mixed_symmetry(ctx):
    """Test that int+float == float+int (commutative property)."""
    # Test with arrays
    int_array = [10, 20, 30]
    float_array = [1.5, 2.5, 3.5]

    # int + float
    a1 = await create_object_from_value(int_array, aai_id=True)
    b1 = await create_object_from_value(float_array, aai_id=True)
    result1 = await (a1.view(order_by="value") + b1.view(order_by="value"))
    data1 = await result1.data(order_by="value")

    # float + int
    a2 = await create_object_from_value(float_array, aai_id=True)
    b2 = await create_object_from_value(int_array, aai_id=True)
    result2 = await (a2.view(order_by="value") + b2.view(order_by="value"))
    data2 = await result2.data(order_by="value")

    # Results should be identical
    for i in range(len(data1)):
        assert abs(data1[i] - data2[i]) < THRESHOLD


# =============================================================================
# Concat Tests with Mixed Types
# =============================================================================


@pytest.mark.parametrize(
    "arr_a,arr_b",
    [
        pytest.param([1, 2, 3], [4.5, 5.5, 6.5], id="int-float"),
        pytest.param([1.5, 2.5, 3.5], [4, 5, 6], id="float-int"),
        pytest.param([1, 2, 3], ["a", "b", "c"], id="int-str"),
    ],
)
async def test_mixed_type_concat_fails(ctx, arr_a, arr_b):
    """Test that concatenating incompatible types fails with type error."""
    a = await create_object_from_value(arr_a, aai_id=True)
    b = await create_object_from_value(arr_b, aai_id=True)

    with pytest.raises(ValueError, match="incompatible type"):
        await a.concat(b)


# =============================================================================
# Insert Tests with Mixed Types
# =============================================================================


async def test_mixed_int_float_insert_succeeds(ctx):
    """Test that inserting float array into int array succeeds (ClickHouse allows casting)."""
    a = await create_object_from_value([1, 2, 3], aai_id=True)
    b = await create_object_from_value([4.5, 5.5, 6.5], aai_id=True)

    await a.insert(b)
    data = await a.data()

    # Float values get truncated when cast to int
    assert data == [1, 2, 3, 4, 5, 6]


async def test_mixed_float_int_insert_succeeds(ctx):
    """Test that inserting int array into float array succeeds (ClickHouse allows casting)."""
    a = await create_object_from_value([1.5, 2.5, 3.5], aai_id=True)
    b = await create_object_from_value([4, 5, 6], aai_id=True)

    await a.insert(b)
    data = await a.data()

    # Int values get converted to float
    assert data == [1.5, 2.5, 3.5, 4.0, 5.0, 6.0]


async def test_mixed_int_string_insert_fails(ctx):
    """Test that inserting string array into int array fails with type error."""
    a = await create_object_from_value([1, 2, 3], aai_id=True)
    b = await create_object_from_value(["a", "b", "c"], aai_id=True)

    with pytest.raises(ValueError, match="types are incompatible"):
        await a.insert(b)


async def test_mixed_insert_float_value_into_int_succeeds(ctx):
    """Test that inserting float value into int array succeeds (truncates)."""
    a = await create_object_from_value([1, 2, 3], aai_id=True)

    await a.insert(4.5)
    data = await a.data()

    # Float value gets truncated when cast to int
    assert data == [1, 2, 3, 4]


async def test_mixed_insert_float_list_into_int_succeeds(ctx):
    """Test that inserting float list into int array succeeds (truncates)."""
    a = await create_object_from_value([1, 2, 3], aai_id=True)

    await a.insert([4.5, 5.5])
    data = await a.data()

    # Float values get truncated when cast to int
    assert data == [1, 2, 3, 4, 5]
