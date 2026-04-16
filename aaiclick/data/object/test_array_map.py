"""
Tests for Object.array_map() method.

Verifies element-wise operations using ClickHouse's arrayMap function,
including array-array, array-scalar, and size mismatch error handling.
"""

import pytest

from aaiclick import create_object_from_value

THRESHOLD = 1e-5


# =============================================================================
# Array-Array: element-wise operations via arrayMap
# =============================================================================


@pytest.mark.parametrize(
    "a_vals,b_vals,operator,expected",
    [
        pytest.param([1, 2, 3], [10, 20, 30], "+", [11, 22, 33], id="add"),
        pytest.param([10, 20, 30], [1, 2, 3], "-", [9, 18, 27], id="sub"),
        pytest.param([2, 3, 4], [5, 6, 7], "*", [10, 18, 28], id="mul"),
        pytest.param([10.0, 20.0, 30.0], [2.0, 4.0, 5.0], "/", [5.0, 5.0, 6.0], id="div"),
        pytest.param([10, 23, 37], [3, 5, 7], "//", [3, 4, 5], id="floordiv"),
        pytest.param([10, 23, 37], [3, 5, 7], "%", [1, 3, 2], id="mod"),
        pytest.param([2.0, 3.0, 4.0], [3.0, 2.0, 2.0], "**", [8.0, 9.0, 16.0], id="pow"),
    ],
)
async def test_array_map_arithmetic(ctx, a_vals, b_vals, operator, expected):
    """Test array_map with arithmetic operators."""
    a = await create_object_from_value(a_vals)
    b = await create_object_from_value(b_vals)
    result = await a.array_map(b, operator)
    data = await result.data()
    assert len(data) == len(expected)
    for actual, exp in zip(data, expected, strict=False):
        assert abs(actual - exp) < THRESHOLD


@pytest.mark.parametrize(
    "a_vals,b_vals,operator,expected",
    [
        pytest.param([1, 2, 3], [1, 2, 4], "==", [1, 1, 0], id="eq"),
        pytest.param([1, 2, 3], [1, 2, 4], "!=", [0, 0, 1], id="ne"),
        pytest.param([1, 5, 3], [2, 4, 3], "<", [1, 0, 0], id="lt"),
        pytest.param([1, 5, 3], [2, 4, 3], "<=", [1, 0, 1], id="le"),
        pytest.param([1, 5, 3], [2, 4, 3], ">", [0, 1, 0], id="gt"),
        pytest.param([1, 5, 3], [2, 4, 3], ">=", [0, 1, 1], id="ge"),
    ],
)
async def test_array_map_comparison(ctx, a_vals, b_vals, operator, expected):
    """Test array_map with comparison operators."""
    a = await create_object_from_value(a_vals)
    b = await create_object_from_value(b_vals)
    result = await a.array_map(b, operator)
    data = await result.data()
    assert data == expected


@pytest.mark.parametrize(
    "a_vals,b_vals,operator,expected",
    [
        pytest.param([0b1100, 0b1010], [0b1010, 0b0110], "&", [0b1000, 0b0010], id="and"),
        pytest.param([0b1100, 0b1010], [0b1010, 0b0110], "|", [0b1110, 0b1110], id="or"),
        pytest.param([0b1100, 0b1010], [0b1010, 0b0110], "^", [0b0110, 0b1100], id="xor"),
    ],
)
async def test_array_map_bitwise(ctx, a_vals, b_vals, operator, expected):
    """Test array_map with bitwise operators."""
    a = await create_object_from_value(a_vals)
    b = await create_object_from_value(b_vals)
    result = await a.array_map(b, operator)
    data = await result.data()
    assert data == expected


# =============================================================================
# Array-Scalar: broadcast scalar across array via arrayMap
# =============================================================================


@pytest.mark.parametrize(
    "a_vals,scalar,operator,expected",
    [
        pytest.param([1, 2, 3], 10, "+", [11, 12, 13], id="add_scalar"),
        pytest.param([10, 20, 30], 5, "-", [5, 15, 25], id="sub_scalar"),
        pytest.param([2, 3, 4], 5, "*", [10, 15, 20], id="mul_scalar"),
        pytest.param([10.0, 20.0, 30.0], 2.0, "/", [5.0, 10.0, 15.0], id="div_scalar"),
        pytest.param([2.0, 3.0, 4.0], 2.0, "**", [4.0, 9.0, 16.0], id="pow_scalar"),
    ],
)
async def test_array_map_scalar(ctx, a_vals, scalar, operator, expected):
    """Test array_map with Python scalar operand."""
    a = await create_object_from_value(a_vals)
    result = await a.array_map(scalar, operator)
    data = await result.data()
    assert len(data) == len(expected)
    for actual, exp in zip(data, expected, strict=False):
        assert abs(actual - exp) < THRESHOLD


# =============================================================================
# Size mismatch: arrayMap raises error when array sizes differ
# =============================================================================


async def test_array_map_size_mismatch(ctx):
    """arrayMap raises an error when array sizes don't match."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20])
    with pytest.raises(Exception, match="equal size"):
        await a.array_map(b, "+")


# =============================================================================
# Invalid operator
# =============================================================================


async def test_array_map_invalid_operator(ctx):
    """array_map raises ValueError for unsupported operator."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([4, 5, 6])
    with pytest.raises(ValueError, match="Unsupported operator"):
        await a.array_map(b, "invalid")
