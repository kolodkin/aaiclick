"""
Tests for scalar broadcast operators.

Verifies that Object operators work with Python scalar operands (int, float)
on both left and right sides, for both scalar and array Objects.
"""

import pytest

from aaiclick import create_object_from_value

THRESHOLD = 1e-5


# =============================================================================
# Scalar Object + Python scalar (obj + 5, obj * 2, etc.)
# =============================================================================


@pytest.mark.parametrize(
    "obj_val,scalar,operator,expected",
    [
        # Arithmetic
        pytest.param(10, 5, "+", 15, id="add"),
        pytest.param(10, 3, "-", 7, id="sub"),
        pytest.param(10, 3, "*", 30, id="mul"),
        pytest.param(10.0, 4.0, "/", 2.5, id="div"),
        pytest.param(10, 3, "//", 3, id="floordiv"),
        pytest.param(10, 3, "%", 1, id="mod"),
        pytest.param(2.0, 3.0, "**", 8.0, id="pow"),
    ],
)
async def test_scalar_obj_op_scalar(ctx, obj_val, scalar, operator, expected):
    """Test scalar Object <op> Python scalar."""
    obj = await create_object_from_value(obj_val)

    match operator:
        case "+":
            result = await (obj + scalar)
        case "-":
            result = await (obj - scalar)
        case "*":
            result = await (obj * scalar)
        case "/":
            result = await (obj / scalar)
        case "//":
            result = await (obj // scalar)
        case "%":
            result = await (obj % scalar)
        case "**":
            result = await (obj**scalar)

    data = await result.data()
    assert abs(data - expected) < THRESHOLD


# =============================================================================
# Python scalar + Scalar Object (5 + obj, 2 * obj, etc.) - reverse operators
# =============================================================================


@pytest.mark.parametrize(
    "scalar,obj_val,operator,expected",
    [
        pytest.param(5, 10, "+", 15, id="radd"),
        pytest.param(20, 7, "-", 13, id="rsub"),
        pytest.param(3, 10, "*", 30, id="rmul"),
        pytest.param(10.0, 4.0, "/", 2.5, id="rtruediv"),
        pytest.param(10, 3, "//", 3, id="rfloordiv"),
        pytest.param(10, 3, "%", 1, id="rmod"),
        pytest.param(2.0, 3.0, "**", 8.0, id="rpow"),
    ],
)
async def test_scalar_reverse_op(ctx, scalar, obj_val, operator, expected):
    """Test Python scalar <op> scalar Object (reverse operators)."""
    obj = await create_object_from_value(obj_val)

    match operator:
        case "+":
            result = await (scalar + obj)
        case "-":
            result = await (scalar - obj)
        case "*":
            result = await (scalar * obj)
        case "/":
            result = await (scalar / obj)
        case "//":
            result = await (scalar // obj)
        case "%":
            result = await (scalar % obj)
        case "**":
            result = await (scalar**obj)

    data = await result.data()
    assert abs(data - expected) < THRESHOLD


# =============================================================================
# Array Object + Python scalar (broadcast scalar across array)
# =============================================================================


@pytest.mark.parametrize(
    "arr,scalar,operator,expected",
    [
        pytest.param([1, 2, 3], 10, "+", [11, 12, 13], id="add"),
        pytest.param([10, 20, 30], 5, "-", [5, 15, 25], id="sub"),
        pytest.param([1, 2, 3], 10, "*", [10, 20, 30], id="mul"),
        pytest.param([10.0, 20.0, 30.0], 10.0, "/", [1.0, 2.0, 3.0], id="div"),
        pytest.param([10, 25, 30], 7, "//", [1, 3, 4], id="floordiv"),
        pytest.param([10, 25, 30], 7, "%", [3, 4, 2], id="mod"),
        pytest.param([2.0, 3.0, 4.0], 2.0, "**", [4.0, 9.0, 16.0], id="pow"),
    ],
)
async def test_array_obj_op_scalar(ctx, arr, scalar, operator, expected):
    """Test array Object <op> Python scalar (broadcast)."""
    obj = await create_object_from_value(arr)

    match operator:
        case "+":
            result = await (obj + scalar)
        case "-":
            result = await (obj - scalar)
        case "*":
            result = await (obj * scalar)
        case "/":
            result = await (obj / scalar)
        case "//":
            result = await (obj // scalar)
        case "%":
            result = await (obj % scalar)
        case "**":
            result = await (obj**scalar)

    data = await result.data()
    for i, val in enumerate(data):
        assert abs(val - expected[i]) < THRESHOLD


# =============================================================================
# Python scalar + Array Object (broadcast with reverse operators)
# =============================================================================


@pytest.mark.parametrize(
    "scalar,arr,operator,expected",
    [
        pytest.param(10, [1, 2, 3], "+", [11, 12, 13], id="radd"),
        pytest.param(100, [10, 20, 30], "-", [90, 80, 70], id="rsub"),
        pytest.param(10, [1, 2, 3], "*", [10, 20, 30], id="rmul"),
        pytest.param(100.0, [10.0, 20.0, 50.0], "/", [10.0, 5.0, 2.0], id="rtruediv"),
        pytest.param(100, [7, 13, 33], "//", [14, 7, 3], id="rfloordiv"),
        pytest.param(10, [3, 4, 7], "%", [1, 2, 3], id="rmod"),
        pytest.param(2.0, [1.0, 2.0, 3.0], "**", [2.0, 4.0, 8.0], id="rpow"),
    ],
)
async def test_scalar_op_array_obj(ctx, scalar, arr, operator, expected):
    """Test Python scalar <op> array Object (reverse broadcast)."""
    obj = await create_object_from_value(arr)

    match operator:
        case "+":
            result = await (scalar + obj)
        case "-":
            result = await (scalar - obj)
        case "*":
            result = await (scalar * obj)
        case "/":
            result = await (scalar / obj)
        case "//":
            result = await (scalar // obj)
        case "%":
            result = await (scalar % obj)
        case "**":
            result = await (scalar**obj)

    data = await result.data()
    for i, val in enumerate(data):
        assert abs(val - expected[i]) < THRESHOLD


# =============================================================================
# Comparison operators with scalar broadcast
# =============================================================================


@pytest.mark.parametrize(
    "arr,scalar,operator,expected",
    [
        pytest.param([1, 5, 10], 5, "==", [0, 1, 0], id="eq"),
        pytest.param([1, 5, 10], 5, "!=", [1, 0, 1], id="ne"),
        pytest.param([1, 5, 10], 5, "<", [1, 0, 0], id="lt"),
        pytest.param([1, 5, 10], 5, "<=", [1, 1, 0], id="le"),
        pytest.param([1, 5, 10], 5, ">", [0, 0, 1], id="gt"),
        pytest.param([1, 5, 10], 5, ">=", [0, 1, 1], id="ge"),
    ],
)
async def test_comparison_with_scalar(ctx, arr, scalar, operator, expected):
    """Test comparison operators with scalar broadcast."""
    obj = await create_object_from_value(arr)

    match operator:
        case "==":
            result = await (obj == scalar)
        case "!=":
            result = await (obj != scalar)
        case "<":
            result = await (obj < scalar)
        case "<=":
            result = await (obj <= scalar)
        case ">":
            result = await (obj > scalar)
        case ">=":
            result = await (obj >= scalar)

    data = await result.data()
    assert data == expected


# =============================================================================
# Chained operations mixing scalars and Objects
# =============================================================================


async def test_chained_scalar_broadcast(ctx):
    """Test chained operations with scalar broadcast: (arr * 2) + 10."""
    obj = await create_object_from_value([1, 2, 3])
    result = await (await (obj * 2) + 10)
    data = await result.data()
    assert data == [12, 14, 16]


async def test_normalize_with_scalar_broadcast(ctx):
    """Test normalization pattern: arr / sum."""
    obj = await create_object_from_value([2.0, 4.0, 6.0, 8.0])
    total = await obj.sum()
    normalized = await (obj / total)
    data = await normalized.data()
    expected = [0.1, 0.2, 0.3, 0.4]
    for i, val in enumerate(data):
        assert abs(val - expected[i]) < THRESHOLD


async def test_scalar_sub_is_noncommutative(ctx):
    """Test that scalar - obj != obj - scalar (order matters)."""
    obj = await create_object_from_value([10, 20, 30])

    forward = await (obj - 5)
    reverse = await (5 - obj)

    forward_data = await forward.data()
    reverse_data = await reverse.data()

    assert forward_data == [5, 15, 25]
    assert reverse_data == [-5, -15, -25]


async def test_scalar_div_is_noncommutative(ctx):
    """Test that scalar / obj != obj / scalar (order matters)."""
    obj = await create_object_from_value([2.0, 4.0, 5.0])

    forward = await (obj / 10.0)
    reverse = await (10.0 / obj)

    forward_data = await forward.data()
    reverse_data = await reverse.data()

    for i, val in enumerate(forward_data):
        assert abs(val - [0.2, 0.4, 0.5][i]) < THRESHOLD
    for i, val in enumerate(reverse_data):
        assert abs(val - [5.0, 2.5, 2.0][i]) < THRESHOLD
