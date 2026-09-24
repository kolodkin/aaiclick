"""
Tests for scalar broadcast operators.

Verifies that Object operators work with Python scalar operands (int, float)
on both left and right sides, for both scalar and array Objects.
"""

import operator

import pytest

from aaiclick import create_object_from_value

THRESHOLD = 1e-5


# =============================================================================
# Object <op> Python scalar (obj + 5, arr * 2, etc.)
# =============================================================================


@pytest.mark.parametrize(
    "obj_val,scalar,op,expected",
    [
        pytest.param(10, 5, operator.add, 15, id="scalar-add"),
        pytest.param(10, 3, operator.sub, 7, id="scalar-sub"),
        pytest.param(10, 3, operator.mul, 30, id="scalar-mul"),
        pytest.param(10.0, 4.0, operator.truediv, 2.5, id="scalar-div"),
        pytest.param(10, 3, operator.floordiv, 3, id="scalar-floordiv"),
        pytest.param(10, 3, operator.mod, 1, id="scalar-mod"),
        pytest.param(2.0, 3.0, operator.pow, 8.0, id="scalar-pow"),
        pytest.param([1, 2, 3], 10, operator.add, [11, 12, 13], id="array-add"),
        pytest.param([10, 20, 30], 5, operator.sub, [5, 15, 25], id="array-sub"),
        pytest.param([1, 2, 3], 10, operator.mul, [10, 20, 30], id="array-mul"),
        pytest.param([10.0, 20.0, 30.0], 10.0, operator.truediv, [1.0, 2.0, 3.0], id="array-div"),
        pytest.param([10, 25, 30], 7, operator.floordiv, [1, 3, 4], id="array-floordiv"),
        pytest.param([10, 25, 30], 7, operator.mod, [3, 4, 2], id="array-mod"),
        pytest.param([2.0, 3.0, 4.0], 2.0, operator.pow, [4.0, 9.0, 16.0], id="array-pow"),
    ],
)
async def test_obj_op_scalar(ctx, obj_val, scalar, op, expected):
    """Scalar or array Object <op> Python scalar (broadcast)."""
    obj = await create_object_from_value(obj_val, aai_id=True)

    result = op(obj, scalar)

    assert await result.data() == pytest.approx(expected, abs=THRESHOLD)


# =============================================================================
# Python scalar <op> Object (5 + obj, 2 * arr, etc.) — reverse operators
# =============================================================================


@pytest.mark.parametrize(
    "scalar,obj_val,op,expected",
    [
        pytest.param(5, 10, operator.add, 15, id="scalar-radd"),
        pytest.param(20, 7, operator.sub, 13, id="scalar-rsub"),
        pytest.param(3, 10, operator.mul, 30, id="scalar-rmul"),
        pytest.param(10.0, 4.0, operator.truediv, 2.5, id="scalar-rtruediv"),
        pytest.param(10, 3, operator.floordiv, 3, id="scalar-rfloordiv"),
        pytest.param(10, 3, operator.mod, 1, id="scalar-rmod"),
        pytest.param(2.0, 3.0, operator.pow, 8.0, id="scalar-rpow"),
        pytest.param(10, [1, 2, 3], operator.add, [11, 12, 13], id="array-radd"),
        pytest.param(100, [10, 20, 30], operator.sub, [90, 80, 70], id="array-rsub"),
        pytest.param(10, [1, 2, 3], operator.mul, [10, 20, 30], id="array-rmul"),
        pytest.param(100.0, [10.0, 20.0, 50.0], operator.truediv, [10.0, 5.0, 2.0], id="array-rtruediv"),
        pytest.param(100, [7, 13, 33], operator.floordiv, [14, 7, 3], id="array-rfloordiv"),
        pytest.param(10, [3, 4, 7], operator.mod, [1, 2, 3], id="array-rmod"),
        pytest.param(2.0, [1.0, 2.0, 3.0], operator.pow, [2.0, 4.0, 8.0], id="array-rpow"),
    ],
)
async def test_scalar_op_obj(ctx, scalar, obj_val, op, expected):
    """Python scalar <op> scalar or array Object (reverse operators)."""
    obj = await create_object_from_value(obj_val, aai_id=True)

    result = op(scalar, obj)

    assert await result.data() == pytest.approx(expected, abs=THRESHOLD)


# =============================================================================
# Chained operations mixing scalars and Objects
# =============================================================================


async def test_chained_scalar_broadcast(ctx):
    """Test chained operations with scalar broadcast: (arr * 2) + 10."""
    obj = await create_object_from_value([1, 2, 3], aai_id=True)
    result = (obj * 2) + 10
    data = await result.data()
    assert data == [12, 14, 16]


async def test_normalize_with_scalar_broadcast(ctx):
    """Test normalization pattern: arr / sum."""
    obj = await create_object_from_value([2.0, 4.0, 6.0, 8.0], aai_id=True)
    total = await obj.sum()
    normalized = obj / total
    assert await normalized.data() == pytest.approx([0.1, 0.2, 0.3, 0.4], abs=THRESHOLD)


async def test_scalar_sub_is_noncommutative(ctx):
    """Test that scalar - obj != obj - scalar (order matters)."""
    obj = await create_object_from_value([10, 20, 30], aai_id=True)

    forward = obj - 5
    reverse = 5 - obj

    forward_data = await forward.data()
    reverse_data = await reverse.data()

    assert forward_data == [5, 15, 25]
    assert reverse_data == [-5, -15, -25]


async def test_scalar_div_is_noncommutative(ctx):
    """Test that scalar / obj != obj / scalar (order matters)."""
    obj = await create_object_from_value([2.0, 4.0, 5.0], aai_id=True)

    forward = obj / 10.0
    reverse = 10.0 / obj

    forward_data = await forward.data()
    reverse_data = await reverse.data()

    assert forward_data == pytest.approx([0.2, 0.4, 0.5], abs=THRESHOLD)
    assert reverse_data == pytest.approx([5.0, 2.5, 2.0], abs=THRESHOLD)


# =============================================================================
# Cross-table operator contract: both sides must be View(order_by=...)
# =============================================================================


async def test_cross_table_add_without_views_raises(ctx):
    """Binary elementwise op on array Objects from different sources raises."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20, 30])
    with pytest.raises(TypeError, match="explicit row order"):
        a + b


async def test_cross_table_add_with_one_view_raises(ctx):
    """Left-only (or right-only) View(order_by=...) is not enough."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20, 30])
    a_view = a.view(order_by="value")
    with pytest.raises(TypeError, match="explicit row order"):
        a_view + b


async def test_cross_table_add_with_two_views_succeeds(ctx):
    """Both sides as View(order_by=...) satisfies the contract."""
    a = await create_object_from_value([1, 2, 3], aai_id=True)
    b = await create_object_from_value([10, 20, 30], aai_id=True)
    result = a.view(order_by="value") + b.view(order_by="value")
    assert sorted(await result.data(order_by="value")) == [11, 22, 33]


async def test_same_table_add_no_views_still_works(ctx):
    """Same-table fast path skips the contract check."""
    a = await create_object_from_value([1, 2, 3], aai_id=True)
    result = a + a
    assert sorted(await result.data(order_by="value")) == [2, 4, 6]


async def test_scalar_broadcast_no_views_still_works(ctx):
    """Scalar broadcast skips the contract check."""
    a = await create_object_from_value([1, 2, 3], aai_id=True)
    s = await create_object_from_value(10, aai_id=True)
    result = a + s
    assert sorted(await result.data(order_by="value")) == [11, 12, 13]
