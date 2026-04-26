"""
Parametrized tests for comparison operators (==, !=, <, <=, >, >=).

Tests element-wise Object-Object comparison for scalar and array Objects.
Scalar-broadcast comparisons are covered in test_scalar_broadcast.py.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.testing import with_value_order


async def apply_comparison(obj_a, obj_b, operator: str):
    """Apply a comparison operator to two Objects."""
    a, b = with_value_order(obj_a), with_value_order(obj_b)
    match operator:
        case "==":
            return await (a == b)
        case "!=":
            return await (a != b)
        case "<":
            return await (a < b)
        case "<=":
            return await (a <= b)
        case ">":
            return await (a > b)
        case ">=":
            return await (a >= b)
        case _:
            raise ValueError(f"Unsupported operator: {operator}")


# =============================================================================
# Scalar Object comparisons
# =============================================================================


@pytest.mark.parametrize(
    "val_a,val_b,operator,expected",
    [
        pytest.param(5, 5, "==", 1, id="eq-equal"),
        pytest.param(5, 6, "==", 0, id="eq-not-equal"),
        pytest.param(5, 5, "!=", 0, id="ne-equal"),
        pytest.param(5, 6, "!=", 1, id="ne-not-equal"),
        pytest.param(3, 5, "<", 1, id="lt-true"),
        pytest.param(5, 3, "<", 0, id="lt-false"),
        pytest.param(5, 5, "<=", 1, id="le-equal"),
        pytest.param(4, 5, "<=", 1, id="le-less"),
        pytest.param(6, 5, "<=", 0, id="le-greater"),
        pytest.param(5, 3, ">", 1, id="gt-true"),
        pytest.param(3, 5, ">", 0, id="gt-false"),
        pytest.param(5, 5, ">=", 1, id="ge-equal"),
        pytest.param(6, 5, ">=", 1, id="ge-greater"),
        pytest.param(4, 5, ">=", 0, id="ge-less"),
    ],
)
async def test_scalar_comparison(ctx, val_a, val_b, operator, expected):
    """Test comparison operators on scalar Objects."""
    obj_a = await create_object_from_value(val_a, aai_id=True)
    obj_b = await create_object_from_value(val_b, aai_id=True)
    result = await apply_comparison(obj_a, obj_b, operator)
    assert await result.data() == expected


# =============================================================================
# Array Object comparisons
# =============================================================================


@pytest.mark.parametrize(
    "vals_a,vals_b,operator,expected",
    [
        pytest.param([1, 2, 3], [1, 3, 2], "==", [1, 0, 0], id="eq"),
        pytest.param([1, 2, 3], [1, 3, 2], "!=", [0, 1, 1], id="ne"),
        pytest.param([1, 5, 3], [2, 4, 3], "<", [1, 0, 0], id="lt"),
        pytest.param([1, 5, 3], [2, 4, 3], "<=", [1, 0, 1], id="le"),
        pytest.param([1, 5, 3], [2, 4, 3], ">", [0, 1, 0], id="gt"),
        pytest.param([1, 5, 3], [2, 4, 3], ">=", [0, 1, 1], id="ge"),
    ],
)
async def test_array_comparison(ctx, vals_a, vals_b, operator, expected):
    """Test comparison operators on array Objects."""
    obj_a = await create_object_from_value(vals_a, aai_id=True)
    obj_b = await create_object_from_value(vals_b, aai_id=True)
    result = await apply_comparison(obj_a, obj_b, operator)
    assert await result.data() == expected


# =============================================================================
# Chaining: comparison result can be aggregated
# =============================================================================


async def test_comparison_then_sum(ctx):
    """Comparison result (UInt8) can be summed to count matches."""
    obj_a = await create_object_from_value([1, 2, 3, 4, 5], aai_id=True)
    obj_b = await create_object_from_value([1, 0, 3, 0, 5], aai_id=True)
    matches = await (obj_a.view(order_by="value") == obj_b.view(order_by="value"))
    count = await (await matches.sum()).data()
    assert count == 3


async def test_comparison_then_unique(ctx):
    """Comparison result values are 0 and 1 only."""
    obj_a = await create_object_from_value([1, 2, 3, 4], aai_id=True)
    obj_b = await create_object_from_value([1, 1, 1, 1], aai_id=True)
    result = await (obj_a.view(order_by="value") == obj_b.view(order_by="value"))
    unique_vals = sorted(await (await result.unique()).data())
    assert unique_vals == [0, 1]


# =============================================================================
# Float comparisons
# =============================================================================


@pytest.mark.parametrize(
    "vals_a,vals_b,operator,expected",
    [
        pytest.param([1.5, 2.5], [2.5, 1.5], "<", [1, 0], id="float-lt"),
        pytest.param([1.0, 1.0], [1.0, 2.0], "<=", [1, 1], id="float-le"),
        pytest.param([3.14, 2.71], [2.71, 3.14], ">", [1, 0], id="float-gt"),
    ],
)
async def test_float_comparison(ctx, vals_a, vals_b, operator, expected):
    """Test comparison operators on float arrays."""
    obj_a = await create_object_from_value(vals_a, aai_id=True)
    obj_b = await create_object_from_value(vals_b, aai_id=True)
    result = await apply_comparison(obj_a, obj_b, operator)
    assert await result.data() == expected
