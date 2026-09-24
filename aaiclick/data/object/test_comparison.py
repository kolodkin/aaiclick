"""
Parametrized tests for comparison operators (==, !=, <, <=, >, >=).

Tests element-wise Object-Object comparison for scalar and array Objects.
Scalar-broadcast comparisons are covered in test_arithmetic_broadcast.py.
"""

import operator
from datetime import datetime, timezone

import pytest

from aaiclick import create_object_from_value

# =============================================================================
# Object <op> Object — scalar and array operands
# =============================================================================


@pytest.mark.parametrize(
    "val_a,val_b,op,expected",
    [
        pytest.param(5, 5, operator.eq, 1, id="scalar-eq-equal"),
        pytest.param(5, 6, operator.eq, 0, id="scalar-eq-not-equal"),
        pytest.param(5, 5, operator.ne, 0, id="scalar-ne-equal"),
        pytest.param(5, 6, operator.ne, 1, id="scalar-ne-not-equal"),
        pytest.param(3, 5, operator.lt, 1, id="scalar-lt-true"),
        pytest.param(5, 3, operator.lt, 0, id="scalar-lt-false"),
        pytest.param(5, 5, operator.le, 1, id="scalar-le-equal"),
        pytest.param(4, 5, operator.le, 1, id="scalar-le-less"),
        pytest.param(6, 5, operator.le, 0, id="scalar-le-greater"),
        pytest.param(5, 3, operator.gt, 1, id="scalar-gt-true"),
        pytest.param(3, 5, operator.gt, 0, id="scalar-gt-false"),
        pytest.param(5, 5, operator.ge, 1, id="scalar-ge-equal"),
        pytest.param(6, 5, operator.ge, 1, id="scalar-ge-greater"),
        pytest.param(4, 5, operator.ge, 0, id="scalar-ge-less"),
        pytest.param([1, 2, 3], [1, 3, 2], operator.eq, [1, 0, 0], id="array-eq"),
        pytest.param([1, 2, 3], [1, 3, 2], operator.ne, [0, 1, 1], id="array-ne"),
        pytest.param([1, 5, 3], [2, 4, 3], operator.lt, [1, 0, 0], id="array-lt"),
        pytest.param([1, 5, 3], [2, 4, 3], operator.le, [1, 0, 1], id="array-le"),
        pytest.param([1, 5, 3], [2, 4, 3], operator.gt, [0, 1, 0], id="array-gt"),
        pytest.param([1, 5, 3], [2, 4, 3], operator.ge, [0, 1, 1], id="array-ge"),
        pytest.param([1.5, 2.5], [2.5, 1.5], operator.lt, [1, 0], id="array-float-lt"),
        pytest.param([1.0, 1.0], [1.0, 2.0], operator.le, [1, 1], id="array-float-le"),
        pytest.param([3.14, 2.71], [2.71, 3.14], operator.gt, [1, 0], id="array-float-gt"),
    ],
)
async def test_comparison(ctx, val_a, val_b, op, expected):
    """Comparison operators on scalar and array Objects."""
    obj_a = await create_object_from_value(val_a, aai_id=True)
    obj_b = await create_object_from_value(val_b, aai_id=True)
    result = op(obj_a, obj_b)
    assert await result.data() == expected


# =============================================================================
# Chaining: comparison result can be aggregated
# =============================================================================


async def test_comparison_then_sum(ctx):
    """Comparison result (UInt8) can be summed to count matches."""
    obj_a = await create_object_from_value([1, 2, 3, 4, 5], aai_id=True)
    obj_b = await create_object_from_value([1, 0, 3, 0, 5], aai_id=True)
    matches = obj_a == obj_b
    count = await matches.sum().data()
    assert count == 3


async def test_comparison_then_unique(ctx):
    """Comparison result values are 0 and 1 only."""
    obj_a = await create_object_from_value([1, 2, 3, 4], aai_id=True)
    obj_b = await create_object_from_value([1, 1, 1, 1], aai_id=True)
    result = obj_a == obj_b
    unique_vals = sorted(await result.unique().data())
    assert unique_vals == [0, 1]


# =============================================================================
# Result type — comparisons yield UInt8 (0/1) whatever the operand type
# =============================================================================


@pytest.mark.parametrize(
    "rows, rhs",
    [
        pytest.param([1, 5, 9], 5, id="int"),
        pytest.param([1.5, 5.0, 9.5], 5.0, id="float"),
        pytest.param(["a", "b", "c"], "b", id="str"),
        pytest.param(
            [datetime(2024, 1, 1, tzinfo=timezone.utc)] * 2 + [datetime(2025, 1, 1, tzinfo=timezone.utc)],
            datetime(2024, 6, 1, tzinfo=timezone.utc),
            id="datetime",
        ),
    ],
)
async def test_comparison_result_is_uint8(ctx, rows, rhs):
    """The result column is UInt8, as in ClickHouse — not the operand type
    (which would store the 0/1 flags as '0'/'1' strings or epoch datetimes)."""
    obj = await create_object_from_value(rows)
    lazy = obj > rhs
    result = await lazy
    assert lazy.schema.columns["value"].type == "UInt8"
    assert result.schema.columns["value"].type == "UInt8"
    assert await result.data() == [0, 0, 1]


# =============================================================================
# Comparison operators with scalar broadcast
# =============================================================================


@pytest.mark.parametrize(
    "arr,scalar,op,expected",
    [
        pytest.param([1, 5, 10], 5, operator.eq, [0, 1, 0], id="eq"),
        pytest.param([1, 5, 10], 5, operator.ne, [1, 0, 1], id="ne"),
        pytest.param([1, 5, 10], 5, operator.lt, [1, 0, 0], id="lt"),
        pytest.param([1, 5, 10], 5, operator.le, [1, 1, 0], id="le"),
        pytest.param([1, 5, 10], 5, operator.gt, [0, 0, 1], id="gt"),
        pytest.param([1, 5, 10], 5, operator.ge, [0, 1, 1], id="ge"),
    ],
)
async def test_comparison_with_scalar(ctx, arr, scalar, op, expected):
    """Comparison operators with scalar broadcast."""
    obj = await create_object_from_value(arr, aai_id=True)

    result = op(obj, scalar)

    data = await result.data()
    assert data == expected
