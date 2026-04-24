"""
Parametrized tests for bitwise operators (&, |, ^).

Tests element-wise Object-Object bitwise operations for scalar and array Objects.
Array-field bitwise via array_map is covered in test_array_map.py.
Scalar broadcast is covered in test_scalar_broadcast.py.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.models import FIELDTYPE_ARRAY


def _with_order(obj):
    if obj._schema.fieldtype == FIELDTYPE_ARRAY:
        return obj.view(order_by="value")
    return obj


async def apply_bitwise(obj_a, obj_b, operator: str):
    """Apply a bitwise operator to two Objects."""
    a, b = _with_order(obj_a), _with_order(obj_b)
    match operator:
        case "&":
            return await (a & b)
        case "|":
            return await (a | b)
        case "^":
            return await (a ^ b)
        case _:
            raise ValueError(f"Unsupported operator: {operator}")


# =============================================================================
# Scalar Object bitwise
# =============================================================================


@pytest.mark.parametrize(
    "val_a,val_b,operator,expected",
    [
        pytest.param(0b1100, 0b1010, "&", 0b1000, id="and-scalar"),
        pytest.param(0b1100, 0b1010, "|", 0b1110, id="or-scalar"),
        pytest.param(0b1100, 0b1010, "^", 0b0110, id="xor-scalar"),
        pytest.param(0b1111, 0b0000, "&", 0b0000, id="and-zero"),
        pytest.param(0b0000, 0b1111, "|", 0b1111, id="or-ones"),
        pytest.param(0b1111, 0b1111, "^", 0b0000, id="xor-same"),
    ],
)
async def test_scalar_bitwise(ctx, val_a, val_b, operator, expected):
    """Test bitwise operators on scalar Objects."""
    obj_a = await create_object_from_value(val_a)
    obj_b = await create_object_from_value(val_b)
    result = await apply_bitwise(obj_a, obj_b, operator)
    assert await result.data() == expected


# =============================================================================
# Array Object bitwise
# =============================================================================


@pytest.mark.parametrize(
    "vals_a,vals_b,operator,expected",
    [
        pytest.param([0b1100, 0b1010, 0b1111], [0b1010, 0b0110, 0b0000], "&", [0b1000, 0b0010, 0b0000], id="and-array"),
        pytest.param([0b1100, 0b1010, 0b0000], [0b1010, 0b0110, 0b1111], "|", [0b1110, 0b1110, 0b1111], id="or-array"),
        pytest.param([0b1100, 0b1010, 0b1111], [0b1010, 0b0110, 0b1111], "^", [0b0110, 0b1100, 0b0000], id="xor-array"),
    ],
)
async def test_array_bitwise(ctx, vals_a, vals_b, operator, expected):
    """Test bitwise operators on array Objects."""
    obj_a = await create_object_from_value(vals_a)
    obj_b = await create_object_from_value(vals_b)
    result = await apply_bitwise(obj_a, obj_b, operator)
    assert await result.data() == expected


# =============================================================================
# Reverse operators (scalar & obj, scalar | obj, scalar ^ obj)
# =============================================================================


@pytest.mark.parametrize(
    "scalar,val,operator,expected",
    [
        pytest.param(0b1100, 0b1010, "&", 0b1000, id="rand"),
        pytest.param(0b1100, 0b1010, "|", 0b1110, id="ror"),
        pytest.param(0b1100, 0b1010, "^", 0b0110, id="rxor"),
    ],
)
async def test_reverse_bitwise(ctx, scalar, val, operator, expected):
    """Test reverse bitwise operators (scalar <op> obj)."""
    obj = await create_object_from_value(val)
    match operator:
        case "&":
            result = await (scalar & obj)
        case "|":
            result = await (scalar | obj)
        case "^":
            result = await (scalar ^ obj)
    assert await result.data() == expected


# =============================================================================
# Chaining: bitwise then aggregate
# =============================================================================


async def test_bitwise_and_then_sum(ctx):
    """AND mask result can be summed (counts set bits per element)."""
    obj_a = await create_object_from_value([0b1111, 0b1010, 0b0000, 0b1100])
    obj_b = await create_object_from_value([0b1010, 0b1010, 0b1111, 0b0101])
    masked = await (obj_a.view(order_by="value") & obj_b.view(order_by="value"))
    total = await (await masked.sum()).data()
    assert total == 0b1010 + 0b1010 + 0b0000 + 0b0100
