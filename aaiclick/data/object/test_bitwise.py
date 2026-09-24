"""
Parametrized tests for bitwise operators (&, |, ^).

Tests element-wise Object-Object bitwise operations for scalar and array Objects.
Array-field bitwise via array_map is covered in test_array_map.py.
Scalar broadcast is covered in test_arithmetic_broadcast.py.
"""

import operator

import pytest

from aaiclick import create_object_from_value

# =============================================================================
# Object <op> Object — scalar and array operands
# =============================================================================


@pytest.mark.parametrize(
    "val_a,val_b,op,expected",
    [
        pytest.param(0b1100, 0b1010, operator.and_, 0b1000, id="scalar-and"),
        pytest.param(0b1100, 0b1010, operator.or_, 0b1110, id="scalar-or"),
        pytest.param(0b1100, 0b1010, operator.xor, 0b0110, id="scalar-xor"),
        pytest.param(0b1111, 0b0000, operator.and_, 0b0000, id="scalar-and-zero"),
        pytest.param(0b0000, 0b1111, operator.or_, 0b1111, id="scalar-or-ones"),
        pytest.param(0b1111, 0b1111, operator.xor, 0b0000, id="scalar-xor-same"),
        pytest.param(
            [0b1100, 0b1010, 0b1111], [0b1010, 0b0110, 0b0000], operator.and_, [0b1000, 0b0010, 0b0000], id="array-and"
        ),
        pytest.param(
            [0b1100, 0b1010, 0b0000], [0b1010, 0b0110, 0b1111], operator.or_, [0b1110, 0b1110, 0b1111], id="array-or"
        ),
        pytest.param(
            [0b1100, 0b1010, 0b1111], [0b1010, 0b0110, 0b1111], operator.xor, [0b0110, 0b1100, 0b0000], id="array-xor"
        ),
    ],
)
async def test_bitwise(ctx, val_a, val_b, op, expected):
    """Bitwise operators on scalar and array Objects."""
    obj_a = await create_object_from_value(val_a, aai_id=True)
    obj_b = await create_object_from_value(val_b, aai_id=True)
    result = op(obj_a, obj_b)
    assert await result.data() == expected


# =============================================================================
# Reverse operators (scalar & obj, scalar | obj, scalar ^ obj)
# =============================================================================


@pytest.mark.parametrize(
    "scalar,val,op,expected",
    [
        pytest.param(0b1100, 0b1010, operator.and_, 0b1000, id="rand"),
        pytest.param(0b1100, 0b1010, operator.or_, 0b1110, id="ror"),
        pytest.param(0b1100, 0b1010, operator.xor, 0b0110, id="rxor"),
    ],
)
async def test_reverse_bitwise(ctx, scalar, val, op, expected):
    """Test reverse bitwise operators (scalar <op> obj)."""
    obj = await create_object_from_value(val, aai_id=True)
    result = op(scalar, obj)
    assert await result.data() == expected


# =============================================================================
# Chaining: bitwise then aggregate
# =============================================================================


async def test_bitwise_and_then_sum(ctx):
    """AND mask result can be summed (counts set bits per element)."""
    obj_a = await create_object_from_value([0b1111, 0b1010, 0b0000, 0b1100], aai_id=True)
    obj_b = await create_object_from_value([0b1010, 0b1010, 0b1111, 0b0101], aai_id=True)
    masked = obj_a & obj_b
    total = await masked.sum().data()
    assert total == 0b1010 + 0b1010 + 0b0000 + 0b0100
