"""
Tests for boolean (UInt8) data type - scalars, arrays, operators, and statistics.

Note: Booleans are stored as UInt8 in ClickHouse (True=1, False=0),
so arithmetic operations work on the underlying integer values.
"""

import pytest

from aaiclick import create_object_from_value

# =============================================================================
# Scalar Tests
# =============================================================================


@pytest.mark.parametrize(
    "value,expected",
    [
        pytest.param(True, 1, id="scalar-true"),
        pytest.param(False, 0, id="scalar-false"),
        pytest.param([True, False, True, False], [1, 0, 1, 0], id="array"),
    ],
)
async def test_bool_creation(ctx, value, expected):
    """Booleans are stored as UInt8, so they read back as 1/0."""
    obj = await create_object_from_value(value, aai_id=True)
    data = await obj.data()
    assert data == expected


@pytest.mark.parametrize(
    "left, right, expected",
    [
        pytest.param(True, True, 2, id="scalar"),  # 1 + 1
        pytest.param([True, True, False], [True, False, False], [2, 1, 0], id="array"),
    ],
)
async def test_bool_add(ctx, left, right, expected):
    """Test addition of booleans (as their underlying integers)."""
    a = await create_object_from_value(left, aai_id=True)
    b = await create_object_from_value(right, aai_id=True)

    result = a + b

    assert await result.data() == expected


@pytest.mark.parametrize(
    "left, right, expected",
    [
        pytest.param(True, False, 1, id="scalar"),  # 1 - 0
        pytest.param([True, True, True], [False, True, False], [1, 0, 1], id="array"),
    ],
)
async def test_bool_sub(ctx, left, right, expected):
    """Test subtraction of booleans (as their underlying integers)."""
    a = await create_object_from_value(left, aai_id=True)
    b = await create_object_from_value(right, aai_id=True)

    result = a - b

    assert await result.data() == expected
