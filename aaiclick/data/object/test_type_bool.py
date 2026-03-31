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
        pytest.param(True, 1, id="true"),
        pytest.param(False, 0, id="false"),
    ],
)
async def test_bool_scalar_creation(ctx, value, expected):
    """Test creating boolean scalar objects (stored as UInt8)."""
    obj = await create_object_from_value(value)
    data = await obj.data()
    assert data == expected


async def test_bool_scalar_add(ctx):
    """Test addition of boolean scalars (as integers)."""
    a = await create_object_from_value(True)  # 1
    b = await create_object_from_value(True)  # 1

    result = await (a + b)
    data = await result.data()

    assert data == 2  # 1 + 1



async def test_bool_scalar_sub(ctx):
    """Test subtraction of boolean scalars (as integers)."""
    a = await create_object_from_value(True)   # 1
    b = await create_object_from_value(False)  # 0

    result = await (a - b)
    data = await result.data()

    assert data == 1  # 1 - 0



# =============================================================================
# Array Tests
# =============================================================================

async def test_bool_array_creation(ctx):
    """Test creating a boolean array object."""
    obj = await create_object_from_value([True, False, True, False])
    data = await obj.data()
    assert data == [1, 0, 1, 0]  # Stored as UInt8


async def test_bool_array_add(ctx):
    """Test element-wise addition of boolean arrays."""
    a = await create_object_from_value([True, True, False])    # [1, 1, 0]
    b = await create_object_from_value([True, False, False])   # [1, 0, 0]

    result = await (a + b)
    data = await result.data()

    assert data == [2, 1, 0]



async def test_bool_array_sub(ctx):
    """Test element-wise subtraction of boolean arrays."""
    a = await create_object_from_value([True, True, True])     # [1, 1, 1]
    b = await create_object_from_value([False, True, False])   # [0, 1, 0]

    result = await (a - b)
    data = await result.data()

    assert data == [1, 0, 1]

