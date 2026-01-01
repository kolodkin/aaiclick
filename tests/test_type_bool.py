"""
Tests for boolean (UInt8) data type - scalars, arrays, operators, and statistics.

Note: Booleans are stored as UInt8 in ClickHouse (True=1, False=0),
so arithmetic operations work on the underlying integer values.
"""

import numpy as np
from aaiclick import create_object_from_value

THRESHOLD = 1e-5


# =============================================================================
# Scalar Tests
# =============================================================================

async def test_bool_scalar_creation_true():
    """Test creating a True boolean scalar object."""
    obj = await create_object_from_value(True)
    data = await obj.data()
    assert data == 1  # Stored as UInt8
    await obj.delete_table()


async def test_bool_scalar_creation_false():
    """Test creating a False boolean scalar object."""
    obj = await create_object_from_value(False)
    data = await obj.data()
    assert data == 0  # Stored as UInt8
    await obj.delete_table()


async def test_bool_scalar_add():
    """Test addition of boolean scalars (as integers)."""
    a = await create_object_from_value(True)  # 1
    b = await create_object_from_value(True)  # 1

    result = await (a + b)
    data = await result.data()

    assert data == 2  # 1 + 1

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


async def test_bool_scalar_sub():
    """Test subtraction of boolean scalars (as integers)."""
    a = await create_object_from_value(True)   # 1
    b = await create_object_from_value(False)  # 0

    result = await (a - b)
    data = await result.data()

    assert data == 1  # 1 - 0

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


# =============================================================================
# Array Tests
# =============================================================================

async def test_bool_array_creation():
    """Test creating a boolean array object."""
    obj = await create_object_from_value([True, False, True, False])
    data = await obj.data()
    assert data == [1, 0, 1, 0]  # Stored as UInt8
    await obj.delete_table()


async def test_bool_array_add():
    """Test element-wise addition of boolean arrays."""
    a = await create_object_from_value([True, True, False])    # [1, 1, 0]
    b = await create_object_from_value([True, False, False])   # [1, 0, 0]

    result = await (a + b)
    data = await result.data()

    assert data == [2, 1, 0]

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


async def test_bool_array_sub():
    """Test element-wise subtraction of boolean arrays."""
    a = await create_object_from_value([True, True, True])     # [1, 1, 1]
    b = await create_object_from_value([False, True, False])   # [0, 1, 0]

    result = await (a - b)
    data = await result.data()

    assert data == [1, 0, 1]

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


# =============================================================================
# Statistics Tests
# =============================================================================

async def test_bool_array_min():
    """Test min() on boolean array."""
    obj = await create_object_from_value([True, False, True])  # [1, 0, 1]

    result = await obj.min()

    assert result == 0

    await obj.delete_table()


async def test_bool_array_max():
    """Test max() on boolean array."""
    obj = await create_object_from_value([True, False, True])  # [1, 0, 1]

    result = await obj.max()

    assert result == 1

    await obj.delete_table()


async def test_bool_array_sum():
    """Test sum() on boolean array (counts True values)."""
    obj = await create_object_from_value([True, False, True, True, False])  # [1, 0, 1, 1, 0]

    result = await obj.sum()

    assert result == 3  # Three True values

    await obj.delete_table()


async def test_bool_array_mean():
    """Test mean() on boolean array (proportion of True values)."""
    obj = await create_object_from_value([True, False, True, False])  # [1, 0, 1, 0]

    result = await obj.mean()

    assert abs(result - 0.5) < THRESHOLD  # 2/4 = 0.5

    await obj.delete_table()


async def test_bool_array_std():
    """Test std() on boolean array."""
    values = [True, False, True, False]  # [1, 0, 1, 0]
    obj = await create_object_from_value(values)

    result = await obj.std()
    expected = np.std([1, 0, 1, 0], ddof=0)

    assert abs(result - expected) < THRESHOLD

    await obj.delete_table()


async def test_bool_all_true():
    """Test statistics on all-True array."""
    obj = await create_object_from_value([True, True, True])  # [1, 1, 1]

    assert await obj.min() == 1
    assert await obj.max() == 1
    assert await obj.sum() == 3
    assert abs(await obj.mean() - 1.0) < THRESHOLD
    assert abs(await obj.std() - 0.0) < THRESHOLD

    await obj.delete_table()


async def test_bool_all_false():
    """Test statistics on all-False array."""
    obj = await create_object_from_value([False, False, False])  # [0, 0, 0]

    assert await obj.min() == 0
    assert await obj.max() == 0
    assert await obj.sum() == 0
    assert abs(await obj.mean() - 0.0) < THRESHOLD
    assert abs(await obj.std() - 0.0) < THRESHOLD

    await obj.delete_table()
