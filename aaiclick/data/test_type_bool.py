"""
Tests for boolean (UInt8) data type - scalars, arrays, operators, and statistics.

Note: Booleans are stored as UInt8 in ClickHouse (True=1, False=0),
so arithmetic operations work on the underlying integer values.
"""

import numpy as np

from aaiclick import create_object_from_value, create_object

THRESHOLD = 1e-5


# =============================================================================
# Scalar Tests
# =============================================================================

async def test_bool_scalar_creation_true(ctx):
    """Test creating a True boolean scalar object."""
    obj = await create_object_from_value(True)
    data = await obj.data()
    assert data == 1  # Stored as UInt8


async def test_bool_scalar_creation_false(ctx):
    """Test creating a False boolean scalar object."""
    obj = await create_object_from_value(False)
    data = await obj.data()
    assert data == 0  # Stored as UInt8


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



# =============================================================================
# Statistics Tests
# =============================================================================

async def test_bool_array_min(ctx):
    """Test min() on boolean array. Returns Object, use .data() to extract value."""
    obj = await create_object_from_value([True, False, True])  # [1, 0, 1]

    result_obj = await obj.min()
    result = await result_obj.data()

    assert result == 0



async def test_bool_array_max(ctx):
    """Test max() on boolean array. Returns Object, use .data() to extract value."""
    obj = await create_object_from_value([True, False, True])  # [1, 0, 1]

    result_obj = await obj.max()
    result = await result_obj.data()

    assert result == 1



async def test_bool_array_sum(ctx):
    """Test sum() on boolean array (counts True values). Returns Object, use .data() to extract value."""
    obj = await create_object_from_value([True, False, True, True, False])  # [1, 0, 1, 1, 0]

    result_obj = await obj.sum()
    result = await result_obj.data()

    assert result == 3  # Three True values



async def test_bool_array_mean(ctx):
    """Test mean() on boolean array (proportion of True values). Returns Object, use .data() to extract value."""
    obj = await create_object_from_value([True, False, True, False])  # [1, 0, 1, 0]

    result_obj = await obj.mean()
    result = await result_obj.data()

    assert abs(result - 0.5) < THRESHOLD  # 2/4 = 0.5



async def test_bool_array_std(ctx):
    """Test std() on boolean array. Returns Object, use .data() to extract value."""
    values = [True, False, True, False]  # [1, 0, 1, 0]
    obj = await create_object_from_value(values)

    result_obj = await obj.std()
    result = await result_obj.data()
    expected = np.std([1, 0, 1, 0], ddof=0)

    assert abs(result - expected) < THRESHOLD



async def test_bool_all_true(ctx):
    """Test statistics on all-True array. Returns Objects, use .data() to extract values."""
    obj = await create_object_from_value([True, True, True])  # [1, 1, 1]

    assert await (await obj.min()).data() == 1
    assert await (await obj.max()).data() == 1
    assert await (await obj.sum()).data() == 3
    assert abs(await (await obj.mean()).data() - 1.0) < THRESHOLD
    assert abs(await (await obj.std()).data() - 0.0) < THRESHOLD



async def test_bool_all_false(ctx):
    """Test statistics on all-False array. Returns Objects, use .data() to extract values."""
    obj = await create_object_from_value([False, False, False])  # [0, 0, 0]

    assert await (await obj.min()).data() == 0
    assert await (await obj.max()).data() == 0
    assert await (await obj.sum()).data() == 0
    assert abs(await (await obj.mean()).data() - 0.0) < THRESHOLD
    assert abs(await (await obj.std()).data() - 0.0) < THRESHOLD

