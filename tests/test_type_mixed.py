"""
Tests for mixed data types (int + float combinations) - scalars, arrays, and operators.

This file tests operations between different numeric types to ensure proper
type coercion and result accuracy.
"""

import numpy as np

THRESHOLD = 1e-5


# =============================================================================
# Scalar Tests - Int + Float
# =============================================================================


async def test_mixed_scalar_int_plus_float(ctx):
    """Test addition of int scalar + float scalar."""
    a = await create_object_from_value(100)
    b = await create_object_from_value(50.5)

    result = await (a + b)
    data = await result.data()

    assert abs(data - 150.5) < THRESHOLD



async def test_mixed_scalar_float_plus_int(ctx):
    """Test addition of float scalar + int scalar."""
    a = await create_object_from_value(100.5)
    b = await create_object_from_value(50)

    result = await (a + b)
    data = await result.data()

    assert abs(data - 150.5) < THRESHOLD



async def test_mixed_scalar_int_minus_float(ctx):
    """Test subtraction of int scalar - float scalar."""
    a = await create_object_from_value(100)
    b = await create_object_from_value(30.5)

    result = await (a - b)
    data = await result.data()

    assert abs(data - 69.5) < THRESHOLD



async def test_mixed_scalar_float_minus_int(ctx):
    """Test subtraction of float scalar - int scalar."""
    a = await create_object_from_value(100.5)
    b = await create_object_from_value(30)

    result = await (a - b)
    data = await result.data()

    assert abs(data - 70.5) < THRESHOLD



async def test_mixed_scalar_zero_combinations(ctx):
    """Test mixed type operations with zero values."""
    # int 0 + float
    a = await create_object_from_value(0)
    b = await create_object_from_value(3.14159)
    result = await (a + b)
    data = await result.data()
    assert abs(data - 3.14159) < THRESHOLD

    # float 0.0 + int
    a = await create_object_from_value(0.0)
    b = await create_object_from_value(42)
    result = await (a + b)
    data = await result.data()
    assert abs(data - 42.0) < THRESHOLD


# =============================================================================
# Array Tests - Int + Float
# =============================================================================


async def test_mixed_array_int_plus_float(ctx):
    """Test element-wise addition of int array + float array."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([0.5, 1.5, 2.5])

    result = await (a + b)
    data = await result.data()

    expected = [1.5, 3.5, 5.5]
    for i, val in enumerate(data):
        assert abs(val - expected[i]) < THRESHOLD



async def test_mixed_array_float_plus_int(ctx):
    """Test element-wise addition of float array + int array."""
    a = await create_object_from_value([10.0, 20.0, 30.0])
    b = await create_object_from_value([1, 2, 3])

    result = await (a + b)
    data = await result.data()

    expected = [11.0, 22.0, 33.0]
    for i, val in enumerate(data):
        assert abs(val - expected[i]) < THRESHOLD



async def test_mixed_array_int_minus_float(ctx):
    """Test element-wise subtraction of int array - float array."""
    a = await create_object_from_value([100, 200, 300])
    b = await create_object_from_value([10.5, 20.5, 30.5])

    result = await (a - b)
    data = await result.data()

    expected = [89.5, 179.5, 269.5]
    for i, val in enumerate(data):
        assert abs(val - expected[i]) < THRESHOLD



async def test_mixed_array_float_minus_int(ctx):
    """Test element-wise subtraction of float array - int array."""
    a = await create_object_from_value([100.5, 200.5, 300.5])
    b = await create_object_from_value([10, 20, 30])

    result = await (a - b)
    data = await result.data()

    expected = [90.5, 180.5, 270.5]
    for i, val in enumerate(data):
        assert abs(val - expected[i]) < THRESHOLD



async def test_mixed_array_large_int_small_float(ctx):
    """Test mixed arrays with large integers and small floats."""
    a = await create_object_from_value([1000000, 2000000, 3000000])
    b = await create_object_from_value([0.001, 0.002, 0.003])

    result = await (a + b)
    data = await result.data()

    expected = [1000000.001, 2000000.002, 3000000.003]
    for i, val in enumerate(data):
        assert abs(val - expected[i]) < THRESHOLD



async def test_mixed_array_negative_combinations(ctx):
    """Test mixed arrays with negative numbers."""
    a = await create_object_from_value([-10, -20, -30])
    b = await create_object_from_value([5.5, 10.5, 15.5])

    result = await (a + b)
    data = await result.data()

    expected = [-4.5, -9.5, -14.5]
    for i, val in enumerate(data):
        assert abs(val - expected[i]) < THRESHOLD



# =============================================================================
# Chained Operations with Mixed Types
# =============================================================================


async def test_mixed_chained_operations(ctx):
    """Test chaining multiple operations with mixed types."""
    # int + float - int
    a = await create_object_from_value([10, 20, 30])
    b = await create_object_from_value([5.5, 10.5, 15.5])
    c = await create_object_from_value([3, 6, 9])

    temp = await (a + b)
    result = await (temp - c)
    data = await result.data()

    expected = [12.5, 24.5, 36.5]
    for i, val in enumerate(data):
        assert abs(val - expected[i]) < THRESHOLD



async def test_mixed_chained_float_int_float(ctx):
    """Test chaining with float - int + float pattern."""
    # float - int + float
    a = await create_object_from_value([100.5, 200.5])
    b = await create_object_from_value([10, 20])
    c = await create_object_from_value([5.25, 10.25])

    temp = await (a - b)
    result = await (temp + c)
    data = await result.data()

    expected = [95.75, 190.75]
    for i, val in enumerate(data):
        assert abs(val - expected[i]) < THRESHOLD



async def test_mixed_triple_addition(ctx):
    """Test triple addition with mixed types."""
    # int + float + int
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([0.5, 1.0, 1.5])
    c = await create_object_from_value([10, 20, 30])

    temp = await (a + b)
    result = await (temp + c)
    data = await result.data()

    expected = [11.5, 23.0, 34.5]
    for i, val in enumerate(data):
        assert abs(val - expected[i]) < THRESHOLD



# =============================================================================
# Statistics Tests on Mixed Type Results
# =============================================================================


async def test_mixed_statistics_after_operation(ctx):
    """Test statistics on result of mixed type operations."""
    a = await create_object_from_value([10, 20, 30, 40])
    b = await create_object_from_value([0.5, 1.5, 2.5, 3.5])

    result = await (a + b)

    expected_values = np.array([10.5, 21.5, 32.5, 43.5])

    assert abs(await result.min() - np.min(expected_values)) < THRESHOLD
    assert abs(await result.max() - np.max(expected_values)) < THRESHOLD
    assert abs(await result.sum() - np.sum(expected_values)) < THRESHOLD
    assert abs(await result.mean() - np.mean(expected_values)) < THRESHOLD
    assert abs(await result.std() - np.std(expected_values, ddof=0)) < THRESHOLD



async def test_mixed_min_max_after_subtraction(ctx):
    """Test min/max on result of mixed type subtraction."""
    a = await create_object_from_value([100, 200, 300])
    b = await create_object_from_value([0.1, 0.2, 0.3])

    result = await (a - b)

    assert abs(await result.min() - 99.9) < THRESHOLD
    assert abs(await result.max() - 299.7) < THRESHOLD



async def test_mixed_sum_mean_precision(ctx):
    """Test sum and mean with mixed types requiring precision."""
    # Create arrays where int + float requires precision
    a = await create_object_from_value([1, 2, 3, 4, 5])
    b = await create_object_from_value([0.1, 0.2, 0.3, 0.4, 0.5])

    result = await (a + b)

    expected_values = np.array([1.1, 2.2, 3.3, 4.4, 5.5])
    expected_sum = np.sum(expected_values)
    expected_mean = np.mean(expected_values)

    assert abs(await result.sum() - expected_sum) < THRESHOLD
    assert abs(await result.mean() - expected_mean) < THRESHOLD



# =============================================================================
# Edge Cases with Mixed Types
# =============================================================================


async def test_mixed_single_element_arrays(ctx):
    """Test mixed type operations on single element arrays."""
    a = await create_object_from_value([42])
    b = await create_object_from_value([0.5])

    result = await (a + b)
    data = await result.data()

    assert abs(data[0] - 42.5) < THRESHOLD



async def test_mixed_very_small_float_with_large_int(ctx):
    """Test operations with very small float and large int."""
    a = await create_object_from_value([1000000])
    b = await create_object_from_value([1e-10])

    result = await (a + b)
    data = await result.data()

    # Result should be very close to 1000000 due to float precision
    assert abs(data[0] - 1000000.0000000001) < 1e-9



async def test_mixed_boundary_values(ctx):
    """Test mixed operations with boundary values."""
    # Test with zero and negative transitions
    a = await create_object_from_value([-1, 0, 1])
    b = await create_object_from_value([0.5, 0.5, 0.5])

    result = await (a + b)
    data = await result.data()

    expected = [-0.5, 0.5, 1.5]
    for i, val in enumerate(data):
        assert abs(val - expected[i]) < THRESHOLD



async def test_mixed_symmetry(ctx):
    """Test that int+float == float+int (commutative property)."""
    # Test with arrays
    int_array = [10, 20, 30]
    float_array = [1.5, 2.5, 3.5]

    # int + float
    a1 = await create_object_from_value(int_array)
    b1 = await create_object_from_value(float_array)
    result1 = await (a1 + b1)
    data1 = await result1.data()

    # float + int
    a2 = await create_object_from_value(float_array)
    b2 = await create_object_from_value(int_array)
    result2 = await (a2 + b2)
    data2 = await result2.data()

    # Results should be identical
    for i in range(len(data1)):
        assert abs(data1[i] - data2[i]) < THRESHOLD



# =============================================================================
# Concat Tests with Mixed Types
# =============================================================================


async def test_mixed_int_float_concat_fails(ctx):
    """Test that concatenating int array with float array fails with type error."""
    import pytest
    from clickhouse_connect.driver.exceptions import DatabaseError

    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([4.5, 5.5, 6.5])

    with pytest.raises(DatabaseError, match="NO_COMMON_TYPE"):
        await a.concat(b)



async def test_mixed_float_int_concat_fails(ctx):
    """Test that concatenating float array with int array fails with type error."""
    import pytest
    from clickhouse_connect.driver.exceptions import DatabaseError

    a = await create_object_from_value([1.5, 2.5, 3.5])
    b = await create_object_from_value([4, 5, 6])

    with pytest.raises(DatabaseError, match="NO_COMMON_TYPE"):
        await a.concat(b)



async def test_mixed_int_string_concat_fails(ctx):
    """Test that concatenating int array with string array fails with type error."""
    import pytest
    from clickhouse_connect.driver.exceptions import DatabaseError

    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value(["a", "b", "c"])

    with pytest.raises(DatabaseError, match="NO_COMMON_TYPE"):
        await a.concat(b)


# =============================================================================
# Insert Tests with Mixed Types
# =============================================================================


async def test_mixed_int_float_insert_succeeds(ctx):
    """Test that inserting float array into int array succeeds (ClickHouse allows casting)."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([4.5, 5.5, 6.5])

    await a.insert(b)
    data = await a.data()

    # Float values get truncated when cast to int
    assert data == [1, 2, 3, 4, 5, 6]


async def test_mixed_float_int_insert_succeeds(ctx):
    """Test that inserting int array into float array succeeds (ClickHouse allows casting)."""
    a = await create_object_from_value([1.5, 2.5, 3.5])
    b = await create_object_from_value([4, 5, 6])

    await a.insert(b)
    data = await a.data()

    # Int values get converted to float
    assert data == [1.5, 2.5, 3.5, 4.0, 5.0, 6.0]


async def test_mixed_int_string_insert_fails(ctx):
    """Test that inserting string array into int array fails with type error."""
    import pytest

    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value(["a", "b", "c"])

    with pytest.raises(ValueError, match="types are incompatible"):
        await a.insert(b)


async def test_mixed_insert_float_value_into_int_succeeds(ctx):
    """Test that inserting float value into int array succeeds (truncates)."""
    a = await create_object_from_value([1, 2, 3])

    await a.insert(4.5)
    data = await a.data()

    # Float value gets truncated when cast to int
    assert data == [1, 2, 3, 4]


async def test_mixed_insert_float_list_into_int_succeeds(ctx):
    """Test that inserting float list into int array succeeds (truncates)."""
    a = await create_object_from_value([1, 2, 3])

    await a.insert([4.5, 5.5])
    data = await a.data()

    # Float values get truncated when cast to int
    assert data == [1, 2, 3, 4, 5]


async def test_mixed_insert_modifies_in_place(ctx):
    """Test that insert with same type modifies the original object."""
    a = await create_object_from_value([1, 2, 3])
    original_table = a.table

    await a.insert([4, 5])
    data = await a.data()

    # Verify data was inserted
    assert data == [1, 2, 3, 4, 5]
    # Verify table name unchanged (in-place modification)
    assert a.table == original_table

