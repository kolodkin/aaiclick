"""
Parametrized tests for insert operations across different data types.

Tests in-place insertion into arrays with objects, scalar values, and list values.
"""

import pytest

THRESHOLD = 1e-5


# =============================================================================
# Basic Array Insert Tests
# =============================================================================


@pytest.mark.parametrize(
    "array_a,array_b,expected_result",
    [
        # Integer arrays
        ([1, 2, 3], [4, 5, 6], [1, 2, 3, 4, 5, 6]),
        ([10], [20, 30], [10, 20, 30]),
        ([1, 2], [3], [1, 2, 3]),
        ([0, 0], [0, 0], [0, 0, 0, 0]),
        # Float arrays
        ([1.5, 2.5], [3.5, 4.5], [1.5, 2.5, 3.5, 4.5]),
        ([10.0], [20.0, 30.0], [10.0, 20.0, 30.0]),
        ([0.0, 0.0], [0.0], [0.0, 0.0, 0.0]),
        # String arrays
        (["hello", "world"], ["foo", "bar"], ["hello", "world", "foo", "bar"]),
        (["apple"], ["banana", "cherry"], ["apple", "banana", "cherry"]),
        (["a", "b"], ["c"], ["a", "b", "c"]),
    ],
)
async def test_array_insert(ctx, array_a, array_b, expected_result):
    """Test inserting arrays of the same type in place."""
    obj_a = await ctx.create_object_from_value(array_a)
    obj_b = await ctx.create_object_from_value(array_b)

    await obj_a.insert(obj_b)
    data = await obj_a.data()

    # Use threshold for float comparisons
    if len(expected_result) > 0 and isinstance(expected_result[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert data == expected_result


# =============================================================================
# Insert with Scalar Value Tests
# =============================================================================


@pytest.mark.parametrize(
    "array,scalar_value,expected_result",
    [
        # Integer arrays + scalar
        ([1, 2, 3], 42, [1, 2, 3, 42]),
        ([10], 20, [10, 20]),
        ([0, 0], 1, [0, 0, 1]),
        # Float arrays + scalar
        ([1.5, 2.5], 3.5, [1.5, 2.5, 3.5]),
        ([10.0], 20.0, [10.0, 20.0]),
        ([0.0], 1.0, [0.0, 1.0]),
        # String arrays + scalar
        (["hello", "world"], "test", ["hello", "world", "test"]),
        (["a"], "b", ["a", "b"]),
    ],
)
async def test_array_insert_with_scalar_value(ctx, array, scalar_value, expected_result):
    """Test inserting scalar value into array in place."""
    obj = await ctx.create_object_from_value(array)

    await obj.insert(scalar_value)
    data = await obj.data()

    # Use threshold for float comparisons
    if len(expected_result) > 0 and isinstance(expected_result[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert data == expected_result


# =============================================================================
# Insert with List Value Tests
# =============================================================================


@pytest.mark.parametrize(
    "array,list_value,expected_result",
    [
        # Integer arrays + list
        ([1, 2, 3], [4, 5, 6], [1, 2, 3, 4, 5, 6]),
        ([10], [20, 30], [10, 20, 30]),
        ([0], [1, 2, 3], [0, 1, 2, 3]),
        # Float arrays + list
        ([1.5, 2.5], [3.5, 4.5], [1.5, 2.5, 3.5, 4.5]),
        ([10.0], [20.0, 30.0], [10.0, 20.0, 30.0]),
        # String arrays + list
        (["hello"], ["world", "test"], ["hello", "world", "test"]),
        (["a", "b"], ["c", "d", "e"], ["a", "b", "c", "d", "e"]),
    ],
)
async def test_array_insert_with_list_value(ctx, array, list_value, expected_result):
    """Test inserting list value into array in place."""
    obj = await ctx.create_object_from_value(array)

    await obj.insert(list_value)
    data = await obj.data()

    # Use threshold for float comparisons
    if len(expected_result) > 0 and isinstance(expected_result[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert data == expected_result


# =============================================================================
# Insert with Empty List Tests
# =============================================================================


@pytest.mark.parametrize(
    "array,expected_result",
    [
        # Integer arrays
        ([1, 2, 3], [1, 2, 3]),
        ([42], [42]),
        # Float arrays
        ([1.5, 2.5, 3.5], [1.5, 2.5, 3.5]),
        ([3.14], [3.14]),
        # String arrays
        (["hello", "world"], ["hello", "world"]),
        (["test"], ["test"]),
    ],
)
async def test_array_insert_with_empty_list(ctx, array, expected_result):
    """Test inserting empty list into array (should remain unchanged)."""
    obj = await ctx.create_object_from_value(array)

    await obj.insert([])
    data = await obj.data()

    # Use threshold for float comparisons
    if len(expected_result) > 0 and isinstance(expected_result[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert data == expected_result


# =============================================================================
# Insert Modifies In Place Tests
# =============================================================================


@pytest.mark.parametrize(
    "initial_array,insert_value,expected_result",
    [
        # Integer arrays
        ([1, 2, 3], [4, 5], [1, 2, 3, 4, 5]),
        ([10], 20, [10, 20]),
        # Float arrays
        ([1.5, 2.5], [3.5], [1.5, 2.5, 3.5]),
        ([10.0], 20.0, [10.0, 20.0]),
        # String arrays
        (["hello"], ["world"], ["hello", "world"]),
        (["a"], "b", ["a", "b"]),
    ],
)
async def test_insert_modifies_in_place(ctx, initial_array, insert_value, expected_result):
    """Test that insert modifies the original object in place."""
    obj = await ctx.create_object_from_value(initial_array)
    original_table = obj.table

    await obj.insert(insert_value)
    data = await obj.data()

    # Verify data was inserted
    if len(expected_result) > 0 and isinstance(expected_result[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert data == expected_result

    # Verify table name unchanged (in-place modification)
    assert obj.table == original_table


# =============================================================================
# Multiple Inserts Tests
# =============================================================================


@pytest.mark.parametrize(
    "initial_array,inserts,expected_result",
    [
        # Integer arrays - multiple inserts
        ([1, 2], [[3, 4], 5, [6]], [1, 2, 3, 4, 5, 6]),
        ([10], [[20, 30], 40], [10, 20, 30, 40]),
        # Float arrays - multiple inserts
        ([1.5], [[2.5, 3.5], 4.5], [1.5, 2.5, 3.5, 4.5]),
        ([10.0], [20.0, [30.0]], [10.0, 20.0, 30.0]),
        # String arrays - multiple inserts
        (["a"], [["b", "c"], "d"], ["a", "b", "c", "d"]),
        (["hello"], ["world", ["test"]], ["hello", "world", "test"]),
    ],
)
async def test_multiple_inserts(ctx, initial_array, inserts, expected_result):
    """Test multiple consecutive inserts."""
    obj = await ctx.create_object_from_value(initial_array)

    for insert_value in inserts:
        await obj.insert(insert_value)

    data = await obj.data()

    # Use threshold for float comparisons
    if len(expected_result) > 0 and isinstance(expected_result[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert data == expected_result


# =============================================================================
# Scalar Insert Failure Tests
# =============================================================================


@pytest.mark.parametrize(
    "scalar_value,array_value",
    [
        # Integer scalar + array
        (42, [1, 2, 3]),
        (0, [10, 20]),
        # Float scalar + array
        (3.14, [1.0, 2.0]),
        (0.0, [5.5, 6.6]),
        # Boolean scalar + array
        (True, [1, 2, 3]),
        (False, [0, 0]),
        # String scalar + array
        ("hello", ["a", "b"]),
        ("", ["test"]),
    ],
)
async def test_scalar_insert_fails(ctx, scalar_value, array_value):
    """Test that insert method on scalar fails."""
    scalar_obj = await ctx.create_object_from_value(scalar_value)
    array_obj = await ctx.create_object_from_value(array_value)

    with pytest.raises(ValueError, match="insert requires target table to have array fieldtype"):
        await scalar_obj.insert(array_obj)


# =============================================================================
# Data Integrity Tests
# =============================================================================


@pytest.mark.parametrize(
    "array_a,array_b",
    [
        # Note: Result order is based on Snowflake ID timestamps (creation order),
        # not argument order. These tests verify data integrity.
        ([1, 2, 3], [4, 5, 6]),
        ([5.5, 6.6], [7.7, 8.8]),
        (["a", "b"], ["c", "d"]),
    ],
)
async def test_insert_preserves_data_integrity(ctx, array_a, array_b):
    """Test that insert preserves all data from both arrays."""
    obj_a = await ctx.create_object_from_value(array_a)
    obj_b = await ctx.create_object_from_value(array_b)

    await obj_a.insert(obj_b)
    data = await obj_a.data()

    # Verify all elements are present (order depends on Snowflake IDs)
    assert len(data) == len(array_a) + len(array_b)

    # For numeric types, verify sum matches
    if isinstance(array_a[0], (int, float)):
        expected_sum = sum(array_a) + sum(array_b)
        actual_sum = sum(data)
        if isinstance(expected_sum, float):
            assert abs(actual_sum - expected_sum) < THRESHOLD
        else:
            assert actual_sum == expected_sum
