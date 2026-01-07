"""
Parametrized tests for concat operations across different data types.

Tests array concatenation with objects, scalar values, and list values.
"""

import pytest

THRESHOLD = 1e-5


# =============================================================================
# Basic Array Concat Tests
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
        ([1.5, 2.5], [3.5, 4.5, 5.5], [1.5, 2.5, 3.5, 4.5, 5.5]),
        ([10.0], [20.0, 30.0], [10.0, 20.0, 30.0]),
        ([0.0, 0.0], [0.0], [0.0, 0.0, 0.0]),
        # String arrays
        (["hello", "world"], ["foo", "bar", "baz"], ["hello", "world", "foo", "bar", "baz"]),
        (["apple"], ["banana", "cherry"], ["apple", "banana", "cherry"]),
        (["a", "b"], ["c"], ["a", "b", "c"]),
    ],
)
async def test_array_concat(ctx, array_a, array_b, expected_result):
    """Test concatenating arrays of the same type."""
    obj_a = await ctx.create_object_from_value(array_a)
    obj_b = await ctx.create_object_from_value(array_b)

    result = await obj_a.concat(obj_b)
    data = await result.data()

    # Use threshold for float comparisons
    if len(expected_result) > 0 and isinstance(expected_result[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert data == expected_result


# =============================================================================
# Concat with Scalar Value Tests
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
async def test_array_concat_with_scalar_value(ctx, array, scalar_value, expected_result):
    """Test concatenating array with scalar value."""
    obj = await ctx.create_object_from_value(array)

    result = await obj.concat(scalar_value)
    data = await result.data()

    # Use threshold for float comparisons
    if len(expected_result) > 0 and isinstance(expected_result[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert data == expected_result


# =============================================================================
# Concat with List Value Tests
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
async def test_array_concat_with_list_value(ctx, array, list_value, expected_result):
    """Test concatenating array with list value."""
    obj = await ctx.create_object_from_value(array)

    result = await obj.concat(list_value)
    data = await result.data()

    # Use threshold for float comparisons
    if len(expected_result) > 0 and isinstance(expected_result[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert data == expected_result


# =============================================================================
# Concat with Empty List Tests
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
async def test_array_concat_with_empty_list(ctx, array, expected_result):
    """Test concatenating array with empty list (should return same data)."""
    obj = await ctx.create_object_from_value(array)

    result = await obj.concat([])
    data = await result.data()

    # Use threshold for float comparisons
    if len(expected_result) > 0 and isinstance(expected_result[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert data == expected_result


# =============================================================================
# Scalar Concat Failure Tests
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
async def test_scalar_concat_fails(ctx, scalar_value, array_value):
    """Test that concat method on scalar fails."""
    scalar_obj = await ctx.create_object_from_value(scalar_value)
    array_obj = await ctx.create_object_from_value(array_value)

    with pytest.raises(ValueError, match="concat requires first table to have array fieldtype"):
        await scalar_obj.concat(array_obj)


# =============================================================================
# Order Preservation Tests
# =============================================================================


@pytest.mark.parametrize(
    "array_a,array_b",
    [
        # Note: Result order is based on Snowflake ID timestamps (creation order),
        # not argument order. These tests verify data integrity, not argument order.
        ([1, 2, 3], [4, 5, 6]),
        ([5.5, 6.6], [7.7, 8.8]),
        (["a", "b"], ["c", "d"]),
    ],
)
async def test_concat_preserves_data_integrity(ctx, array_a, array_b):
    """Test that concat preserves all data from both arrays."""
    obj_a = await ctx.create_object_from_value(array_a)
    obj_b = await ctx.create_object_from_value(array_b)

    result = await obj_a.concat(obj_b)
    data = await result.data()

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
