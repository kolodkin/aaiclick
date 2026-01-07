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
    "data_type,array_a,array_b,expected_result",
    [
        # Integer arrays
        pytest.param("int", [1, 2, 3], [4, 5, 6], [1, 2, 3, 4, 5, 6], id="int-basic"),
        pytest.param("int", [10], [20, 30], [10, 20, 30], id="int-single-multi"),
        pytest.param("int", [1, 2], [3], [1, 2, 3], id="int-multi-single"),
        pytest.param("int", [0, 0], [0, 0], [0, 0, 0, 0], id="int-zeros"),
        # Float arrays
        pytest.param("float", [1.5, 2.5], [3.5, 4.5], [1.5, 2.5, 3.5, 4.5], id="float-basic"),
        pytest.param("float", [10.0], [20.0, 30.0], [10.0, 20.0, 30.0], id="float-single-multi"),
        pytest.param("float", [0.0, 0.0], [0.0], [0.0, 0.0, 0.0], id="float-zeros"),
        # String arrays
        pytest.param("str", ["hello", "world"], ["foo", "bar"], ["hello", "world", "foo", "bar"], id="str-basic"),
        pytest.param("str", ["apple"], ["banana", "cherry"], ["apple", "banana", "cherry"], id="str-single-multi"),
        pytest.param("str", ["a", "b"], ["c"], ["a", "b", "c"], id="str-multi-single"),
    ],
)
async def test_array_insert(ctx, data_type, array_a, array_b, expected_result):
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
    "data_type,array,scalar_value,expected_result",
    [
        # Integer arrays + scalar
        pytest.param("int", [1, 2, 3], 42, [1, 2, 3, 42], id="int-basic"),
        pytest.param("int", [10], 20, [10, 20], id="int-single-array"),
        pytest.param("int", [0, 0], 1, [0, 0, 1], id="int-zeros-plus-one"),
        # Float arrays + scalar
        pytest.param("float", [1.5, 2.5], 3.5, [1.5, 2.5, 3.5], id="float-basic"),
        pytest.param("float", [10.0], 20.0, [10.0, 20.0], id="float-single-array"),
        pytest.param("float", [0.0], 1.0, [0.0, 1.0], id="float-zero-plus-one"),
        # String arrays + scalar
        pytest.param("str", ["hello", "world"], "test", ["hello", "world", "test"], id="str-basic"),
        pytest.param("str", ["a"], "b", ["a", "b"], id="str-single-array"),
    ],
)
async def test_array_insert_with_scalar_value(ctx, data_type, array, scalar_value, expected_result):
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
    "data_type,array,list_value,expected_result",
    [
        # Integer arrays + list
        pytest.param("int", [1, 2, 3], [4, 5, 6], [1, 2, 3, 4, 5, 6], id="int-basic"),
        pytest.param("int", [10], [20, 30], [10, 20, 30], id="int-single-multi"),
        pytest.param("int", [0], [1, 2, 3], [0, 1, 2, 3], id="int-zero-plus-list"),
        # Float arrays + list
        pytest.param("float", [1.5, 2.5], [3.5, 4.5], [1.5, 2.5, 3.5, 4.5], id="float-basic"),
        pytest.param("float", [10.0], [20.0, 30.0], [10.0, 20.0, 30.0], id="float-single-multi"),
        # String arrays + list
        pytest.param("str", ["hello"], ["world", "test"], ["hello", "world", "test"], id="str-single-multi"),
        pytest.param("str", ["a", "b"], ["c", "d", "e"], ["a", "b", "c", "d", "e"], id="str-basic"),
    ],
)
async def test_array_insert_with_list_value(ctx, data_type, array, list_value, expected_result):
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
    "data_type,array,expected_result",
    [
        # Integer arrays
        pytest.param("int", [1, 2, 3], [1, 2, 3], id="int-array"),
        pytest.param("int", [42], [42], id="int-single"),
        # Float arrays
        pytest.param("float", [1.5, 2.5, 3.5], [1.5, 2.5, 3.5], id="float-array"),
        pytest.param("float", [3.14], [3.14], id="float-single"),
        # String arrays
        pytest.param("str", ["hello", "world"], ["hello", "world"], id="str-array"),
        pytest.param("str", ["test"], ["test"], id="str-single"),
    ],
)
async def test_array_insert_with_empty_list(ctx, data_type, array, expected_result):
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
    "data_type,initial_array,insert_value,expected_result",
    [
        # Integer arrays
        pytest.param("int", [1, 2, 3], [4, 5], [1, 2, 3, 4, 5], id="int-list"),
        pytest.param("int", [10], 20, [10, 20], id="int-scalar"),
        # Float arrays
        pytest.param("float", [1.5, 2.5], [3.5], [1.5, 2.5, 3.5], id="float-list"),
        pytest.param("float", [10.0], 20.0, [10.0, 20.0], id="float-scalar"),
        # String arrays
        pytest.param("str", ["hello"], ["world"], ["hello", "world"], id="str-list"),
        pytest.param("str", ["a"], "b", ["a", "b"], id="str-scalar"),
    ],
)
async def test_insert_modifies_in_place(ctx, data_type, initial_array, insert_value, expected_result):
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
    "data_type,initial_array,inserts,expected_result",
    [
        # Integer arrays - multiple inserts
        pytest.param("int", [1, 2], [[3, 4], 5, [6]], [1, 2, 3, 4, 5, 6], id="int-complex"),
        pytest.param("int", [10], [[20, 30], 40], [10, 20, 30, 40], id="int-simple"),
        # Float arrays - multiple inserts
        pytest.param("float", [1.5], [[2.5, 3.5], 4.5], [1.5, 2.5, 3.5, 4.5], id="float-complex"),
        pytest.param("float", [10.0], [20.0, [30.0]], [10.0, 20.0, 30.0], id="float-simple"),
        # String arrays - multiple inserts
        pytest.param("str", ["a"], [["b", "c"], "d"], ["a", "b", "c", "d"], id="str-complex"),
        pytest.param("str", ["hello"], ["world", ["test"]], ["hello", "world", "test"], id="str-simple"),
    ],
)
async def test_multiple_inserts(ctx, data_type, initial_array, inserts, expected_result):
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
    "data_type,scalar_value,array_value",
    [
        # Integer scalar + array
        pytest.param("int", 42, [1, 2, 3], id="int-positive"),
        pytest.param("int", 0, [10, 20], id="int-zero"),
        # Float scalar + array
        pytest.param("float", 3.14, [1.0, 2.0], id="float-pi"),
        pytest.param("float", 0.0, [5.5, 6.6], id="float-zero"),
        # Boolean scalar + array
        pytest.param("bool", True, [1, 2, 3], id="bool-true"),
        pytest.param("bool", False, [0, 0], id="bool-false"),
        # String scalar + array
        pytest.param("str", "hello", ["a", "b"], id="str-text"),
        pytest.param("str", "", ["test"], id="str-empty"),
    ],
)
async def test_scalar_insert_fails(ctx, data_type, scalar_value, array_value):
    """Test that insert method on scalar fails."""
    scalar_obj = await ctx.create_object_from_value(scalar_value)
    array_obj = await ctx.create_object_from_value(array_value)

    with pytest.raises(ValueError, match="insert requires target table to have array fieldtype"):
        await scalar_obj.insert(array_obj)


# =============================================================================
# Data Integrity Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,array_a,array_b",
    [
        # Note: Result order is based on Snowflake ID timestamps (creation order),
        # not argument order. These tests verify data integrity.
        pytest.param("int", [1, 2, 3], [4, 5, 6], id="int-arrays"),
        pytest.param("float", [5.5, 6.6], [7.7, 8.8], id="float-arrays"),
        pytest.param("str", ["a", "b"], ["c", "d"], id="str-arrays"),
    ],
)
async def test_insert_preserves_data_integrity(ctx, data_type, array_a, array_b):
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
