"""
Parametrized tests for copy operations across different data types.

Tests scalar and array copying with verification that new tables are created.
"""

import pytest

THRESHOLD = 1e-5


# =============================================================================
# Scalar Copy Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,input_value,expected_output",
    [
        # Integer scalars
        pytest.param("int", 42, 42, id="int-positive"),
        pytest.param("int", 0, 0, id="int-zero"),
        pytest.param("int", -100, -100, id="int-negative"),
        pytest.param("int", 1000000, 1000000, id="int-large"),
        # Float scalars
        pytest.param("float", 3.14159, 3.14159, id="float-pi"),
        pytest.param("float", 0.0, 0.0, id="float-zero"),
        pytest.param("float", -10.5, -10.5, id="float-negative"),
        pytest.param("float", 1.5, 1.5, id="float-small"),
        # Boolean scalars (stored as UInt8)
        pytest.param("bool", True, 1, id="bool-true"),
        pytest.param("bool", False, 0, id="bool-false"),
        # String scalars
        pytest.param("str", "hello", "hello", id="str-simple"),
        pytest.param("str", "", "", id="str-empty"),
        pytest.param("str", "hello world", "hello world", id="str-spaces"),
        pytest.param("str", "こんにちは", "こんにちは", id="str-unicode"),
    ],
)
async def test_scalar_copy(ctx, data_type, input_value, expected_output):
    """Test copying scalar objects across all data types."""
    obj = await create_object_from_value(input_value)

    copy = await obj.copy()
    data = await copy.data()

    # Verify data matches
    if isinstance(expected_output, float):
        assert abs(data - expected_output) < THRESHOLD
    else:
        assert data == expected_output

    # Verify tables are different
    assert copy.table != obj.table


# =============================================================================
# Array Copy Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,input_value,expected_output",
    [
        # Integer arrays
        pytest.param("int", [1, 2, 3], [1, 2, 3], id="int-array"),
        pytest.param("int", [0, 0, 0], [0, 0, 0], id="int-zeros"),
        pytest.param("int", [-5, -10, -15], [-5, -10, -15], id="int-negative"),
        pytest.param("int", [42], [42], id="int-single"),
        pytest.param("int", [1, 2, 3, 4, 5], [1, 2, 3, 4, 5], id="int-longer"),
        # Float arrays
        pytest.param("float", [1.5, 2.5, 3.5], [1.5, 2.5, 3.5], id="float-array"),
        pytest.param("float", [0.0, 0.0], [0.0, 0.0], id="float-zeros"),
        pytest.param("float", [-5.5, -10.5], [-5.5, -10.5], id="float-negative"),
        pytest.param("float", [3.14159], [3.14159], id="float-single"),
        # Boolean arrays (stored as UInt8)
        pytest.param("bool", [True, False, True], [1, 0, 1], id="bool-mixed"),
        pytest.param("bool", [True, True, True], [1, 1, 1], id="bool-all-true"),
        pytest.param("bool", [False, False, False], [0, 0, 0], id="bool-all-false"),
        # String arrays
        pytest.param("str", ["apple", "banana", "cherry"], ["apple", "banana", "cherry"], id="str-array"),
        pytest.param("str", ["single"], ["single"], id="str-single"),
        pytest.param("str", ["hello", "world"], ["hello", "world"], id="str-pair"),
        pytest.param("str", ["a", "", "b"], ["a", "", "b"], id="str-with-empty"),
    ],
)
async def test_array_copy(ctx, data_type, input_value, expected_output):
    """Test copying array objects across all data types."""
    obj = await create_object_from_value(input_value)

    copy = await obj.copy()
    data = await copy.data()

    # Verify data matches
    if len(expected_output) > 0 and isinstance(expected_output[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_output[i]) < THRESHOLD
    else:
        assert data == expected_output

    # Verify tables are different
    assert copy.table != obj.table


# =============================================================================
# Copy Preserves Order Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,input_value",
    [
        # Unsorted integer array
        pytest.param("int", [5, 1, 9, 3, 7], id="int-unsorted"),
        # Unsorted float array
        pytest.param("float", [5.5, 1.1, 9.9, 3.3], id="float-unsorted"),
        # Unsorted string array
        pytest.param("str", ["z", "a", "m", "b", "y"], id="str-unsorted"),
    ],
)
async def test_copy_preserves_order(ctx, data_type, input_value):
    """Test that copy preserves original array order."""
    obj = await create_object_from_value(input_value)

    copy = await obj.copy()
    data = await copy.data()

    # Verify order is preserved
    assert data == input_value

    # Verify tables are different
    assert copy.table != obj.table


# =============================================================================
# Multiple Copies Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,input_value",
    [
        # Various types
        pytest.param("int", 42, id="int-scalar"),
        pytest.param("int", [1, 2, 3], id="int-array"),
        pytest.param("float", 3.14159, id="float-scalar"),
        pytest.param("float", [1.5, 2.5, 3.5], id="float-array"),
        pytest.param("str", "hello", id="str-scalar"),
        pytest.param("str", ["a", "b", "c"], id="str-array"),
    ],
)
async def test_multiple_copies_create_different_tables(ctx, data_type, input_value):
    """Test that multiple copies create different tables."""
    obj = await create_object_from_value(input_value)

    copy1 = await obj.copy()
    copy2 = await obj.copy()
    copy3 = await obj.copy()

    # All tables should be different
    assert copy1.table != obj.table
    assert copy2.table != obj.table
    assert copy3.table != obj.table
    assert copy1.table != copy2.table
    assert copy2.table != copy3.table
    assert copy1.table != copy3.table
