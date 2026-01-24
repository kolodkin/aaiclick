"""
Parametrized tests for object creation across different data types.

Tests scalar and array creation with various data types,
using pytest parametrization for comprehensive coverage.
"""

import pytest
from aaiclick import create_object_from_value, create_object

THRESHOLD = 1e-5


# =============================================================================
# Scalar Creation Tests
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
        pytest.param("str", "hello@world.com", "hello@world.com", id="str-special-chars"),
        pytest.param("str", "こんにちは", "こんにちは", id="str-unicode"),
    ],
)
async def test_scalar_creation(ctx, data_type, input_value, expected_output):
    """Test creating scalar objects across all data types."""
    obj = await create_object_from_value(input_value)
    data = await obj.data()

    # Use threshold for float comparisons, exact match for others
    if isinstance(expected_output, float):
        assert abs(data - expected_output) < THRESHOLD
    else:
        assert data == expected_output


# =============================================================================
# Array Creation Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,input_value,expected_output",
    [
        # Integer arrays
        pytest.param("int", [1, 2, 3, 4, 5], [1, 2, 3, 4, 5], id="int-array"),
        pytest.param("int", [0, 0, 0], [0, 0, 0], id="int-zeros"),
        pytest.param("int", [-5, -10, -15], [-5, -10, -15], id="int-negative"),
        pytest.param("int", [42], [42], id="int-single"),
        # Float arrays
        pytest.param("float", [1.5, 2.5, 3.5], [1.5, 2.5, 3.5], id="float-array"),
        pytest.param("float", [0.0, 0.0], [0.0, 0.0], id="float-zeros"),
        pytest.param("float", [-5.5, -10.5], [-5.5, -10.5], id="float-negative"),
        pytest.param("float", [3.14159], [3.14159], id="float-single"),
        # Boolean arrays (stored as UInt8)
        pytest.param("bool", [True, False, True, False], [1, 0, 1, 0], id="bool-mixed"),
        pytest.param("bool", [True, True, True], [1, 1, 1], id="bool-all-true"),
        pytest.param("bool", [False, False, False], [0, 0, 0], id="bool-all-false"),
        # String arrays
        pytest.param("str", ["apple", "banana", "cherry"], ["apple", "banana", "cherry"], id="str-array"),
        pytest.param("str", ["single"], ["single"], id="str-single"),
        pytest.param("str", ["a", "", "b", ""], ["a", "", "b", ""], id="str-with-empty"),
        pytest.param("str", ["hello world", "foo bar"], ["hello world", "foo bar"], id="str-with-spaces"),
        pytest.param("str", ["hello", "世界", "🎉"], ["hello", "世界", "🎉"], id="str-unicode"),
    ],
)
async def test_array_creation(ctx, data_type, input_value, expected_output):
    """Test creating array objects across all data types."""
    obj = await create_object_from_value(input_value)
    data = await obj.data()

    assert data == expected_output


# =============================================================================
# Array Order Preservation Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,input_value",
    [
        # Integer array - unsorted order
        pytest.param("int", [5, 1, 9, 3, 7], id="int-unsorted"),
        # Float array - unsorted order
        pytest.param("float", [5.5, 1.1, 9.9, 3.3], id="float-unsorted"),
        # String array - unsorted order
        pytest.param("str", ["z", "a", "m", "b", "y"], id="str-unsorted"),
    ],
)
async def test_array_preserves_order(ctx, data_type, input_value):
    """Test that arrays preserve insertion order (not sorted)."""
    obj = await create_object_from_value(input_value)
    data = await obj.data()

    assert data == input_value


# =============================================================================
# Edge Cases
# =============================================================================


@pytest.mark.parametrize(
    "data_type,input_value,expected_output",
    [
        # Zero values
        pytest.param("int", 0, 0, id="int-zero-scalar"),
        pytest.param("float", 0.0, 0.0, id="float-zero-scalar"),
        pytest.param("int", [0, 0, 0], [0, 0, 0], id="int-zero-array"),
        pytest.param("float", [0.0, 0.0], [0.0, 0.0], id="float-zero-array"),
        # Large numbers
        pytest.param("int", 999999999, 999999999, id="int-large-scalar"),
        pytest.param("int", [1000000, 2000000], [1000000, 2000000], id="int-large-array"),
        # Very small floats
        pytest.param("float", 0.001, 0.001, id="float-small-scalar"),
        pytest.param("float", [0.001, 0.002, 0.003], [0.001, 0.002, 0.003], id="float-small-array"),
        # Negative numbers
        pytest.param("int", -100, -100, id="int-negative-scalar"),
        pytest.param("int", [-100, -200, -300], [-100, -200, -300], id="int-negative-array"),
        # Empty strings in arrays
        pytest.param("str", ["", "", ""], ["", "", ""], id="str-empty-array"),
        # Unicode and special characters
        pytest.param("str", "🎉", "🎉", id="str-emoji"),
        pytest.param("str", ["α", "β", "γ"], ["α", "β", "γ"], id="str-greek"),
    ],
)
async def test_edge_cases(ctx, data_type, input_value, expected_output):
    """Test edge cases for object creation."""
    obj = await create_object_from_value(input_value)
    data = await obj.data()

    # Use threshold for float comparisons
    if isinstance(expected_output, float):
        assert abs(data - expected_output) < THRESHOLD
    elif isinstance(expected_output, list) and len(expected_output) > 0 and isinstance(expected_output[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_output[i]) < THRESHOLD
    else:
        assert data == expected_output
