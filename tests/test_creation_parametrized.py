"""
Parametrized tests for object creation across different data types.

Tests scalar and array creation with various data types,
using pytest parametrization for comprehensive coverage.
"""

import pytest

THRESHOLD = 1e-5


# =============================================================================
# Scalar Creation Tests
# =============================================================================


@pytest.mark.parametrize(
    "input_value,expected_output",
    [
        # Integer scalars
        (42, 42),
        (0, 0),
        (-100, -100),
        (1000000, 1000000),
        # Float scalars
        (3.14159, 3.14159),
        (0.0, 0.0),
        (-10.5, -10.5),
        (1.5, 1.5),
        # Boolean scalars (stored as UInt8)
        (True, 1),
        (False, 0),
        # String scalars
        ("hello", "hello"),
        ("", ""),
        ("hello world", "hello world"),
        ("hello@world.com", "hello@world.com"),
        ("こんにちは", "こんにちは"),
    ],
)
async def test_scalar_creation(ctx, input_value, expected_output):
    """Test creating scalar objects across all data types."""
    obj = await ctx.create_object_from_value(input_value)
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
    "input_value,expected_output",
    [
        # Integer arrays
        ([1, 2, 3, 4, 5], [1, 2, 3, 4, 5]),
        ([0, 0, 0], [0, 0, 0]),
        ([-5, -10, -15], [-5, -10, -15]),
        ([42], [42]),
        # Float arrays
        ([1.5, 2.5, 3.5], [1.5, 2.5, 3.5]),
        ([0.0, 0.0], [0.0, 0.0]),
        ([-5.5, -10.5], [-5.5, -10.5]),
        ([3.14159], [3.14159]),
        # Boolean arrays (stored as UInt8)
        ([True, False, True, False], [1, 0, 1, 0]),
        ([True, True, True], [1, 1, 1]),
        ([False, False, False], [0, 0, 0]),
        # String arrays
        (["apple", "banana", "cherry"], ["apple", "banana", "cherry"]),
        (["single"], ["single"]),
        (["a", "", "b", ""], ["a", "", "b", ""]),
        (["hello world", "foo bar"], ["hello world", "foo bar"]),
        (["hello", "世界", "🎉"], ["hello", "世界", "🎉"]),
    ],
)
async def test_array_creation(ctx, input_value, expected_output):
    """Test creating array objects across all data types."""
    obj = await ctx.create_object_from_value(input_value)
    data = await obj.data()

    assert data == expected_output


# =============================================================================
# Array Order Preservation Tests
# =============================================================================


@pytest.mark.parametrize(
    "input_value",
    [
        # Integer array - unsorted order
        [5, 1, 9, 3, 7],
        # Float array - unsorted order
        [5.5, 1.1, 9.9, 3.3],
        # String array - unsorted order
        ["z", "a", "m", "b", "y"],
    ],
)
async def test_array_preserves_order(ctx, input_value):
    """Test that arrays preserve insertion order (not sorted)."""
    obj = await ctx.create_object_from_value(input_value)
    data = await obj.data()

    assert data == input_value


# =============================================================================
# Edge Cases
# =============================================================================


@pytest.mark.parametrize(
    "input_value,expected_output",
    [
        # Zero values
        (0, 0),
        (0.0, 0.0),
        ([0, 0, 0], [0, 0, 0]),
        ([0.0, 0.0], [0.0, 0.0]),
        # Large numbers
        (999999999, 999999999),
        ([1000000, 2000000], [1000000, 2000000]),
        # Very small floats
        (0.001, 0.001),
        ([0.001, 0.002, 0.003], [0.001, 0.002, 0.003]),
        # Negative numbers
        (-100, -100),
        ([-100, -200, -300], [-100, -200, -300]),
        # Empty strings in arrays
        (["", "", ""], ["", "", ""]),
        # Unicode and special characters
        ("🎉", "🎉"),
        (["α", "β", "γ"], ["α", "β", "γ"]),
    ],
)
async def test_edge_cases(ctx, input_value, expected_output):
    """Test edge cases for object creation."""
    obj = await ctx.create_object_from_value(input_value)
    data = await obj.data()

    # Use threshold for float comparisons
    if isinstance(expected_output, float):
        assert abs(data - expected_output) < THRESHOLD
    elif isinstance(expected_output, list) and len(expected_output) > 0 and isinstance(expected_output[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_output[i]) < THRESHOLD
    else:
        assert data == expected_output
