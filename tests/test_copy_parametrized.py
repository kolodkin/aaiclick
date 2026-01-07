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
        ("こんにちは", "こんにちは"),
    ],
)
async def test_scalar_copy(ctx, input_value, expected_output):
    """Test copying scalar objects across all data types."""
    obj = await ctx.create_object_from_value(input_value)

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
    "input_value,expected_output",
    [
        # Integer arrays
        ([1, 2, 3], [1, 2, 3]),
        ([0, 0, 0], [0, 0, 0]),
        ([-5, -10, -15], [-5, -10, -15]),
        ([42], [42]),
        ([1, 2, 3, 4, 5], [1, 2, 3, 4, 5]),
        # Float arrays
        ([1.5, 2.5, 3.5], [1.5, 2.5, 3.5]),
        ([0.0, 0.0], [0.0, 0.0]),
        ([-5.5, -10.5], [-5.5, -10.5]),
        ([3.14159], [3.14159]),
        # Boolean arrays (stored as UInt8)
        ([True, False, True], [1, 0, 1]),
        ([True, True, True], [1, 1, 1]),
        ([False, False, False], [0, 0, 0]),
        # String arrays
        (["apple", "banana", "cherry"], ["apple", "banana", "cherry"]),
        (["single"], ["single"]),
        (["hello", "world"], ["hello", "world"]),
        (["a", "", "b"], ["a", "", "b"]),
    ],
)
async def test_array_copy(ctx, input_value, expected_output):
    """Test copying array objects across all data types."""
    obj = await ctx.create_object_from_value(input_value)

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
    "input_value",
    [
        # Unsorted integer array
        [5, 1, 9, 3, 7],
        # Unsorted float array
        [5.5, 1.1, 9.9, 3.3],
        # Unsorted string array
        ["z", "a", "m", "b", "y"],
    ],
)
async def test_copy_preserves_order(ctx, input_value):
    """Test that copy preserves original array order."""
    obj = await ctx.create_object_from_value(input_value)

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
    "input_value",
    [
        # Various types
        (42),
        ([1, 2, 3]),
        (3.14159),
        ([1.5, 2.5, 3.5]),
        ("hello"),
        (["a", "b", "c"]),
    ],
)
async def test_multiple_copies_create_different_tables(ctx, input_value):
    """Test that multiple copies create different tables."""
    obj = await ctx.create_object_from_value(input_value)

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
