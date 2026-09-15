"""
Tests for string (String) data type - scalars and arrays.

Note: String type does not support arithmetic operators (+, -) or statistics.
Only creation and data() retrieval are tested.
"""

import pytest

from aaiclick import create_object_from_value

# =============================================================================
# Scalar Tests
# =============================================================================


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("hello", id="simple"),
        pytest.param("", id="empty"),
        pytest.param("hello world", id="spaces"),
        pytest.param("hello@world.com", id="special-chars"),
        pytest.param("こんにちは", id="unicode"),
    ],
)
async def test_str_scalar_creation(ctx, value):
    """Test creating string scalar objects."""
    obj = await create_object_from_value(value)
    data = await obj.data()
    assert data == value


# =============================================================================
# Array Tests
# =============================================================================


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(["apple", "banana", "cherry"], id="simple"),
        pytest.param(["single"], id="single-element"),
        pytest.param(["a", "", "b", ""], id="empty-strings"),
        pytest.param(["hello world", "foo bar", "test string"], id="spaces"),
        pytest.param(["hello", "世界", "🎉"], id="unicode"),
        # Insertion order is preserved, so an unsorted input round-trips as-is.
        pytest.param(["z", "a", "m", "b", "y"], id="preserves-order"),
    ],
)
async def test_str_array_creation(ctx, value):
    """Test creating string array objects."""
    obj = await create_object_from_value(value)
    data = await obj.data()
    assert data == value
