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


async def test_str_array_creation(ctx):
    """Test creating a string array object."""
    obj = await create_object_from_value(["apple", "banana", "cherry"])
    data = await obj.data()
    assert data == ["apple", "banana", "cherry"]


async def test_str_array_single_element(ctx):
    """Test creating a string array with single element."""
    obj = await create_object_from_value(["single"])
    data = await obj.data()
    assert data == ["single"]


async def test_str_array_with_empty_strings(ctx):
    """Test creating a string array containing empty strings."""
    obj = await create_object_from_value(["a", "", "b", ""])
    data = await obj.data()
    assert data == ["a", "", "b", ""]


async def test_str_array_with_spaces(ctx):
    """Test creating a string array with strings containing spaces."""
    obj = await create_object_from_value(["hello world", "foo bar", "test string"])
    data = await obj.data()
    assert data == ["hello world", "foo bar", "test string"]


async def test_str_array_unicode(ctx):
    """Test creating a string array with unicode characters."""
    obj = await create_object_from_value(["hello", "世界", "🎉"])
    data = await obj.data()
    assert data == ["hello", "世界", "🎉"]


async def test_str_array_preserves_order(ctx):
    """Test that string array preserves insertion order."""
    values = ["z", "a", "m", "b", "y"]
    obj = await create_object_from_value(values)
    data = await obj.data()
    assert data == values  # Order should be preserved
