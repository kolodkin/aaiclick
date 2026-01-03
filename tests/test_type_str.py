"""
Tests for string (String) data type - scalars and arrays.

Note: String type does not support arithmetic operators (+, -) or statistics.
Only creation and data() retrieval are tested.
"""

from aaiclick import create_object_from_value


# =============================================================================
# Scalar Tests
# =============================================================================

async def test_str_scalar_creation():
    """Test creating a string scalar object."""
    obj = await create_object_from_value("hello")
    data = await obj.data()
    assert data == "hello"
    await obj.delete_table()


async def test_str_scalar_empty():
    """Test creating an empty string scalar object."""
    obj = await create_object_from_value("")
    data = await obj.data()
    assert data == ""
    await obj.delete_table()


async def test_str_scalar_with_spaces():
    """Test creating a string scalar with spaces."""
    obj = await create_object_from_value("hello world")
    data = await obj.data()
    assert data == "hello world"
    await obj.delete_table()


async def test_str_scalar_with_special_chars():
    """Test creating a string scalar with special characters."""
    obj = await create_object_from_value("hello@world.com")
    data = await obj.data()
    assert data == "hello@world.com"
    await obj.delete_table()


async def test_str_scalar_unicode():
    """Test creating a string scalar with unicode characters."""
    obj = await create_object_from_value("こんにちは")
    data = await obj.data()
    assert data == "こんにちは"
    await obj.delete_table()


# =============================================================================
# Array Tests
# =============================================================================

async def test_str_array_creation():
    """Test creating a string array object."""
    obj = await create_object_from_value(["apple", "banana", "cherry"])
    data = await obj.data()
    assert data == ["apple", "banana", "cherry"]
    await obj.delete_table()


async def test_str_array_single_element():
    """Test creating a string array with single element."""
    obj = await create_object_from_value(["single"])
    data = await obj.data()
    assert data == ["single"]
    await obj.delete_table()


async def test_str_array_with_empty_strings():
    """Test creating a string array containing empty strings."""
    obj = await create_object_from_value(["a", "", "b", ""])
    data = await obj.data()
    assert data == ["a", "", "b", ""]
    await obj.delete_table()


async def test_str_array_with_spaces():
    """Test creating a string array with strings containing spaces."""
    obj = await create_object_from_value(["hello world", "foo bar", "test string"])
    data = await obj.data()
    assert data == ["hello world", "foo bar", "test string"]
    await obj.delete_table()


async def test_str_array_unicode():
    """Test creating a string array with unicode characters."""
    obj = await create_object_from_value(["hello", "世界", "🎉"])
    data = await obj.data()
    assert data == ["hello", "世界", "🎉"]
    await obj.delete_table()


async def test_str_array_preserves_order():
    """Test that string array preserves insertion order."""
    values = ["z", "a", "m", "b", "y"]
    obj = await create_object_from_value(values)
    data = await obj.data()
    assert data == values  # Order should be preserved
    await obj.delete_table()


async def test_str_array_concat():
    """Test concatenating string arrays."""
    a = await create_object_from_value(["hello", "world"])
    b = await create_object_from_value(["foo", "bar", "baz"])

    result = await a.concat(b)
    data = await result.data()

    assert data == ["hello", "world", "foo", "bar", "baz"]

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()
