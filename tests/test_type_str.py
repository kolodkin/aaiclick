"""
Tests for string (String) data type - scalars and arrays.

Note: String type does not support arithmetic operators (+, -) or statistics.
Only creation and data() retrieval are tested.
"""



# =============================================================================
# Scalar Tests
# =============================================================================

async def test_str_scalar_creation(ctx):
    """Test creating a string scalar object."""
    obj = await ctx.create_object_from_value("hello")
    data = await obj.data()
    assert data == "hello"


async def test_str_scalar_empty(ctx):
    """Test creating an empty string scalar object."""
    obj = await ctx.create_object_from_value("")
    data = await obj.data()
    assert data == ""


async def test_str_scalar_with_spaces(ctx):
    """Test creating a string scalar with spaces."""
    obj = await ctx.create_object_from_value("hello world")
    data = await obj.data()
    assert data == "hello world"


async def test_str_scalar_with_special_chars(ctx):
    """Test creating a string scalar with special characters."""
    obj = await ctx.create_object_from_value("hello@world.com")
    data = await obj.data()
    assert data == "hello@world.com"


async def test_str_scalar_unicode(ctx):
    """Test creating a string scalar with unicode characters."""
    obj = await ctx.create_object_from_value("こんにちは")
    data = await obj.data()
    assert data == "こんにちは"


# =============================================================================
# Array Tests
# =============================================================================

async def test_str_array_creation(ctx):
    """Test creating a string array object."""
    obj = await ctx.create_object_from_value(["apple", "banana", "cherry"])
    data = await obj.data()
    assert data == ["apple", "banana", "cherry"]


async def test_str_array_single_element(ctx):
    """Test creating a string array with single element."""
    obj = await ctx.create_object_from_value(["single"])
    data = await obj.data()
    assert data == ["single"]


async def test_str_array_with_empty_strings(ctx):
    """Test creating a string array containing empty strings."""
    obj = await ctx.create_object_from_value(["a", "", "b", ""])
    data = await obj.data()
    assert data == ["a", "", "b", ""]


async def test_str_array_with_spaces(ctx):
    """Test creating a string array with strings containing spaces."""
    obj = await ctx.create_object_from_value(["hello world", "foo bar", "test string"])
    data = await obj.data()
    assert data == ["hello world", "foo bar", "test string"]


async def test_str_array_unicode(ctx):
    """Test creating a string array with unicode characters."""
    obj = await ctx.create_object_from_value(["hello", "世界", "🎉"])
    data = await obj.data()
    assert data == ["hello", "世界", "🎉"]


async def test_str_array_preserves_order(ctx):
    """Test that string array preserves insertion order."""
    values = ["z", "a", "m", "b", "y"]
    obj = await ctx.create_object_from_value(values)
    data = await obj.data()
    assert data == values  # Order should be preserved


async def test_str_array_concat(ctx):
    """Test concatenating string arrays."""
    a = await ctx.create_object_from_value(["hello", "world"])
    b = await ctx.create_object_from_value(["foo", "bar", "baz"])

    result = await a.concat(b)
    data = await result.data()

    assert data == ["hello", "world", "foo", "bar", "baz"]

    await ctx.delete(result)
