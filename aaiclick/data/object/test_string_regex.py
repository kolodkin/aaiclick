"""
Tests for string/regex operators on Object class.

Tests match(), like(), ilike(), extract(), and replace() methods
on both scalar and array string Objects.
"""

from aaiclick import create_object_from_value

# =============================================================================
# match() tests - RE2 regex matching, returns UInt8
# =============================================================================


async def test_match_array_basic(ctx):
    obj = await create_object_from_value(["apple", "banana", "avocado"])
    result = await obj.match("^a")
    assert await result.data() == [1, 0, 1]


async def test_match_scalar(ctx):
    obj = await create_object_from_value("hello")
    result = await obj.match("ell")
    assert await result.data() == 1


async def test_match_no_match(ctx):
    obj = await create_object_from_value(["abc", "def"])
    result = await obj.match("xyz")
    assert await result.data() == [0, 0]


async def test_match_all_match(ctx):
    obj = await create_object_from_value(["abc", "abcdef"])
    result = await obj.match("abc")
    assert await result.data() == [1, 1]


# =============================================================================
# like() tests - SQL LIKE pattern matching, returns UInt8
# =============================================================================


async def test_like_array_prefix(ctx):
    obj = await create_object_from_value(["apple", "banana", "avocado"])
    result = await obj.like("a%")
    assert await result.data() == [1, 0, 1]


async def test_like_array_suffix(ctx):
    obj = await create_object_from_value(["apple", "banana", "avocado"])
    result = await obj.like("%a")
    assert await result.data() == [0, 1, 0]


async def test_like_array_contains(ctx):
    obj = await create_object_from_value(["apple", "banana", "cherry"])
    result = await obj.like("%an%")
    assert await result.data() == [0, 1, 0]


async def test_like_scalar(ctx):
    obj = await create_object_from_value("hello")
    result = await obj.like("h%")
    assert await result.data() == 1


async def test_like_underscore_wildcard(ctx):
    obj = await create_object_from_value(["cat", "cut", "cot"])
    result = await obj.like("c_t")
    assert await result.data() == [1, 1, 1]


# =============================================================================
# ilike() tests - case-insensitive LIKE, returns UInt8
# =============================================================================


async def test_ilike_case_insensitive(ctx):
    obj = await create_object_from_value(["Apple", "BANANA", "avocado"])
    result = await obj.ilike("a%")
    assert await result.data() == [1, 0, 1]


async def test_ilike_scalar(ctx):
    obj = await create_object_from_value("Hello")
    result = await obj.ilike("h%")
    assert await result.data() == 1


# =============================================================================
# extract() tests - regex group extraction, returns String
# =============================================================================


async def test_extract_digits(ctx):
    obj = await create_object_from_value(["user_123", "user_456", "admin_789"])
    result = await obj.extract("(\\d+)")
    assert await result.data() == ["123", "456", "789"]


async def test_extract_no_match(ctx):
    obj = await create_object_from_value(["abc", "def"])
    result = await obj.extract("(\\d+)")
    assert await result.data() == ["", ""]


async def test_extract_scalar(ctx):
    obj = await create_object_from_value("hello_123")
    result = await obj.extract("(\\d+)")
    assert await result.data() == "123"


# =============================================================================
# replace() tests - regex replacement, returns String
# =============================================================================


async def test_replace_basic(ctx):
    obj = await create_object_from_value(["hello world", "foo bar"])
    result = await obj.replace(" ", "_")
    assert await result.data() == ["hello_world", "foo_bar"]


async def test_replace_regex(ctx):
    obj = await create_object_from_value(["a1b2c3", "x9y8z7"])
    result = await obj.replace("\\d", "")
    assert await result.data() == ["abc", "xyz"]


async def test_replace_all_occurrences(ctx):
    obj = await create_object_from_value(["aaa", "bbb"])
    result = await obj.replace("a", "x")
    assert await result.data() == ["xxx", "bbb"]


async def test_replace_scalar(ctx):
    obj = await create_object_from_value("hello world")
    result = await obj.replace("world", "there")
    assert await result.data() == "hello there"


# =============================================================================
# Chaining tests - regex results used in further operations
# =============================================================================


async def test_match_result_used_in_sum(ctx):
    """match() returns UInt8 which can be summed to count matches."""
    obj = await create_object_from_value(["apple", "banana", "avocado", "apricot"])
    matches = await obj.match("^a")
    total = await matches.sum()
    assert await total.data() == 3


async def test_extract_then_match(ctx):
    """Chain extract followed by match."""
    obj = await create_object_from_value(["id:123", "id:abc", "id:456"])
    extracted = await obj.extract("id:(.*)")
    is_numeric = await extracted.match("^\\d+$")
    assert await is_numeric.data() == [1, 0, 1]
