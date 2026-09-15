"""
Tests for string/regex operators on Object class.

Tests match(), like(), ilike(), extract(), and replace() methods
on both scalar and array string Objects.
"""

import pytest

from aaiclick import create_object_from_value

# =============================================================================
# match() tests - RE2 regex matching, returns UInt8
# =============================================================================


@pytest.mark.parametrize(
    "value, pattern, expected",
    [
        pytest.param(["apple", "banana", "avocado"], "^a", [1, 0, 1], id="array-basic"),
        pytest.param("hello", "ell", 1, id="scalar"),
        pytest.param(["abc", "def"], "xyz", [0, 0], id="no-match"),
        pytest.param(["abc", "abcdef"], "abc", [1, 1], id="all-match"),
    ],
)
async def test_match(ctx, value, pattern, expected):
    obj = await create_object_from_value(value)
    result = await obj.match(pattern)
    assert await result.data() == expected


# =============================================================================
# like() tests - SQL LIKE pattern matching, returns UInt8
# =============================================================================


@pytest.mark.parametrize(
    "value, pattern, expected",
    [
        pytest.param(["apple", "banana", "avocado"], "a%", [1, 0, 1], id="array-prefix"),
        pytest.param(["apple", "banana", "avocado"], "%a", [0, 1, 0], id="array-suffix"),
        pytest.param(["apple", "banana", "cherry"], "%an%", [0, 1, 0], id="array-contains"),
        pytest.param("hello", "h%", 1, id="scalar"),
        pytest.param(["cat", "cut", "cot"], "c_t", [1, 1, 1], id="underscore-wildcard"),
    ],
)
async def test_like(ctx, value, pattern, expected):
    obj = await create_object_from_value(value)
    result = await obj.like(pattern)
    assert await result.data() == expected


# =============================================================================
# ilike() tests - case-insensitive LIKE, returns UInt8
# =============================================================================


@pytest.mark.parametrize(
    "value, pattern, expected",
    [
        pytest.param(["Apple", "BANANA", "avocado"], "a%", [1, 0, 1], id="case-insensitive"),
        pytest.param("Hello", "h%", 1, id="scalar"),
    ],
)
async def test_ilike(ctx, value, pattern, expected):
    obj = await create_object_from_value(value)
    result = await obj.ilike(pattern)
    assert await result.data() == expected


# =============================================================================
# extract() tests - regex group extraction, returns String
# =============================================================================


@pytest.mark.parametrize(
    "value, pattern, expected",
    [
        pytest.param(["user_123", "user_456", "admin_789"], "(\\d+)", ["123", "456", "789"], id="digits"),
        pytest.param(["abc", "def"], "(\\d+)", ["", ""], id="no-match"),
        pytest.param("hello_123", "(\\d+)", "123", id="scalar"),
    ],
)
async def test_extract(ctx, value, pattern, expected):
    obj = await create_object_from_value(value)
    result = await obj.extract(pattern)
    assert await result.data() == expected


# =============================================================================
# replace() tests - regex replacement, returns String
# =============================================================================


@pytest.mark.parametrize(
    "value, pattern, replacement, expected",
    [
        pytest.param(["hello world", "foo bar"], " ", "_", ["hello_world", "foo_bar"], id="basic"),
        pytest.param(["a1b2c3", "x9y8z7"], "\\d", "", ["abc", "xyz"], id="regex"),
        pytest.param(["aaa", "bbb"], "a", "x", ["xxx", "bbb"], id="all-occurrences"),
        pytest.param("hello world", "world", "there", "hello there", id="scalar"),
    ],
)
async def test_replace(ctx, value, pattern, replacement, expected):
    obj = await create_object_from_value(value)
    result = await obj.replace(pattern, replacement)
    assert await result.data() == expected


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
