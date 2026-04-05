"""
Tests for Object unary transform operators (year, month, lower, abs, etc.)
and computed column helper functions (cast, split_by_char).
"""

import math
from datetime import datetime, timezone

import pytest

from aaiclick import cast, create_object_from_value, literal, split_by_char


# =============================================================================
# Date/time transforms
# =============================================================================


async def test_year_scalar(ctx):
    """Extract year from a scalar DateTime."""
    dt = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
    obj = await create_object_from_value(dt)
    result = await obj.year()
    assert await result.data() == 2024


async def test_year_array(ctx):
    """Extract year from an array of DateTimes."""
    dates = [
        datetime(2023, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 6, 15, tzinfo=timezone.utc),
        datetime(2025, 12, 31, tzinfo=timezone.utc),
    ]
    obj = await create_object_from_value(dates)
    result = await obj.year()
    assert await result.data() == [2023, 2024, 2025]


async def test_month_array(ctx):
    """Extract month from an array of DateTimes."""
    dates = [
        datetime(2024, 1, 15, tzinfo=timezone.utc),
        datetime(2024, 6, 15, tzinfo=timezone.utc),
        datetime(2024, 12, 25, tzinfo=timezone.utc),
    ]
    obj = await create_object_from_value(dates)
    result = await obj.month()
    assert await result.data() == [1, 6, 12]


async def test_day_of_week_array(ctx):
    """Extract day of week from DateTimes (1=Mon, 7=Sun)."""
    dates = [
        datetime(2024, 1, 1, tzinfo=timezone.utc),   # Monday
        datetime(2024, 1, 3, tzinfo=timezone.utc),   # Wednesday
        datetime(2024, 1, 7, tzinfo=timezone.utc),   # Sunday
    ]
    obj = await create_object_from_value(dates)
    result = await obj.day_of_week()
    assert await result.data() == [1, 3, 7]


# =============================================================================
# String transforms
# =============================================================================


async def test_lower(ctx):
    """Lowercase string values."""
    obj = await create_object_from_value(["Hello", "WORLD", "FoO"])
    result = await obj.lower()
    assert await result.data() == ["hello", "world", "foo"]


async def test_upper(ctx):
    """Uppercase string values."""
    obj = await create_object_from_value(["Hello", "world", "FoO"])
    result = await obj.upper()
    assert await result.data() == ["HELLO", "WORLD", "FOO"]


async def test_length(ctx):
    """String length of values."""
    obj = await create_object_from_value(["", "hi", "hello"])
    result = await obj.length()
    assert await result.data() == [0, 2, 5]


async def test_trim(ctx):
    """Trim whitespace from string values."""
    obj = await create_object_from_value(["  hello  ", " world", "foo "])
    result = await obj.trim()
    assert await result.data() == ["hello", "world", "foo"]


# =============================================================================
# Math transforms
# =============================================================================


async def test_abs_array(ctx):
    """Absolute value of numeric array."""
    obj = await create_object_from_value([-3, -1, 0, 2, 5])
    result = await obj.abs()
    assert await result.data() == [3.0, 1.0, 0.0, 2.0, 5.0]


async def test_abs_scalar(ctx):
    """Absolute value of a scalar."""
    obj = await create_object_from_value(-42)
    result = await obj.abs()
    assert await result.data() == 42.0


async def test_log2_array(ctx):
    """Log base 2 of numeric values."""
    obj = await create_object_from_value([1, 2, 4, 8, 16])
    result = await obj.log2()
    assert await result.data() == [0.0, 1.0, 2.0, 3.0, 4.0]


async def test_sqrt_array(ctx):
    """Square root of numeric values."""
    obj = await create_object_from_value([0, 1, 4, 9, 16])
    result = await obj.sqrt()
    assert await result.data() == [0.0, 1.0, 2.0, 3.0, 4.0]


async def test_sqrt_scalar(ctx):
    """Square root of a scalar."""
    obj = await create_object_from_value(25)
    result = await obj.sqrt()
    assert await result.data() == 5.0


# =============================================================================
# Chaining: transforms return Objects that support further operations
# =============================================================================


async def test_chain_year_then_sum(ctx):
    """year() returns an Object that can be aggregated."""
    dates = [
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 6, 15, tzinfo=timezone.utc),
        datetime(2025, 12, 31, tzinfo=timezone.utc),
    ]
    obj = await create_object_from_value(dates)
    years = await obj.year()
    total = await years.sum()
    assert await total.data() == 2024 + 2024 + 2025


async def test_chain_abs_then_sum(ctx):
    """abs() returns an Object that can be aggregated."""
    obj = await create_object_from_value([-3, -1, 2])
    result = await (await obj.abs()).sum()
    assert await result.data() == 6.0


async def test_chain_length_then_max(ctx):
    """length() returns an Object that can be aggregated."""
    obj = await create_object_from_value(["a", "bb", "ccc", "dddd"])
    result = await (await obj.length()).max()
    assert await result.data() == 4


# =============================================================================
# Computed column helper functions (cast, split_by_char)
# =============================================================================


async def test_cast_nullable(ctx):
    obj = await create_object_from_value([{"n": "42"}, {"n": "abc"}, {"n": "100"}])
    result = await obj.with_columns({"n_int": cast("n", "UInt32")}).data()
    assert result["n_int"] == [42, None, 100]


async def test_cast_not_nullable(ctx):
    obj = await create_object_from_value([{"n": "42"}, {"n": "100"}])
    result = await obj.with_columns({"n_int": cast("n", "UInt32", nullable=False)}).data()
    assert result["n_int"] == [42, 100]


async def test_cast_returns_computed(ctx):
    c = cast("col", "UInt32")
    assert c.type == "Nullable(UInt32)"
    assert c.expression == "toUInt32OrNull(col)"


async def test_cast_not_nullable_returns_computed(ctx):
    c = cast("col", "Float64", nullable=False)
    assert c.type == "Float64"
    assert c.expression == "toFloat64(col)"


async def test_split_by_char_returns_computed(ctx):
    c = split_by_char("genres", ",")
    assert c.type == "Array(String)"
    assert c.expression == "splitByChar(',', genres)"


async def test_split_by_char_element_type(ctx):
    c = split_by_char("tags", ",", element_type="LowCardinality(String)")
    assert c.type == "Array(LowCardinality(String))"


async def test_split_by_char_explode(ctx):
    obj = await create_object_from_value([{"s": "a,b,c"}, {"s": "d,e"}])
    result = await obj.with_columns({"parts": split_by_char("s", ",")}).explode("parts").data()
    assert sorted(result["parts"]) == ["a", "b", "c", "d", "e"]


async def test_with_cast_method_nullable(ctx):
    obj = await create_object_from_value([{"n": "42"}, {"n": "bad"}, {"n": "7"}])
    result = await obj.with_cast("n", "UInt32", nullable=True).data()
    assert result["n_uint32"] == [42, None, 7]


async def test_with_cast_method_non_nullable(ctx):
    obj = await create_object_from_value([{"n": "42"}, {"n": "7"}])
    result = await obj.with_cast("n", "UInt32").data()
    assert result["n_uint32"] == [42, 7]


async def test_with_cast_method_string(ctx):
    obj = await create_object_from_value([{"n": 42}, {"n": 7}])
    result = await obj.with_cast("n", "String").data()
    assert result["n_string"] == ["42", "7"]


async def test_with_cast_method_alias(ctx):
    obj = await create_object_from_value([{"n": "10"}, {"n": "20"}])
    result = await obj.with_cast("n", "UInt32", alias="n_int").data()
    assert result["n_int"] == [10, 20]


async def test_with_split_by_char_method(ctx):
    obj = await create_object_from_value([{"genres": "Drama,Comedy"}, {"genres": "Action"}])
    result = await obj.with_split_by_char("genres", ",").explode("genres_parts").data()
    assert sorted(result["genres_parts"]) == ["Action", "Comedy", "Drama"]


async def test_with_split_by_char_method_alias(ctx):
    obj = await create_object_from_value([{"genres": "Drama,Comedy"}])
    result = await obj.with_split_by_char("genres", ",", alias="genre").explode("genre").data()
    assert sorted(result["genre"]) == ["Comedy", "Drama"]


# =============================================================================
# Computed column helper: literal()
# =============================================================================


def test_literal_string_returns_computed():
    c = literal("hello", "String")
    assert c.type == "String"
    assert c.expression == "'hello'"


def test_literal_int_returns_computed():
    c = literal(42, "UInt32")
    assert c.type == "UInt32"
    assert c.expression == "42"


def test_literal_float_returns_computed():
    c = literal(3.14, "Float64")
    assert c.type == "Float64"
    assert c.expression == "3.14"


def test_literal_bool_true_returns_computed():
    c = literal(True, "UInt8")
    assert c.type == "UInt8"
    assert c.expression == "true"


def test_literal_bool_false_returns_computed():
    c = literal(False, "UInt8")
    assert c.type == "UInt8"
    assert c.expression == "false"


def test_literal_string_escapes_quotes():
    c = literal("it's", "String")
    assert c.expression == r"'it\'s'"


async def test_literal_string_with_columns(ctx):
    obj = await create_object_from_value([{"x": 1}, {"x": 2}])
    result = await obj.with_columns({"source": literal("dataset_a", "String")}).data()
    assert result["source"] == ["dataset_a", "dataset_a"]


async def test_literal_int_with_columns(ctx):
    obj = await create_object_from_value([{"x": 10}, {"x": 20}])
    result = await obj.with_columns({"flag": literal(1, "UInt8")}).data()
    assert result["flag"] == [1, 1]


async def test_literal_bool_with_columns(ctx):
    obj = await create_object_from_value([{"x": 1}, {"x": 2}])
    result = await obj.with_columns({"active": literal(True, "UInt8")}).data()
    assert result["active"] == [1, 1]


async def test_literal_float_with_columns(ctx):
    obj = await create_object_from_value([{"x": 1}, {"x": 2}])
    result = await obj.with_columns({"pi": literal(3.14, "Float64")}).data()
    assert result["pi"] == [3.14, 3.14]


def test_literal_unsupported_type():
    with pytest.raises(TypeError, match="Unsupported literal type"):
        literal([1, 2], "Array(UInt8)")
