"""
Tests for Object unary transform operators (year, month, lower, abs, etc.).
"""

from datetime import datetime, timezone

from aaiclick import create_object_from_value

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
        datetime(2024, 1, 1, tzinfo=timezone.utc),  # Monday
        datetime(2024, 1, 3, tzinfo=timezone.utc),  # Wednesday
        datetime(2024, 1, 7, tzinfo=timezone.utc),  # Sunday
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
