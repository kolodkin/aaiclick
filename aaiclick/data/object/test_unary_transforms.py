"""
Tests for Object unary transform operators (year, month, lower, abs, etc.).
"""

from datetime import datetime, timezone

import pytest

from aaiclick import create_object_from_value

# =============================================================================
# Date/time transforms
# =============================================================================


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param(datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc), 2024, id="scalar"),
        pytest.param(
            [
                datetime(2023, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 6, 15, tzinfo=timezone.utc),
                datetime(2025, 12, 31, tzinfo=timezone.utc),
            ],
            [2023, 2024, 2025],
            id="array",
        ),
    ],
)
async def test_year(ctx, value, expected):
    """Extract year from DateTime values."""
    obj = await create_object_from_value(value)
    result = await obj.year()
    assert await result.data() == expected


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


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param([-3, -1, 0, 2, 5], [3.0, 1.0, 0.0, 2.0, 5.0], id="array"),
        pytest.param(-42, 42.0, id="scalar"),
    ],
)
async def test_abs(ctx, value, expected):
    """Absolute value of numeric values."""
    obj = await create_object_from_value(value)
    result = await obj.abs()
    assert await result.data() == expected


async def test_log2_array(ctx):
    """Log base 2 of numeric values."""
    obj = await create_object_from_value([1, 2, 4, 8, 16])
    result = await obj.log2()
    assert await result.data() == [0.0, 1.0, 2.0, 3.0, 4.0]


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param([0, 1, 4, 9, 16], [0.0, 1.0, 2.0, 3.0, 4.0], id="array"),
        pytest.param(25, 5.0, id="scalar"),
    ],
)
async def test_sqrt(ctx, value, expected):
    """Square root of numeric values."""
    obj = await create_object_from_value(value)
    result = await obj.sqrt()
    assert await result.data() == expected


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
    absolute = await obj.abs()
    result = await absolute.sum()
    assert await result.data() == 6.0


async def test_chain_length_then_max(ctx):
    """length() returns an Object that can be aggregated."""
    obj = await create_object_from_value(["a", "bb", "ccc", "dddd"])
    lengths = await obj.length()
    result = await lengths.max()
    assert await result.data() == 4
