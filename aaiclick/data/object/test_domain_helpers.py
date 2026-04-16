"""
Parametrized tests for Object domain helper methods.

Each helper is a shortcut for a common with_columns() pattern.
Full with_columns() tests are in test_with_columns.py.
"""

from datetime import datetime, timezone

import pytest

from aaiclick import create_object_from_value

# =============================================================================
# Date / time helpers: with_year, with_month, with_day_of_week
# =============================================================================


DATES = [
    datetime(2023, 1, 2, tzinfo=timezone.utc),  # Monday
    datetime(2024, 6, 15, tzinfo=timezone.utc),  # Saturday
    datetime(2025, 12, 31, tzinfo=timezone.utc),  # Wednesday
]


@pytest.mark.parametrize(
    "helper,expected_col,expected",
    [
        pytest.param("with_year", "ts_year", [2023, 2024, 2025], id="year"),
        pytest.param("with_month", "ts_month", [1, 6, 12], id="month"),
        pytest.param("with_day_of_week", "ts_dow", [1, 6, 3], id="day_of_week"),
    ],
)
async def test_date_helpers(ctx, helper, expected_col, expected):
    """with_year, with_month, with_day_of_week extract correct values."""
    obj = await create_object_from_value({"ts": DATES})
    view = getattr(obj, helper)("ts")
    result = await view.data()
    assert result[expected_col] == expected


async def test_with_date_diff(ctx):
    """with_date_diff computes difference between two date columns in given unit."""
    obj = await create_object_from_value(
        {
            "start": [
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 3, 1, tzinfo=timezone.utc),
            ],
            "end": [
                datetime(2024, 1, 31, tzinfo=timezone.utc),
                datetime(2024, 6, 1, tzinfo=timezone.utc),
            ],
        }
    )
    view = obj.with_date_diff("day", "start", "end")
    result = await view.data()
    assert result["start_end_diff"] == [30, 92]


async def test_with_date_diff_custom_alias(ctx):
    """with_date_diff accepts alias override."""
    obj = await create_object_from_value(
        {
            "a": [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            "b": [datetime(2024, 4, 1, tzinfo=timezone.utc)],
        }
    )
    view = obj.with_date_diff("month", "a", "b", alias="months_apart")
    result = await view.data()
    assert result["months_apart"] == [3]


# =============================================================================
# String helpers: with_lower, with_upper, with_length, with_trim
# =============================================================================


@pytest.mark.parametrize(
    "helper,input_vals,col,expected_col,expected",
    [
        pytest.param("with_lower", ["Hello", "WORLD"], "name", "name_lower", ["hello", "world"], id="lower"),
        pytest.param("with_upper", ["hello", "world"], "name", "name_upper", ["HELLO", "WORLD"], id="upper"),
        pytest.param("with_length", ["", "hi", "hey"], "name", "name_length", [0, 2, 3], id="length"),
        pytest.param("with_trim", [" a ", " b"], "name", "name_trimmed", ["a", "b"], id="trim"),
    ],
)
async def test_string_helpers(ctx, helper, input_vals, col, expected_col, expected):
    """String domain helpers produce correctly named and typed columns."""
    obj = await create_object_from_value({col: input_vals})
    view = getattr(obj, helper)(col)
    result = await view.data()
    assert result[expected_col] == expected


async def test_string_helpers_custom_alias(ctx):
    """String helpers accept alias= override."""
    obj = await create_object_from_value({"city": ["New York", "London"]})
    view = obj.with_lower("city", alias="city_lc")
    result = await view.data()
    assert result["city_lc"] == ["new york", "london"]


# =============================================================================
# Math helpers: with_abs, with_log2, with_sqrt
# =============================================================================


@pytest.mark.parametrize(
    "helper,input_vals,expected",
    [
        pytest.param("with_abs", [-3, 0, 5], [3.0, 0.0, 5.0], id="abs"),
        pytest.param("with_log2", [1, 2, 4, 8], [0.0, 1.0, 2.0, 3.0], id="log2"),
        pytest.param("with_sqrt", [0, 1, 4, 9, 16], [0.0, 1.0, 2.0, 3.0, 4.0], id="sqrt"),
    ],
)
async def test_math_helpers(ctx, helper, input_vals, expected):
    """Math domain helpers produce correctly typed columns."""
    obj = await create_object_from_value({"x": input_vals})
    view = getattr(obj, helper)("x")
    result = await view.data()
    col = f"x_{helper.removeprefix('with_')}"
    assert result[col] == expected


# =============================================================================
# Bucketing helpers: with_bucket, with_hash_bucket
# =============================================================================


@pytest.mark.parametrize(
    "scores,bucket_size,expected_buckets",
    [
        pytest.param([0, 5, 10, 15, 24], 10, [0, 0, 1, 1, 2], id="size-10"),
        pytest.param([0, 99, 100, 199], 100, [0, 0, 1, 1], id="size-100"),
    ],
)
async def test_with_bucket(ctx, scores, bucket_size, expected_buckets):
    """with_bucket() divides values into equal-width integer buckets."""
    obj = await create_object_from_value({"score": scores})
    view = obj.with_bucket("score", bucket_size)
    result = await view.data()
    assert result["score_bucket"] == expected_buckets


async def test_with_hash_bucket(ctx):
    """with_hash_bucket() assigns values to n buckets deterministically."""
    obj = await create_object_from_value({"key": ["a", "b", "c", "d", "e", "f"]})
    view = obj.with_hash_bucket("key", 3)
    result = await view.data()
    # All values must be in range [0, n)
    assert all(0 <= v < 3 for v in result["key_hash"])
    # Same input always maps to same bucket (deterministic)
    view2 = obj.with_hash_bucket("key", 3)
    result2 = await view2.data()
    assert result["key_hash"] == result2["key_hash"]


# =============================================================================
# with_if: conditional column
# =============================================================================


async def test_with_if(ctx):
    """with_if() creates a conditional String column."""
    obj = await create_object_from_value({"score": [30, 60, 90]})
    view = obj.with_if("score >= 50", "'pass'", "'fail'", alias="result")
    data = await view.data()
    assert data["result"] == ["fail", "pass", "pass"]


async def test_with_if_chained_with_group_by(ctx):
    """with_if() result can be used as a group_by key."""
    obj = await create_object_from_value({"score": [10, 40, 60, 80, 95]})
    view = obj.with_if("score >= 50", "'high'", "'low'", alias="tier")
    result = await view.group_by("tier").count()
    data = await result.data()
    pairs = dict(zip(data["tier"], data["_count"], strict=False))
    assert pairs["low"] == 2
    assert pairs["high"] == 3


# =============================================================================
# with_split_by_char: split string column into Array
# =============================================================================


async def test_with_split_by_char(ctx):
    """with_split_by_char() splits a String column into Array(String)."""
    obj = await create_object_from_value({"tags": ["a,b,c", "d,e"]})
    view = obj.with_split_by_char("tags", ",")
    result = await view.data()
    assert result["tags_parts"] == [["a", "b", "c"], ["d", "e"]]


async def test_with_split_by_char_custom_alias(ctx):
    """with_split_by_char() accepts alias= override."""
    obj = await create_object_from_value({"csv": ["x:y", "z"]})
    view = obj.with_split_by_char("csv", ":", alias="parts")
    result = await view.data()
    assert result["parts"] == [["x", "y"], ["z"]]
