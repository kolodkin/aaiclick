"""
Tests for group_by operations on Object and View.

Tests the GroupByQuery intermediate object and all supported aggregation
methods (sum, mean, min, max, count, std, var, agg).
"""

import pytest

from aaiclick import create_object_from_value

THRESHOLD = 1e-5


# =============================================================================
# Single-key aggregation tests
# =============================================================================


async def test_group_by_sum_single_key(ctx):
    """Basic sum with one group key."""
    obj = await create_object_from_value({
        "category": ["A", "A", "B", "B"],
        "amount": [10, 20, 30, 40],
    })
    result = await obj.group_by("category").sum("amount")
    data = await result.data()

    # Sort by category for deterministic comparison
    pairs = sorted(zip(data["category"], data["amount"]))
    assert pairs == [("A", 30), ("B", 70)]


async def test_group_by_sum_multiple_keys(ctx):
    """Sum with two group keys."""
    obj = await create_object_from_value({
        "region": ["East", "East", "West", "West"],
        "category": ["A", "B", "A", "B"],
        "amount": [10, 20, 30, 40],
    })
    result = await obj.group_by("region", "category").sum("amount")
    data = await result.data()

    # Build lookup for deterministic comparison
    lookup = {
        (r, c): a
        for r, c, a in zip(data["region"], data["category"], data["amount"])
    }
    assert lookup[("East", "A")] == 10
    assert lookup[("East", "B")] == 20
    assert lookup[("West", "A")] == 30
    assert lookup[("West", "B")] == 40


async def test_group_by_mean(ctx):
    """Mean aggregation."""
    obj = await create_object_from_value({
        "category": ["A", "A", "B", "B"],
        "amount": [10, 20, 30, 40],
    })
    result = await obj.group_by("category").mean("amount")
    data = await result.data()

    pairs = dict(zip(data["category"], data["amount"]))
    assert abs(pairs["A"] - 15.0) < THRESHOLD
    assert abs(pairs["B"] - 35.0) < THRESHOLD


async def test_group_by_min_max(ctx):
    """Min/max preserve source type."""
    obj = await create_object_from_value({
        "category": ["A", "A", "B", "B"],
        "value": [5, 1, 8, 3],
    })
    min_result = await obj.group_by("category").min("value")
    max_result = await obj.group_by("category").max("value")

    min_data = await min_result.data()
    max_data = await max_result.data()

    min_pairs = dict(zip(min_data["category"], min_data["value"]))
    max_pairs = dict(zip(max_data["category"], max_data["value"]))

    assert min_pairs["A"] == 1
    assert min_pairs["B"] == 3
    assert max_pairs["A"] == 5
    assert max_pairs["B"] == 8


async def test_group_by_count(ctx):
    """Count returns key + _count columns."""
    obj = await create_object_from_value({
        "category": ["A", "A", "A", "B", "B"],
        "amount": [1, 2, 3, 4, 5],
    })
    result = await obj.group_by("category").count()
    data = await result.data()

    pairs = dict(zip(data["category"], data["_count"]))
    assert pairs["A"] == 3
    assert pairs["B"] == 2


async def test_group_by_std_var(ctx):
    """Std/var return Float64."""
    obj = await create_object_from_value({
        "category": ["A", "A", "A", "A"],
        "value": [2, 4, 6, 8],
    })
    std_result = await obj.group_by("category").std("value")
    var_result = await obj.group_by("category").var("value")

    std_data = await std_result.data()
    var_data = await var_result.data()

    # stddevPop of [2,4,6,8] = sqrt(5) ≈ 2.236
    assert abs(std_data["value"][0] - 2.2360679774997898) < THRESHOLD
    # varPop of [2,4,6,8] = 5.0
    assert abs(var_data["value"][0] - 5.0) < THRESHOLD


# =============================================================================
# Multi-aggregation tests
# =============================================================================


async def test_group_by_agg_multiple(ctx):
    """Multi-agg with explicit names via agg()."""
    obj = await create_object_from_value({
        "category": ["A", "A", "B", "B"],
        "amount": [10, 20, 30, 40],
    })
    result = await obj.group_by("category").agg({
        "total": ("sum", "amount"),
        "avg_amt": ("mean", "amount"),
        "rows": ("count", None),
    })
    data = await result.data()

    lookup = {cat: i for i, cat in enumerate(data["category"])}
    a_idx = lookup["A"]
    b_idx = lookup["B"]

    assert data["total"][a_idx] == 30
    assert data["total"][b_idx] == 70
    assert abs(data["avg_amt"][a_idx] - 15.0) < THRESHOLD
    assert abs(data["avg_amt"][b_idx] - 35.0) < THRESHOLD
    assert data["rows"][a_idx] == 2
    assert data["rows"][b_idx] == 2


# =============================================================================
# Result Object behavior tests
# =============================================================================


async def test_group_by_result_is_dict_object(ctx):
    """Result is normal dict Object, supports data()."""
    obj = await create_object_from_value({
        "category": ["A", "B"],
        "amount": [10, 20],
    })
    result = await obj.group_by("category").sum("amount")

    # Should be a dict Object with category + amount columns
    meta = await result.metadata()
    assert "category" in meta.columns
    assert "amount" in meta.columns


async def test_group_by_result_field_selection(ctx):
    """result['column'] returns View."""
    obj = await create_object_from_value({
        "category": ["A", "A", "B", "B"],
        "amount": [10, 20, 30, 40],
    })
    result = await obj.group_by("category").sum("amount")

    # Field selection should work on result
    amounts = result["amount"]
    amount_data = await amounts.data()
    assert sorted(amount_data) == [30, 70]


async def test_group_by_result_further_aggregation(ctx):
    """result['amount'].sum() works."""
    obj = await create_object_from_value({
        "category": ["A", "A", "B", "B"],
        "amount": [10, 20, 30, 40],
    })
    result = await obj.group_by("category").sum("amount")

    # Further aggregation on result field
    total = await result["amount"].sum()
    total_data = await total.data()
    assert total_data == 100


async def test_group_by_orient_records(ctx):
    """result.data(orient='records') works."""
    obj = await create_object_from_value({
        "category": ["A", "B"],
        "amount": [10, 20],
    })
    result = await obj.group_by("category").sum("amount")
    data = await result.data(orient="records")

    # Should be list of dicts
    assert isinstance(data, list)
    lookup = {row["category"]: row["amount"] for row in data}
    assert lookup["A"] == 10
    assert lookup["B"] == 20


# =============================================================================
# Array Object support tests
# =============================================================================


async def test_group_by_array_object_count(ctx):
    """Array Object group_by('value').count() for value_counts."""
    arr = await create_object_from_value([1, 1, 2, 3, 3, 3])
    result = await arr.group_by("value").count()
    data = await result.data()

    pairs = dict(zip(data["value"], data["_count"]))
    assert pairs[1] == 2
    assert pairs[2] == 1
    assert pairs[3] == 3


# =============================================================================
# Validation tests
# =============================================================================


async def test_group_by_invalid_key_raises(ctx):
    """ValueError for nonexistent key."""
    obj = await create_object_from_value({
        "category": ["A", "B"],
        "amount": [10, 20],
    })
    with pytest.raises(ValueError, match="not found"):
        obj.group_by("nonexistent")


async def test_group_by_no_keys_raises(ctx):
    """ValueError for empty keys."""
    obj = await create_object_from_value({
        "category": ["A", "B"],
        "amount": [10, 20],
    })
    with pytest.raises(ValueError, match="at least one key"):
        obj.group_by()


async def test_group_by_aai_id_raises(ctx):
    """ValueError for grouping by aai_id."""
    obj = await create_object_from_value({
        "category": ["A", "B"],
        "amount": [10, 20],
    })
    with pytest.raises(ValueError, match="aai_id"):
        obj.group_by("aai_id")


# =============================================================================
# View support tests
# =============================================================================


async def test_group_by_on_multi_field_view(ctx):
    """obj[['cat','amt']].group_by('cat').sum('amt') works."""
    obj = await create_object_from_value({
        "category": ["A", "A", "B", "B"],
        "amount": [10, 20, 30, 40],
        "extra": [1, 2, 3, 4],
    })
    view = obj[["category", "amount"]]
    result = await view.group_by("category").sum("amount")
    data = await result.data()

    pairs = sorted(zip(data["category"], data["amount"]))
    assert pairs == [("A", 30), ("B", 70)]


async def test_group_by_on_single_field_view(ctx):
    """obj['x'].group_by('value').count() for value_counts."""
    obj = await create_object_from_value({
        "x": [1, 1, 2, 3, 3, 3],
        "y": [10, 20, 30, 40, 50, 60],
    })
    view = obj["x"]
    result = await view.group_by("value").count()
    data = await result.data()

    pairs = dict(zip(data["value"], data["_count"]))
    assert pairs[1] == 2
    assert pairs[2] == 1
    assert pairs[3] == 3


async def test_group_by_on_where_view(ctx):
    """obj.view(where='amount > 15').group_by('category').sum('amount')."""
    obj = await create_object_from_value({
        "category": ["A", "A", "B", "B"],
        "amount": [10, 20, 30, 40],
    })
    view = obj.view(where="amount > 15")
    result = await view.group_by("category").sum("amount")
    data = await result.data()

    pairs = dict(zip(data["category"], data["amount"]))
    # Only amount > 15: A has 20, B has 30+40=70
    assert pairs["A"] == 20
    assert pairs["B"] == 70


async def test_group_by_on_limit_view(ctx):
    """obj.view(limit=3).group_by('category').count()."""
    obj = await create_object_from_value({
        "category": ["A", "A", "B", "B"],
        "amount": [10, 20, 30, 40],
    })
    view = obj.view(limit=3)
    result = await view.group_by("category").count()
    data = await result.data()

    # First 3 rows (by snowflake id order): A, A, B
    total_count = sum(data["_count"])
    assert total_count == 3


# =============================================================================
# Edge case tests
# =============================================================================


async def test_group_by_empty_source(ctx):
    """Empty source returns empty dict Object."""
    obj = await create_object_from_value({
        "category": [],
        "amount": [],
    })
    result = await obj.group_by("category").sum("amount")
    data = await result.data()
    assert data == {}


async def test_group_by_string_keys(ctx):
    """String-typed group key columns."""
    obj = await create_object_from_value({
        "name": ["Alice", "Bob", "Alice", "Bob"],
        "score": [90, 80, 85, 95],
    })
    result = await obj.group_by("name").sum("score")
    data = await result.data()

    pairs = dict(zip(data["name"], data["score"]))
    assert pairs["Alice"] == 175
    assert pairs["Bob"] == 175
