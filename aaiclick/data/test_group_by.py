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
    """Multi-agg with field: operator mapping via agg()."""
    obj = await create_object_from_value({
        "category": ["A", "A", "B", "B"],
        "amount": [10, 20, 30, 40],
        "price": [1.5, 2.5, 3.5, 4.5],
    })
    result = await obj.group_by("category").agg({
        "amount": "sum",
        "price": "mean",
    })
    data = await result.data()

    lookup = {cat: i for i, cat in enumerate(data["category"])}
    a_idx = lookup["A"]
    b_idx = lookup["B"]

    assert data["amount"][a_idx] == 30
    assert data["amount"][b_idx] == 70
    assert abs(data["price"][a_idx] - 2.0) < THRESHOLD
    assert abs(data["price"][b_idx] - 4.0) < THRESHOLD


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
    schema = result.schema
    assert "category" in schema.columns
    assert "amount" in schema.columns


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
    """Single-group source returns single-row result."""
    obj = await create_object_from_value({
        "category": ["A", "A", "A"],
        "amount": [10, 20, 30],
    })
    result = await obj.group_by("category").sum("amount")
    data = await result.data()
    assert data["category"] == ["A"]
    assert data["amount"] == [60]


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


# =============================================================================
# HAVING tests
# =============================================================================


async def test_group_by_having_sum(ctx):
    """having('sum(amount) > 30') filters groups by sum."""
    obj = await create_object_from_value({
        "category": ["A", "A", "B", "B"],
        "amount": [10, 20, 30, 40],
    })
    result = await obj.group_by("category").having("sum(amount) > 50").sum("amount")
    data = await result.data()

    # A sum=30 (filtered out), B sum=70 (passes)
    assert data["category"] == ["B"]
    assert data["amount"] == [70]


async def test_group_by_having_count(ctx):
    """having('count() >= 3') filters groups by row count."""
    obj = await create_object_from_value({
        "category": ["A", "A", "A", "B", "B"],
        "amount": [1, 2, 3, 4, 5],
    })
    result = await obj.group_by("category").having("count() >= 3").count()
    data = await result.data()

    # A has 3 rows (passes), B has 2 rows (filtered out)
    assert data["category"] == ["A"]
    assert data["_count"] == [3]


async def test_group_by_having_with_agg(ctx):
    """having() works with multi-agg agg() method."""
    obj = await create_object_from_value({
        "category": ["A", "A", "B", "B", "C", "C", "C"],
        "amount": [10, 20, 30, 40, 1, 2, 3],
        "price": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
    })
    result = await obj.group_by("category").having("sum(amount) > 10").agg({
        "amount": "sum",
        "price": "mean",
    })
    data = await result.data()

    # A sum=30 (passes), B sum=70 (passes), C sum=6 (filtered out)
    cats = set(data["category"])
    assert "A" in cats
    assert "B" in cats
    assert "C" not in cats


async def test_group_by_having_with_where(ctx):
    """WHERE + HAVING together: filter rows then filter groups."""
    obj = await create_object_from_value({
        "category": ["A", "A", "A", "B", "B", "B"],
        "amount": [5, 15, 25, 10, 20, 30],
    })
    # WHERE filters rows first (amount > 10), then HAVING filters groups
    view = obj.view(where="amount > 10")
    result = await view.group_by("category").having("count() >= 2").count()
    data = await result.data()

    # After WHERE amount > 10: A has [15, 25] (2 rows), B has [20, 30] (2 rows)
    # HAVING count() >= 2: both pass
    pairs = dict(zip(data["category"], data["_count"]))
    assert pairs["A"] == 2
    assert pairs["B"] == 2


async def test_group_by_having_all_filtered(ctx):
    """All groups filtered out returns empty result Object."""
    obj = await create_object_from_value({
        "category": ["A", "B"],
        "amount": [10, 20],
    })
    result = await obj.group_by("category").having("sum(amount) > 1000").sum("amount")
    data = await result.data()

    # Both groups filtered out — empty result
    assert data["category"] == []
    assert data["amount"] == []


async def test_group_by_having_empty_string_raises(ctx):
    """Empty string raises ValueError."""
    obj = await create_object_from_value({
        "category": ["A", "B"],
        "amount": [10, 20],
    })
    with pytest.raises(ValueError, match="non-empty"):
        obj.group_by("category").having("")


# =============================================================================
# Chained HAVING tests
# =============================================================================


async def test_group_by_having_returns_new_query(ctx):
    """having() returns a new GroupByQuery, original is unchanged."""
    obj = await create_object_from_value({
        "category": ["A", "A", "B", "B"],
        "amount": [10, 20, 5, 15],
    })
    gbq1 = obj.group_by("category").having("sum(amount) > 10")
    gbq2 = gbq1.having("count() >= 2")
    assert gbq1 is not gbq2
    # gbq1 has one having clause, gbq2 has two
    data1 = await gbq1.sum("amount")
    result1 = await data1.data()
    # A: sum=30 > 10 ✓, B: sum=20 > 10 ✓
    assert set(result1["category"]) == {"A", "B"}

    data2 = await gbq2.sum("amount")
    result2 = await data2.data()
    # A: sum=30>10 AND count=2>=2 ✓, B: sum=20>10 AND count=2>=2 ✓
    assert set(result2["category"]) == {"A", "B"}


async def test_group_by_having_chained_and(ctx):
    """Multiple .having() calls chain with AND."""
    obj = await create_object_from_value({
        "category": ["A", "A", "A", "B", "B", "C"],
        "amount": [10, 20, 30, 5, 15, 100],
    })
    # A: sum=60, count=3 → passes both
    # B: sum=20, count=2 → fails count >= 3
    # C: sum=100, count=1 → fails count >= 3
    result = await (
        obj.group_by("category")
        .having("sum(amount) > 10")
        .having("count() >= 3")
        .sum("amount")
    )
    data = await result.data()
    assert data["category"] == ["A"]
    assert data["amount"] == [60]


async def test_group_by_or_having(ctx):
    """or_having() chains with OR."""
    obj = await create_object_from_value({
        "category": ["A", "A", "B", "B", "C"],
        "amount": [10, 20, 30, 40, 5],
    })
    # A: sum=30, count=2
    # B: sum=70, count=2
    # C: sum=5, count=1
    # sum > 50 → B passes; count = 1 → C passes; A fails both
    result = await (
        obj.group_by("category")
        .having("sum(amount) > 50")
        .or_having("count() = 1")
        .sum("amount")
    )
    data = await result.data()
    cats = set(data["category"])
    assert "B" in cats
    assert "C" in cats
    assert "A" not in cats


async def test_group_by_having_and_or_mixed(ctx):
    """Mixed .having() and .or_having() chaining."""
    obj = await create_object_from_value({
        "category": ["A", "A", "A", "B", "B", "C", "C"],
        "amount": [10, 20, 30, 5, 15, 100, 200],
    })
    # A: sum=60, count=3, max=30
    # B: sum=20, count=2, max=15
    # C: sum=300, count=2, max=200
    # HAVING (sum > 50) AND (count >= 3) OR (max > 100)
    # AND binds tighter: (sum>50 AND count>=3) OR (max>100)
    # A: (60>50 AND 3>=3)=true OR (30>100)=false → true
    # B: (20>50 AND 2>=3)=false OR (15>100)=false → false
    # C: (300>50 AND 2>=3)=false OR (200>100)=true → true
    result = await (
        obj.group_by("category")
        .having("sum(amount) > 50")
        .having("count() >= 3")
        .or_having("max(amount) > 100")
        .sum("amount")
    )
    data = await result.data()
    cats = set(data["category"])
    assert "A" in cats
    assert "C" in cats
    assert "B" not in cats


async def test_group_by_or_having_without_having_raises(ctx):
    """or_having() without prior having() raises ValueError."""
    obj = await create_object_from_value({
        "category": ["A", "B"],
        "amount": [10, 20],
    })
    with pytest.raises(ValueError, match="prior having"):
        obj.group_by("category").or_having("sum(amount) > 10")


async def test_group_by_or_having_empty_string_raises(ctx):
    """or_having() with empty string raises ValueError."""
    obj = await create_object_from_value({
        "category": ["A", "B"],
        "amount": [10, 20],
    })
    with pytest.raises(ValueError, match="non-empty"):
        obj.group_by("category").having("count() > 0").or_having("")
