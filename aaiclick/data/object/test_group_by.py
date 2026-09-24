"""
Tests for group_by operations on Object and View.

Tests the GroupByQuery intermediate object and all supported aggregation
methods (sum, mean, min, max, count, std, var, agg).
"""

import pytest

from aaiclick import create_object, create_object_from_value, delete_persistent_object
from aaiclick.data.models import FIELDTYPE_ARRAY, Agg, ColumnInfo, Computed, Schema

THRESHOLD = 1e-5


# =============================================================================
# Single-key aggregation tests
# =============================================================================


async def test_group_by_sum_single_key(ctx):
    """Basic sum with one group key."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B"],
            "amount": [10, 20, 30, 40],
        }
    )
    result = await obj.group_by("category").sum("amount")
    data = await result.data()

    # Sort by category for deterministic comparison
    pairs = sorted(zip(data["category"], data["amount"], strict=False))
    assert pairs == [("A", 30), ("B", 70)]


async def test_group_by_sum_multiple_keys(ctx):
    """Sum with two group keys."""
    obj = await create_object_from_value(
        {
            "region": ["East", "East", "West", "West"],
            "category": ["A", "B", "A", "B"],
            "amount": [10, 20, 30, 40],
        }
    )
    result = await obj.group_by("region", "category").sum("amount")
    data = await result.data()

    # Build lookup for deterministic comparison
    lookup = {(r, c): a for r, c, a in zip(data["region"], data["category"], data["amount"], strict=False)}
    assert lookup[("East", "A")] == 10
    assert lookup[("East", "B")] == 20
    assert lookup[("West", "A")] == 30
    assert lookup[("West", "B")] == 40


async def test_group_by_mean(ctx):
    """Mean aggregation."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B"],
            "amount": [10, 20, 30, 40],
        }
    )
    result = await obj.group_by("category").mean("amount")
    data = await result.data()

    pairs = dict(zip(data["category"], data["amount"], strict=False))
    assert abs(pairs["A"] - 15.0) < THRESHOLD
    assert abs(pairs["B"] - 35.0) < THRESHOLD


async def test_group_by_min_max(ctx):
    """Min/max preserve source type."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B"],
            "value": [5, 1, 8, 3],
        }
    )
    min_result = await obj.group_by("category").min("value")
    max_result = await obj.group_by("category").max("value")

    min_data = await min_result.data()
    max_data = await max_result.data()

    min_pairs = dict(zip(min_data["category"], min_data["value"], strict=False))
    max_pairs = dict(zip(max_data["category"], max_data["value"], strict=False))

    assert min_pairs["A"] == 1
    assert min_pairs["B"] == 3
    assert max_pairs["A"] == 5
    assert max_pairs["B"] == 8


async def test_group_by_count(ctx):
    """Count returns key + _count columns."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "A", "B", "B"],
            "amount": [1, 2, 3, 4, 5],
        }
    )
    result = await obj.group_by("category").count()
    data = await result.data()

    pairs = dict(zip(data["category"], data["_count"], strict=False))
    assert pairs["A"] == 3
    assert pairs["B"] == 2


async def test_group_by_std_var(ctx):
    """Std/var return Float64."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "A", "A"],
            "value": [2, 4, 6, 8],
        }
    )
    std_result = await obj.group_by("category").std("value")
    var_result = await obj.group_by("category").var("value")

    std_data = await std_result.data()
    var_data = await var_result.data()

    # stddevPop of [2,4,6,8] = sqrt(5) ≈ 2.236
    assert abs(std_data["value"][0] - 2.2360679774997898) < THRESHOLD
    # varPop of [2,4,6,8] = 5.0
    assert abs(var_data["value"][0] - 5.0) < THRESHOLD


async def test_group_by_any(ctx):
    """any() picks an arbitrary non-NULL value per group."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B"],
            "label": ["hello", "hello", "world", "world"],
        }
    )
    result = await obj.group_by("category").any("label")
    data = await result.data()

    pairs = dict(zip(data["category"], data["label"], strict=False))
    assert pairs["A"] == "hello"
    assert pairs["B"] == "world"


async def test_group_by_any_via_agg(ctx):
    """any() works through the agg() interface for multi-column collapse."""
    obj = await create_object_from_value(
        {
            "key": ["x", "x", "y"],
            "val_int": [10, 10, 20],
            "val_str": ["foo", "foo", "bar"],
        }
    )
    result = await obj.group_by("key").agg(
        {
            "val_int": "any",
            "val_str": "any",
        }
    )
    data = await result.data()

    pairs_int = dict(zip(data["key"], data["val_int"], strict=False))
    pairs_str = dict(zip(data["key"], data["val_str"], strict=False))
    assert pairs_int["x"] == 10
    assert pairs_int["y"] == 20
    assert pairs_str["x"] == "foo"
    assert pairs_str["y"] == "bar"


# =============================================================================
# Multi-aggregation tests
# =============================================================================


async def test_group_by_agg_multiple(ctx):
    """Multi-agg with field: operator mapping via agg()."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B"],
            "amount": [10, 20, 30, 40],
            "price": [1.5, 2.5, 3.5, 4.5],
        }
    )
    result = await obj.group_by("category").agg(
        {
            "amount": "sum",
            "price": "mean",
        }
    )
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
    obj = await create_object_from_value(
        {
            "category": ["A", "B"],
            "amount": [10, 20],
        }
    )
    result = await obj.group_by("category").sum("amount")

    # Should be a dict Object with category + amount columns
    schema = result.schema
    assert "category" in schema.columns
    assert "amount" in schema.columns


async def test_group_by_result_field_selection(ctx):
    """result['column'] returns View."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B"],
            "amount": [10, 20, 30, 40],
        }
    )
    result = await obj.group_by("category").sum("amount")

    # Field selection should work on result
    amounts = result["amount"]
    amount_data = await amounts.data()
    assert sorted(amount_data) == [30, 70]


async def test_group_by_result_further_aggregation(ctx):
    """result['amount'].sum() works."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B"],
            "amount": [10, 20, 30, 40],
        }
    )
    result = await obj.group_by("category").sum("amount")

    # Further aggregation on result field
    total = await result["amount"].sum()
    total_data = await total.data()
    assert total_data == 100


async def test_group_by_orient_records(ctx):
    """result.data(orient='records') works."""
    obj = await create_object_from_value(
        {
            "category": ["A", "B"],
            "amount": [10, 20],
        }
    )
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

    pairs = dict(zip(data["value"], data["_count"], strict=False))
    assert pairs[1] == 2
    assert pairs[2] == 1
    assert pairs[3] == 3


# =============================================================================
# Validation tests
# =============================================================================


@pytest.mark.parametrize(
    "keys, match",
    [
        pytest.param(("nonexistent",), "not found", id="nonexistent-key"),
        pytest.param((), "at least one key", id="no-keys"),
    ],
)
async def test_group_by_invalid_keys_raises(ctx, keys, match):
    obj = await create_object_from_value(
        {
            "category": ["A", "B"],
            "amount": [10, 20],
        }
    )
    with pytest.raises(ValueError, match=match):
        obj.group_by(*keys)


# =============================================================================
# View support tests
# =============================================================================


async def test_group_by_on_multi_field_view(ctx):
    """obj[['cat','amt']].group_by('cat').sum('amt') works."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B"],
            "amount": [10, 20, 30, 40],
            "extra": [1, 2, 3, 4],
        }
    )
    view = obj[["category", "amount"]]
    result = await view.group_by("category").sum("amount")
    data = await result.data()

    pairs = sorted(zip(data["category"], data["amount"], strict=False))
    assert pairs == [("A", 30), ("B", 70)]


async def test_group_by_on_single_field_view(ctx):
    """obj['x'].group_by('value').count() for value_counts."""
    obj = await create_object_from_value(
        {
            "x": [1, 1, 2, 3, 3, 3],
            "y": [10, 20, 30, 40, 50, 60],
        }
    )
    view = obj["x"]
    result = await view.group_by("value").count()
    data = await result.data()

    pairs = dict(zip(data["value"], data["_count"], strict=False))
    assert pairs[1] == 2
    assert pairs[2] == 1
    assert pairs[3] == 3


async def test_group_by_on_where_view(ctx):
    """obj.view(where='amount > 15').group_by('category').sum('amount')."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B"],
            "amount": [10, 20, 30, 40],
        }
    )
    view = obj.view(where="amount > 15")
    result = await view.group_by("category").sum("amount")
    data = await result.data()

    pairs = dict(zip(data["category"], data["amount"], strict=False))
    # Only amount > 15: A has 20, B has 30+40=70
    assert pairs["A"] == 20
    assert pairs["B"] == 70


async def test_group_by_on_limit_view(ctx):
    """obj.view(limit=3).group_by('category').count()."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B"],
            "amount": [10, 20, 30, 40],
        }
    )
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
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "A"],
            "amount": [10, 20, 30],
        }
    )
    result = await obj.group_by("category").sum("amount")
    data = await result.data()
    assert data["category"] == ["A"]
    assert data["amount"] == [60]


async def test_group_by_string_keys(ctx):
    """String-typed group key columns."""
    obj = await create_object_from_value(
        {
            "name": ["Alice", "Bob", "Alice", "Bob"],
            "score": [90, 80, 85, 95],
        }
    )
    result = await obj.group_by("name").sum("score")
    data = await result.data()

    pairs = dict(zip(data["name"], data["score"], strict=False))
    assert pairs["Alice"] == 175
    assert pairs["Bob"] == 175


# =============================================================================
# HAVING tests
# =============================================================================


async def test_group_by_having_sum(ctx):
    """having('sum(amount) > 30') filters groups by sum."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B"],
            "amount": [10, 20, 30, 40],
        }
    )
    result = await obj.group_by("category").having("sum(amount) > 50").sum("amount")
    data = await result.data()

    # A sum=30 (filtered out), B sum=70 (passes)
    assert data["category"] == ["B"]
    assert data["amount"] == [70]


async def test_group_by_having_count(ctx):
    """having('count() >= 3') filters groups by row count."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "A", "B", "B"],
            "amount": [1, 2, 3, 4, 5],
        }
    )
    result = await obj.group_by("category").having("count() >= 3").count()
    data = await result.data()

    # A has 3 rows (passes), B has 2 rows (filtered out)
    assert data["category"] == ["A"]
    assert data["_count"] == [3]


async def test_group_by_having_with_agg(ctx):
    """having() works with multi-agg agg() method."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B", "C", "C", "C"],
            "amount": [10, 20, 30, 40, 1, 2, 3],
            "price": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
        }
    )
    result = (
        await obj.group_by("category")
        .having("sum(amount) > 10")
        .agg(
            {
                "amount": "sum",
                "price": "mean",
            }
        )
    )
    data = await result.data()

    # A sum=30 (passes), B sum=70 (passes), C sum=6 (filtered out)
    cats = set(data["category"])
    assert "A" in cats
    assert "B" in cats
    assert "C" not in cats


async def test_group_by_having_with_where(ctx):
    """WHERE + HAVING together: filter rows then filter groups."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "A", "B", "B", "B"],
            "amount": [5, 15, 25, 10, 20, 30],
        }
    )
    # WHERE filters rows first (amount > 10), then HAVING filters groups
    view = obj.view(where="amount > 10")
    result = await view.group_by("category").having("count() >= 2").count()
    data = await result.data()

    # After WHERE amount > 10: A has [15, 25] (2 rows), B has [20, 30] (2 rows)
    # HAVING count() >= 2: both pass
    pairs = dict(zip(data["category"], data["_count"], strict=False))
    assert pairs["A"] == 2
    assert pairs["B"] == 2


async def test_group_by_having_all_filtered(ctx):
    """All groups filtered out returns empty result Object."""
    obj = await create_object_from_value(
        {
            "category": ["A", "B"],
            "amount": [10, 20],
        }
    )
    result = await obj.group_by("category").having("sum(amount) > 1000").sum("amount")
    data = await result.data()

    # Both groups filtered out — empty result
    assert data["category"] == []
    assert data["amount"] == []


async def test_group_by_having_empty_string_raises(ctx):
    """Empty string raises ValueError."""
    obj = await create_object_from_value(
        {
            "category": ["A", "B"],
            "amount": [10, 20],
        }
    )
    with pytest.raises(ValueError, match="non-empty"):
        obj.group_by("category").having("")


# =============================================================================
# Chained HAVING tests
# =============================================================================


async def test_group_by_having_returns_new_query(ctx):
    """having() returns a new GroupByQuery, original is unchanged."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B"],
            "amount": [10, 20, 5, 15],
        }
    )
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
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "A", "B", "B", "C"],
            "amount": [10, 20, 30, 5, 15, 100],
        }
    )
    # A: sum=60, count=3 → passes both
    # B: sum=20, count=2 → fails count >= 3
    # C: sum=100, count=1 → fails count >= 3
    result = await obj.group_by("category").having("sum(amount) > 10").having("count() >= 3").sum("amount")
    data = await result.data()
    assert data["category"] == ["A"]
    assert data["amount"] == [60]


async def test_group_by_or_having(ctx):
    """or_having() chains with OR."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B", "C"],
            "amount": [10, 20, 30, 40, 5],
        }
    )
    # A: sum=30, count=2
    # B: sum=70, count=2
    # C: sum=5, count=1
    # sum > 50 → B passes; count = 1 → C passes; A fails both
    result = await obj.group_by("category").having("sum(amount) > 50").or_having("count() = 1").sum("amount")
    data = await result.data()
    cats = set(data["category"])
    assert "B" in cats
    assert "C" in cats
    assert "A" not in cats


async def test_group_by_having_and_or_mixed(ctx):
    """Mixed .having() and .or_having() chaining."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "A", "B", "B", "C", "C"],
            "amount": [10, 20, 30, 5, 15, 100, 200],
        }
    )
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
    obj = await create_object_from_value(
        {
            "category": ["A", "B"],
            "amount": [10, 20],
        }
    )
    with pytest.raises(ValueError, match="prior having"):
        obj.group_by("category").or_having("sum(amount) > 10")


async def test_group_by_or_having_empty_string_raises(ctx):
    """or_having() with empty string raises ValueError."""
    obj = await create_object_from_value(
        {
            "category": ["A", "B"],
            "amount": [10, 20],
        }
    )
    with pytest.raises(ValueError, match="non-empty"):
        obj.group_by("category").having("count() > 0").or_having("")


# =============================================================================
# group_array_distinct tests
# =============================================================================


async def test_group_array_distinct_basic(ctx):
    """groupArrayDistinct collects unique values per group as an Array."""
    obj = await create_object_from_value(
        {
            "id": [1, 1, 2, 2, 2],
            "tag": ["a", "b", "a", "b", "b"],
        }
    )
    result = await obj.group_by("id").agg({"tag": "group_array_distinct"})
    data = await result.data()

    lookup = dict(zip(data["id"], data["tag"], strict=False))
    assert sorted(lookup[1]) == ["a", "b"]
    assert sorted(lookup[2]) == ["a", "b"]  # duplicate "b" collapsed


async def test_group_array_distinct_convenience(ctx):
    """group_array_distinct() convenience method delegates to agg()."""
    obj = await create_object_from_value(
        {
            "category": ["X", "X", "Y", "Y"],
            "label": ["p", "p", "q", "r"],
        }
    )
    result = await obj.group_by("category").group_array_distinct("label")
    data = await result.data()

    lookup = dict(zip(data["category"], data["label"], strict=False))
    assert sorted(lookup["X"]) == ["p"]  # both "p", deduplicated to one
    assert sorted(lookup["Y"]) == ["q", "r"]


# =============================================================================
# Multi-aggregation on the same column
# =============================================================================


async def test_agg_multi_on_same_column(ctx):
    """Multiple aggregations on the same column via list of Agg."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B"],
            "amount": [10, 20, 30, 40],
        }
    )
    result = await obj.group_by("category").agg(
        {
            "amount": [Agg("sum", "amount_sum"), Agg("mean", "amount_avg")],
        }
    )
    data = await result.data()

    lookup = {cat: i for i, cat in enumerate(data["category"])}
    a, b = lookup["A"], lookup["B"]

    assert data["amount_sum"][a] == 30
    assert data["amount_sum"][b] == 70
    assert abs(data["amount_avg"][a] - 15.0) < THRESHOLD
    assert abs(data["amount_avg"][b] - 35.0) < THRESHOLD


async def test_agg_single_agg_alias(ctx):
    """Single Agg renames the result column."""
    obj = await create_object_from_value(
        {
            "category": ["A", "B"],
            "amount": [10, 20],
        }
    )
    result = await obj.group_by("category").agg(
        {
            "amount": Agg("sum", "total"),
        }
    )
    data = await result.data()

    assert "total" in data
    assert "amount" not in data
    lookup = {cat: i for i, cat in enumerate(data["category"])}
    assert data["total"][lookup["A"]] == 10
    assert data["total"][lookup["B"]] == 20


async def test_agg_multi_mixed_spec(ctx):
    """Mix of plain op, single Agg, and list of Agg in one agg() call."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B"],
            "amount": [10, 20, 30, 40],
            "price": [1.5, 2.5, 3.5, 4.5],
        }
    )
    result = await obj.group_by("category").agg(
        {
            "amount": [Agg("sum", "amount_sum"), Agg("min", "amount_min")],
            "price": "mean",
        }
    )
    data = await result.data()

    lookup = {cat: i for i, cat in enumerate(data["category"])}
    a, b = lookup["A"], lookup["B"]

    assert data["amount_sum"][a] == 30
    assert data["amount_min"][a] == 10
    assert abs(data["price"][a] - 2.0) < THRESHOLD
    assert data["amount_sum"][b] == 70
    assert data["amount_min"][b] == 30
    assert abs(data["price"][b] - 4.0) < THRESHOLD


async def test_agg_multi_same_column_with_having(ctx):
    """Multi-agg on same column combined with HAVING clause."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "A", "B", "B"],
            "amount": [10, 20, 30, 5, 5],
        }
    )
    result = await (
        obj.group_by("category")
        .having("sum(amount) > 15")
        .agg({"amount": [Agg("sum", "amount_sum"), Agg("max", "amount_max")]})
    )
    data = await result.data()

    assert len(data["category"]) == 1
    assert data["category"][0] == "A"
    assert data["amount_sum"][0] == 60
    assert data["amount_max"][0] == 30


# =============================================================================
# Named group-by results
# =============================================================================


async def test_group_by_sum_with_name_global_scope(ctx):
    """sum(name=..., scope='global') routes through the named-table path."""
    obj = await create_object_from_value({"category": ["A", "A", "B"], "amount": [10, 20, 30]})

    result = await obj.group_by("category").sum("amount", name="gb_sum_named_global", scope="global")
    try:
        assert result.table == "p_gb_sum_named_global"
        data = await result.data()
        by_cat = dict(zip(data["category"], data["amount"], strict=True))
        assert by_cat == {"A": 30, "B": 30}
    finally:
        await delete_persistent_object("gb_sum_named_global", scope="global")


async def test_group_by_agg_with_name_temp_scope(ctx):
    """agg(name=...) defaults to temp_named scope (t_<name>_<id>)."""
    obj = await create_object_from_value({"category": ["A", "B"], "amount": [10, 20]})

    result = await obj.group_by("category").agg({"amount": "sum"}, name="gb_agg_named_temp")

    assert result.table.startswith("t_gb_agg_named_temp_")
    data = await result.data()
    assert sorted(zip(data["category"], data["amount"], strict=True)) == [
        ("A", 10),
        ("B", 20),
    ]


async def test_group_by_count_with_name(ctx):
    """count() also accepts name/scope (different code path — no column arg)."""
    obj = await create_object_from_value({"category": ["A", "A", "B"]})

    result = await obj.group_by("category").count(name="gb_count_named_temp")

    assert result.table.startswith("t_gb_count_named_temp_")
    data = await result.data()
    by_cat = dict(zip(data["category"], data["_count"], strict=True))
    assert by_cat == {"A": 2, "B": 1}


# =============================================================================
# Dot-notation column names (nested-dict ingest)
# =============================================================================

_NESTED_AMOUNT_ROWS = [
    {"category": "A", "m": {"amount": 10}},
    {"category": "A", "m": {"amount": 20}},
    {"category": "B", "m": {"amount": 35}},
]


async def test_group_by_agg_on_nested_dot_column(ctx):
    """Aggregating a dot-notation column must quote it in the GROUP BY SQL."""
    obj = await create_object_from_value(_NESTED_AMOUNT_ROWS)
    result = await obj.group_by("category").sum("m.amount")
    data = await result.data()

    # Result column keeps the dotted name, so data() re-nests it under "m".
    pairs = sorted(zip(data["category"], [m["amount"] for m in data["m"]], strict=False))
    assert pairs == [("A", 30), ("B", 35)]


async def test_group_by_nested_dot_key(ctx):
    """A dot-notation column used as a group key must be quoted in GROUP BY."""
    obj = await create_object_from_value(
        [
            {"m": {"category": "A"}, "amount": 10},
            {"m": {"category": "A"}, "amount": 20},
            {"m": {"category": "B"}, "amount": 35},
        ]
    )
    result = await obj.group_by("m.category").sum("amount")
    data = await result.data()

    pairs = sorted(zip([m["category"] for m in data["m"]], data["amount"], strict=False))
    assert pairs == [("A", 30), ("B", 35)]


async def test_group_by_agg_alias_avoids_dotted_result_column(ctx):
    """An explicit Agg alias renames a dotted source column to a flat result column."""
    obj = await create_object_from_value(_NESTED_AMOUNT_ROWS)
    result = await obj.group_by("category").agg({"m.amount": Agg("sum", "total")})
    data = await result.data()

    pairs = sorted(zip(data["category"], data["total"], strict=False))
    assert pairs == [("A", 30), ("B", 35)]


async def test_group_by_having_on_nested_dot_column(ctx):
    """The HAVING path aliases aggregates internally — dotted names must be quoted there too."""
    obj = await create_object_from_value(_NESTED_AMOUNT_ROWS)
    result = await obj.group_by("category").having("sum(`m.amount`) > 30").sum("m.amount")
    data = await result.data()

    assert data["category"] == ["B"]
    assert [m["amount"] for m in data["m"]] == [35]


# =============================================================================
# Aggregation table pattern: multi-source insert() collapsed via group_by().agg(any)
#
# Several sources with different schemas insert() into a shared table (missing
# nullable columns auto-fill with NULL), then group_by().agg() with any() merges
# them into one row per key.
# =============================================================================


async def test_aggregation_table_two_sources(ctx):
    """Two sources insert into shared table, collapse picks non-NULL values."""
    # Source A: has name and score
    source_a = await create_object_from_value(
        {
            "key": ["CVE-1", "CVE-2"],
            "name": ["heartbleed", "shellshock"],
            "score": [9.8, 7.5],
        }
    )

    # Source B: has key and label (different columns)
    source_b = await create_object_from_value(
        {
            "key": ["CVE-1", "CVE-3"],
            "label": ["critical", "medium"],
        }
    )

    # Create aggregation table with all columns
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "key": ColumnInfo("String"),
            "in_a": ColumnInfo("UInt8"),
            "in_b": ColumnInfo("UInt8"),
            "name": ColumnInfo("String", nullable=True),
            "score": ColumnInfo("Float64", nullable=True),
            "label": ColumnInfo("String", nullable=True),
        },
    )
    agg = await create_object(schema)

    # Insert source A with computed flag columns; label auto-fills NULL
    view_a = source_a.with_columns(
        {
            "in_a": Computed("UInt8", "1"),
            "in_b": Computed("UInt8", "0"),
        }
    )
    await agg.insert(view_a)

    # Insert source B with computed flag columns; name/score auto-fill NULL
    view_b = source_b.with_columns(
        {
            "in_a": Computed("UInt8", "0"),
            "in_b": Computed("UInt8", "1"),
        }
    )
    await agg.insert(view_b)

    # Collapse: GROUP BY key, merge with max/any
    result = await agg.group_by("key").agg(
        {
            "in_a": "max",
            "in_b": "max",
            "name": "any",
            "score": "any",
            "label": "any",
        }
    )

    data = await result.data()
    rows = {k: {col: data[col][i] for col in data} for i, k in enumerate(data["key"])}

    # CVE-1: present in both sources
    assert rows["CVE-1"]["in_a"] == 1
    assert rows["CVE-1"]["in_b"] == 1
    assert rows["CVE-1"]["name"] == "heartbleed"
    assert rows["CVE-1"]["score"] == 9.8
    assert rows["CVE-1"]["label"] == "critical"

    # CVE-2: only in source A
    assert rows["CVE-2"]["in_a"] == 1
    assert rows["CVE-2"]["in_b"] == 0
    assert rows["CVE-2"]["name"] == "shellshock"
    assert rows["CVE-2"]["score"] == 7.5

    # CVE-3: only in source B
    assert rows["CVE-3"]["in_a"] == 0
    assert rows["CVE-3"]["in_b"] == 1
    assert rows["CVE-3"]["label"] == "medium"


async def test_aggregation_table_three_sources(ctx):
    """Three sources merging via subset insert, no with_columns needed."""
    src1 = await create_object_from_value(
        {
            "id": ["A", "B"],
            "val1": [10, 20],
        }
    )
    src2 = await create_object_from_value(
        {
            "id": ["B", "C"],
            "val2": [200, 300],
        }
    )
    src3 = await create_object_from_value(
        {
            "id": ["A", "C"],
            "val3": ["x", "z"],
        }
    )

    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "id": ColumnInfo("String"),
            "val1": ColumnInfo("Int64", nullable=True),
            "val2": ColumnInfo("Int64", nullable=True),
            "val3": ColumnInfo("String", nullable=True),
        },
    )
    agg = await create_object(schema)

    # Subset insert: each source only has id + one val column
    await agg.insert(src1)
    await agg.insert(src2)
    await agg.insert(src3)

    result = await agg.group_by("id").agg(
        {
            "val1": "any",
            "val2": "any",
            "val3": "any",
        }
    )

    data = await result.data()
    rows = {k: {col: data[col][i] for col in data} for i, k in enumerate(data["id"])}

    assert rows["A"]["val1"] == 10
    assert rows["A"]["val3"] == "x"
    assert rows["B"]["val1"] == 20
    assert rows["B"]["val2"] == 200
    assert rows["C"]["val2"] == 300
    assert rows["C"]["val3"] == "z"


async def test_aggregation_table_duplicate_key_same_source(ctx):
    """When same key appears in one source, any() still picks a value."""
    src = await create_object_from_value(
        {
            "key": ["X", "X", "Y"],
            "value": [1, 2, 3],
        }
    )

    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "key": ColumnInfo("String"),
            "value": ColumnInfo("Int64", nullable=True),
        },
    )
    agg = await create_object(schema)

    # Direct insert — same schema, no subset needed
    await agg.insert(src)

    result = await agg.group_by("key").agg({"value": "any"})
    data = await result.data()

    pairs = dict(zip(data["key"], data["value"], strict=False))
    # any() picks an arbitrary value from the group — either 1 or 2
    assert pairs["X"] in (1, 2)
    assert pairs["Y"] == 3
