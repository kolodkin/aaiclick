"""
Tests for Object.with_columns() and View.with_columns() functionality.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data import Computed


# =============================================================================
# Basic with_columns on Object
# =============================================================================


async def test_with_columns_single_computed(ctx):
    """Add a single computed column to a dict Object."""
    obj = await create_object_from_value({
        "price": [10, 20, 30],
        "quantity": [2, 3, 1],
    })
    view = obj.with_columns({
        "total": Computed("Int64", "price * quantity"),
    })
    result = await view.data()
    assert result["price"] == [10, 20, 30]
    assert result["quantity"] == [2, 3, 1]
    assert result["total"] == [20, 60, 30]


async def test_with_columns_multiple_computed(ctx):
    """Add multiple computed columns at once."""
    obj = await create_object_from_value({
        "a": [1, 2, 3],
        "b": [10, 20, 30],
    })
    view = obj.with_columns({
        "sum_ab": Computed("Int64", "a + b"),
        "diff_ab": Computed("Int64", "a - b"),
    })
    result = await view.data()
    assert result["sum_ab"] == [11, 22, 33]
    assert result["diff_ab"] == [-9, -18, -27]


async def test_with_columns_returns_view(ctx):
    """with_columns() returns a View, not an Object."""
    from aaiclick.data.object import View

    obj = await create_object_from_value({"x": [1, 2, 3]})
    view = obj.with_columns({"y": Computed("Int64", "x * 2")})
    assert isinstance(view, View)


async def test_with_columns_original_unchanged(ctx):
    """Original Object is not mutated by with_columns()."""
    obj = await create_object_from_value({"x": [1, 2, 3]})
    obj.with_columns({"y": Computed("Int64", "x * 2")})
    result = await obj.data()
    assert list(result.keys()) == ["x"]


# =============================================================================
# Validation errors
# =============================================================================


async def test_with_columns_empty_raises(ctx):
    """Empty dict raises ValueError."""
    obj = await create_object_from_value({"x": [1, 2]})
    with pytest.raises(ValueError, match="non-empty"):
        obj.with_columns({})


async def test_with_columns_scalar_raises(ctx):
    """Scalar Object raises ValueError."""
    obj = await create_object_from_value([1, 2, 3])
    total = await obj.sum()
    with pytest.raises(ValueError, match="scalar"):
        total.with_columns({"y": Computed("Int64", "1")})


async def test_with_columns_name_collision_raises(ctx):
    """Column name colliding with existing column raises ValueError."""
    obj = await create_object_from_value({"x": [1, 2]})
    with pytest.raises(ValueError, match="collides"):
        obj.with_columns({"x": Computed("Int64", "x + 1")})


async def test_with_columns_semicolon_raises(ctx):
    """Expression with semicolon raises ValueError."""
    obj = await create_object_from_value({"x": [1, 2]})
    with pytest.raises(ValueError, match="must not contain"):
        obj.with_columns({"y": Computed("Int64", "x; DROP TABLE t")})


async def test_with_columns_subquery_raises(ctx):
    """Expression with SELECT raises ValueError."""
    obj = await create_object_from_value({"x": [1, 2]})
    with pytest.raises(ValueError, match="subqueries"):
        obj.with_columns({"y": Computed("Int64", "SELECT 1")})


# =============================================================================
# View.with_columns — chaining and constraint preservation
# =============================================================================


async def test_view_with_columns_preserves_where(ctx):
    """Computed columns + WHERE filter work together."""
    obj = await create_object_from_value({
        "price": [10, 20, 30, 40],
        "qty": [5, 3, 2, 1],
    })
    view = obj.where("price > 15").with_columns({
        "total": Computed("Int64", "price * qty"),
    })
    result = await view.data()
    assert result["price"] == [20, 30, 40]
    assert result["total"] == [60, 60, 40]


async def test_view_with_columns_preserves_limit(ctx):
    """Computed columns + LIMIT work together."""
    obj = await create_object_from_value({
        "x": [1, 2, 3, 4, 5],
    })
    view = obj.view(limit=3).with_columns({
        "doubled": Computed("Int64", "x * 2"),
    })
    result = await view.data()
    assert result["x"] == [1, 2, 3]
    assert result["doubled"] == [2, 4, 6]


async def test_view_with_columns_additive(ctx):
    """Chained with_columns() calls merge computed columns."""
    obj = await create_object_from_value({
        "a": [1, 2, 3],
    })
    view1 = obj.with_columns({"b": Computed("Int64", "a * 10")})
    view2 = view1.with_columns({"c": Computed("Int64", "a + 100")})
    result = await view2.data()
    assert result["a"] == [1, 2, 3]
    assert result["b"] == [10, 20, 30]
    assert result["c"] == [101, 102, 103]


async def test_view_with_columns_returns_new_view(ctx):
    """View.with_columns() returns a new View, original unchanged."""
    obj = await create_object_from_value({"x": [1, 2]})
    view1 = obj.with_columns({"y": Computed("Int64", "x + 1")})
    view2 = view1.with_columns({"z": Computed("Int64", "x + 2")})
    assert view1 is not view2
    # view1 still has only x and y
    result1 = await view1.data()
    assert set(result1.keys()) == {"x", "y"}
    # view2 has x, y, and z
    result2 = await view2.data()
    assert set(result2.keys()) == {"x", "y", "z"}


async def test_view_with_columns_collision_with_computed_raises(ctx):
    """Adding a computed column that collides with an existing computed raises."""
    obj = await create_object_from_value({"x": [1, 2]})
    view = obj.with_columns({"y": Computed("Int64", "x + 1")})
    with pytest.raises(ValueError, match="collides"):
        view.with_columns({"y": Computed("Int64", "x + 2")})


# =============================================================================
# with_columns + group_by
# =============================================================================


async def test_with_columns_group_by(ctx):
    """group_by can use computed columns as keys."""
    obj = await create_object_from_value({
        "score": [10, 25, 35, 50, 75, 90],
    })
    view = obj.with_columns({
        "bucket": Computed("String", "if(score < 50, 'low', 'high')"),
    })
    result = await view.group_by("bucket").count()
    data = await result.data()
    pairs = dict(zip(data["bucket"], data["_count"]))
    assert pairs["low"] == 3
    assert pairs["high"] == 3


# =============================================================================
# Domain helper methods
# =============================================================================


async def test_with_lower(ctx):
    """with_lower() lowercases a string column."""
    obj = await create_object_from_value({"name": ["Alice", "BOB", "Charlie"]})
    view = obj.with_lower("name")
    result = await view.data()
    assert result["name_lower"] == ["alice", "bob", "charlie"]


async def test_with_upper(ctx):
    """with_upper() uppercases a string column."""
    obj = await create_object_from_value({"name": ["Alice", "bob"]})
    view = obj.with_upper("name")
    result = await view.data()
    assert result["name_upper"] == ["ALICE", "BOB"]


async def test_with_length(ctx):
    """with_length() computes string length."""
    obj = await create_object_from_value({"word": ["hi", "hello", "x"]})
    view = obj.with_length("word")
    result = await view.data()
    assert result["word_length"] == [2, 5, 1]


async def test_with_trim(ctx):
    """with_trim() strips whitespace."""
    obj = await create_object_from_value({"s": ["  hi ", " x", "ok"]})
    view = obj.with_trim("s")
    result = await view.data()
    assert result["s_trimmed"] == ["hi", "x", "ok"]


async def test_with_abs(ctx):
    """with_abs() computes absolute value."""
    obj = await create_object_from_value({"x": [-3, 0, 5]})
    view = obj.with_abs("x")
    result = await view.data()
    assert result["x_abs"] == [3.0, 0.0, 5.0]


async def test_with_sqrt(ctx):
    """with_sqrt() computes square root."""
    obj = await create_object_from_value({"x": [4, 9, 16]})
    view = obj.with_sqrt("x")
    result = await view.data()
    assert result["x_sqrt"] == [2.0, 3.0, 4.0]


async def test_with_bucket(ctx):
    """with_bucket() does integer division bucketing."""
    obj = await create_object_from_value({"score": [5, 15, 25, 35]})
    view = obj.with_bucket("score", 10)
    result = await view.data()
    assert result["score_bucket"] == [0, 1, 2, 3]


async def test_with_if(ctx):
    """with_if() creates conditional column."""
    obj = await create_object_from_value({"val": [1, 5, 10]})
    view = obj.with_if("val > 3", "'high'", "'low'", alias="level")
    result = await view.data()
    assert result["level"] == ["low", "high", "high"]


async def test_with_cast(ctx):
    """with_cast() converts column type."""
    obj = await create_object_from_value({"x": [1, 2, 3]})
    view = obj.with_cast("x", "String")
    result = await view.data()
    assert result["x_string"] == ["1", "2", "3"]


async def test_with_bucket_group_by(ctx):
    """with_bucket() + group_by() for binned aggregation."""
    obj = await create_object_from_value({
        "score": [5, 15, 25, 12, 22, 8],
        "amount": [100, 200, 300, 150, 250, 50],
    })
    view = obj.with_bucket("score", 10)
    result = await view.group_by("score_bucket").sum("amount")
    data = await result.data()
    pairs = dict(zip(data["score_bucket"], data["amount"]))
    assert pairs[0] == 150   # scores 5, 8
    assert pairs[1] == 350   # scores 15, 12
    assert pairs[2] == 550   # scores 25, 22


async def test_helper_custom_alias(ctx):
    """All helpers accept alias to override default name."""
    obj = await create_object_from_value({"name": ["Alice", "Bob"]})
    view = obj.with_lower("name", alias="lc_name")
    result = await view.data()
    assert "lc_name" in result
    assert result["lc_name"] == ["alice", "bob"]


async def test_helper_chaining(ctx):
    """Helpers chain since they return Views."""
    obj = await create_object_from_value({
        "name": ["Alice", "Bob"],
        "score": [85, 42],
    })
    view = (
        obj.with_lower("name")
           .with_bucket("score", 50)
    )
    result = await view.data()
    assert result["name_lower"] == ["alice", "bob"]
    assert result["score_bucket"] == [1, 0]


async def test_helper_with_where(ctx):
    """Helpers work on Views with existing constraints."""
    obj = await create_object_from_value({
        "name": ["Alice", "Bob", "Charlie"],
        "score": [90, 40, 70],
    })
    view = obj.where("score > 50").with_lower("name")
    result = await view.data()
    assert result["name_lower"] == ["alice", "charlie"]
