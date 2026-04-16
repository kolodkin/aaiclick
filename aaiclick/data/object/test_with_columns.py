"""
Tests for Object.with_columns() and View.with_columns() functionality.
"""

import math

import pytest

from aaiclick import create_object_from_value
from aaiclick.data import Computed
from aaiclick.data.object import View

# =============================================================================
# Basic with_columns on Object
# =============================================================================


async def test_with_columns_computed(ctx):
    """Add computed columns; returns View; original unchanged."""
    obj = await create_object_from_value({
        "price": [10, 20, 30],
        "quantity": [2, 3, 1],
    })
    view = obj.with_columns({
        "total": Computed("Int64", "price * quantity"),
        "double_price": Computed("Int64", "price * 2"),
    })
    # Returns a View, not an Object
    assert isinstance(view, View)
    result = await view.data()
    assert result["total"] == [20, 60, 30]
    assert result["double_price"] == [20, 40, 60]
    # Original unchanged
    orig = await obj.data()
    assert list(orig.keys()) == ["price", "quantity"]


# =============================================================================
# Validation errors
# =============================================================================


async def test_with_columns_validation_errors(ctx):
    """Empty dict, scalar, and collision all raise ValueError."""
    obj = await create_object_from_value({"x": [1, 2]})
    # empty
    with pytest.raises(ValueError, match="non-empty"):
        obj.with_columns({})
    # collision
    with pytest.raises(ValueError, match="collides"):
        obj.with_columns({"x": Computed("Int64", "x + 1")})
    # scalar
    arr = await create_object_from_value([1, 2, 3])
    total = await arr.sum()
    with pytest.raises(ValueError, match="scalar"):
        total.with_columns({"y": Computed("Int64", "1")})


async def test_with_columns_expression_validation(ctx):
    """Semicolons and subqueries are rejected."""
    obj = await create_object_from_value({"x": [1, 2]})
    with pytest.raises(ValueError, match="must not contain"):
        obj.with_columns({"y": Computed("Int64", "x; DROP TABLE t")})
    with pytest.raises(ValueError, match="subqueries"):
        obj.with_columns({"y": Computed("Int64", "SELECT 1")})


# =============================================================================
# View.with_columns — chaining and constraint preservation
# =============================================================================


async def test_view_with_columns_preserves_constraints(ctx):
    """Computed columns work with WHERE and LIMIT."""
    obj = await create_object_from_value({
        "price": [10, 20, 30, 40, 50],
        "qty": [5, 3, 2, 1, 4],
    })
    # WHERE
    view_w = obj.where("price > 15").with_columns({
        "total": Computed("Int64", "price * qty"),
    })
    result_w = await view_w.data()
    assert result_w["price"] == [20, 30, 40, 50]
    assert result_w["total"] == [60, 60, 40, 200]
    # LIMIT
    view_l = obj.view(limit=3).with_columns({
        "doubled": Computed("Int64", "price * 2"),
    })
    result_l = await view_l.data()
    assert result_l["price"] == [10, 20, 30]
    assert result_l["doubled"] == [20, 40, 60]


async def test_view_with_columns_chaining(ctx):
    """Chained with_columns() calls merge; each returns independent View."""
    obj = await create_object_from_value({"a": [1, 2, 3]})
    view1 = obj.with_columns({"b": Computed("Int64", "a * 10")})
    view2 = view1.with_columns({"c": Computed("Int64", "a + 100")})
    assert view1 is not view2
    # view1 only has a and b
    r1 = await view1.data()
    assert set(r1.keys()) == {"a", "b"}
    assert r1["b"] == [10, 20, 30]
    # view2 has a, b, and c
    r2 = await view2.data()
    assert set(r2.keys()) == {"a", "b", "c"}
    assert r2["c"] == [101, 102, 103]
    # collision with existing computed raises
    with pytest.raises(ValueError, match="collides"):
        view1.with_columns({"b": Computed("Int64", "a + 2")})


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
    pairs = dict(zip(data["bucket"], data["_count"], strict=False))
    assert pairs["low"] == 3
    assert pairs["high"] == 3


# =============================================================================
# Domain helper methods
# =============================================================================


async def test_string_helpers(ctx):
    """with_lower, with_upper, with_length, with_trim."""
    obj = await create_object_from_value({"name": ["  Alice ", "BOB", " x"]})
    view = (
        obj.with_lower("name")
           .with_upper("name")
           .with_length("name")
           .with_trim("name")
    )
    result = await view.data()
    assert result["name_lower"] == ["  alice ", "bob", " x"]
    assert result["name_upper"] == ["  ALICE ", "BOB", " X"]
    assert result["name_length"] == [8, 3, 2]
    assert result["name_trimmed"] == ["Alice", "BOB", "x"]


async def test_numeric_helpers(ctx):
    """with_abs and with_sqrt."""
    obj = await create_object_from_value({"x": [-3, 0, 5]})
    view_abs = obj.with_abs("x")
    r_abs = await view_abs.data()
    assert r_abs["x_abs"] == [3.0, 0.0, 5.0]

    obj2 = await create_object_from_value({"x": [4, 9, -16]})
    view_sqrt = obj2.with_sqrt("x")
    r_sqrt = await view_sqrt.data()
    assert r_sqrt["x_sqrt"][:2] == [2.0, 3.0]
    assert math.isnan(r_sqrt["x_sqrt"][2])


async def test_with_bucket(ctx):
    """with_bucket() does integer division bucketing."""
    obj = await create_object_from_value({"score": [5, 15, 25, 35]})
    view = obj.with_bucket("score", 10)
    result = await view.data()
    assert result["score_bucket"] == [0, 1, 2, 3]


async def test_with_if_and_cast(ctx):
    """with_if() conditional and with_cast() type conversion."""
    obj = await create_object_from_value({"val": [1, 5, 10]})
    view = (
        obj.with_if("val > 3", "'high'", "'low'", alias="level")
           .with_cast("val", "String")
    )
    result = await view.data()
    assert result["level"] == ["low", "high", "high"]
    assert result["val_string"] == ["1", "5", "10"]


async def test_helper_alias_chaining_and_where(ctx):
    """Custom alias, chaining, and WHERE interaction."""
    obj = await create_object_from_value({
        "name": ["Alice", "Bob", "Charlie"],
        "score": [90, 40, 70],
    })
    # Custom alias
    view = obj.with_lower("name", alias="lc_name")
    r = await view.data()
    assert r["lc_name"] == ["alice", "bob", "charlie"]
    # Chaining + WHERE
    view2 = obj.where("score > 50").with_lower("name").with_bucket("score", 50)
    r2 = await view2.data()
    assert r2["name_lower"] == ["alice", "charlie"]
    assert r2["score_bucket"] == [1, 1]


async def test_with_bucket_group_by(ctx):
    """with_bucket() + group_by() for binned aggregation."""
    obj = await create_object_from_value({
        "score": [5, 15, 25, 12, 22, 8],
        "amount": [100, 200, 300, 150, 250, 50],
    })
    view = obj.with_bucket("score", 10)
    result = await view.group_by("score_bucket").sum("amount")
    data = await result.data()
    pairs = dict(zip(data["score_bucket"], data["amount"], strict=False))
    assert pairs[0] == 150   # scores 5, 8
    assert pairs[1] == 350   # scores 15, 12
    assert pairs[2] == 550   # scores 25, 22
