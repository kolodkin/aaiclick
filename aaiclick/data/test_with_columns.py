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
    pairs = dict(zip(data["bucket"], data["count"]))
    assert pairs["low"] == 3
    assert pairs["high"] == 3
