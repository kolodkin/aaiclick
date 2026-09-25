"""
Tests for Object.with_columns() and View.with_columns() functionality,
including the Computed helper functions cast(), split_by_char(), and literal().
"""

import pytest

from aaiclick import cast, create_object_from_value, literal, split_by_char
from aaiclick.data import Computed
from aaiclick.data.object import View

# =============================================================================
# Basic with_columns on Object
# =============================================================================


async def test_with_columns_computed(ctx):
    """Add computed columns; returns View; original unchanged."""
    obj = await create_object_from_value(
        {
            "price": [10, 20, 30],
            "quantity": [2, 3, 1],
        }
    )
    view = obj.with_columns(
        {
            "total": Computed("Int64", "price * quantity"),
            "double_price": Computed("Int64", "price * 2"),
        }
    )
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
    obj = await create_object_from_value(
        {
            "price": [10, 20, 30, 40, 50],
            "qty": [5, 3, 2, 1, 4],
        }
    )
    # WHERE
    view_w = obj.where("price > 15").with_columns(
        {
            "total": Computed("Int64", "price * qty"),
        }
    )
    result_w = await view_w.data()
    assert result_w["price"] == [20, 30, 40, 50]
    assert result_w["total"] == [60, 60, 40, 200]
    # LIMIT
    view_l = obj.view(limit=3).with_columns(
        {
            "doubled": Computed("Int64", "price * 2"),
        }
    )
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
    obj = await create_object_from_value(
        {
            "score": [10, 25, 35, 50, 75, 90],
        }
    )
    view = obj.with_columns(
        {
            "bucket": Computed("String", "if(score < 50, 'low', 'high')"),
        }
    )
    result = await view.group_by("bucket").count()
    data = await result.data()
    pairs = dict(zip(data["bucket"], data["_count"], strict=False))
    assert pairs["low"] == 3
    assert pairs["high"] == 3


# =============================================================================
# Computed column helper functions: cast(), split_by_char(), literal()
# =============================================================================


@pytest.mark.parametrize(
    "records, nullable, expected",
    [
        # Unparseable values become NULL under the nullable cast.
        pytest.param([{"n": "42"}, {"n": "abc"}, {"n": "100"}], True, [42, None, 100], id="nullable"),
        pytest.param([{"n": "42"}, {"n": "100"}], False, [42, 100], id="not-nullable"),
    ],
)
async def test_cast(ctx, records, nullable, expected):
    obj = await create_object_from_value(records)
    result = await obj.with_columns({"n_int": cast("n", "UInt32", nullable=nullable)}).data()
    assert result["n_int"] == expected


async def test_split_by_char_explode(ctx):
    obj = await create_object_from_value([{"s": "a,b,c"}, {"s": "d,e"}])
    result = await obj.with_columns({"parts": split_by_char("s", ",")}).explode("parts").data()
    assert sorted(result["parts"]) == ["a", "b", "c", "d", "e"]


@pytest.mark.parametrize(
    "col_name, value, ch_type, expected",
    [
        pytest.param("source", "dataset_a", "String", ["dataset_a", "dataset_a"], id="string"),
        pytest.param("flag", 1, "UInt8", [1, 1], id="int"),
        pytest.param("active", True, "UInt8", [1, 1], id="bool"),
        pytest.param("inactive", False, "UInt8", [0, 0], id="bool-false"),
        # Single quotes are escaped inside the emitted string literal.
        pytest.param("quoted", "it's", "String", ["it's", "it's"], id="string-escapes-quotes"),
        pytest.param("pi", 3.14, "Float64", [3.14, 3.14], id="float"),
    ],
)
async def test_literal_with_columns(ctx, col_name, value, ch_type, expected):
    obj = await create_object_from_value([{"x": 1}, {"x": 2}])
    result = await obj.with_columns({col_name: literal(value, ch_type)}).data()
    assert result[col_name] == expected


def test_literal_unsupported_type():
    with pytest.raises(TypeError, match="Unsupported literal type"):
        literal([1, 2], "Array(UInt8)")
