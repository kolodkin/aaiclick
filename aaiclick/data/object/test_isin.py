"""
Tests for isin() operator and with_isin() domain helper.

Tests membership testing via IN subquery on scalar, array, and dict Objects.
"""

from aaiclick import ORIENT_RECORDS, create_object_from_value

# =============================================================================
# isin() operator tests — returns UInt8 mask Object
# =============================================================================


async def test_isin_array_strings(ctx):
    """isin() on string array: returns 1 for values in the allowed set."""
    obj = await create_object_from_value(["a", "b", "c", "d"])
    allowed = await create_object_from_value(["a", "c"])
    result = await obj.isin(allowed)
    assert await result.data() == [1, 0, 1, 0]


async def test_isin_array_ints(ctx):
    """isin() on integer array: returns 1 for values in the allowed set."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    allowed = await create_object_from_value([2, 4])
    result = await obj.isin(allowed)
    assert await result.data() == [0, 1, 0, 1, 0]


async def test_isin_scalar(ctx):
    """isin() on scalar Object: returns 1 if the value is in the set."""
    obj = await create_object_from_value(42)
    allowed = await create_object_from_value([10, 42, 99])
    result = await obj.isin(allowed)
    assert await result.data() == 1


async def test_isin_scalar_not_found(ctx):
    """isin() on scalar Object: returns 0 if the value is not in the set."""
    obj = await create_object_from_value(7)
    allowed = await create_object_from_value([1, 2, 3])
    result = await obj.isin(allowed)
    assert await result.data() == 0


async def test_isin_no_matches(ctx):
    """isin() returns all zeros when no values match."""
    obj = await create_object_from_value(["x", "y", "z"])
    allowed = await create_object_from_value(["a", "b"])
    result = await obj.isin(allowed)
    assert await result.data() == [0, 0, 0]


async def test_isin_all_matches(ctx):
    """isin() returns all ones when all values match."""
    obj = await create_object_from_value([1, 2, 3])
    allowed = await create_object_from_value([1, 2, 3, 4, 5])
    result = await obj.isin(allowed)
    assert await result.data() == [1, 1, 1]


async def test_isin_with_python_list(ctx):
    """isin() accepts a Python list (auto-converted to Object)."""
    obj = await create_object_from_value(["cat", "dog", "bird"])
    result = await obj.isin(["cat", "bird"])
    assert await result.data() == [1, 0, 1]


async def test_isin_with_python_list_ints(ctx):
    """isin() accepts a Python list of ints."""
    obj = await create_object_from_value([10, 20, 30])
    result = await obj.isin([20, 30])
    assert await result.data() == [0, 1, 1]


async def test_isin_dict_column(ctx):
    """isin() on a selected column from a dict Object."""
    obj = await create_object_from_value(
        {
            "category": ["a", "b", "c", "d"],
            "value": [1, 2, 3, 4],
        }
    )
    allowed = await create_object_from_value(["a", "c"])
    result = await obj["category"].isin(allowed)
    assert await result.data() == [1, 0, 1, 0]


async def test_isin_result_chainable(ctx):
    """isin() result can be chained with sum() to count matches."""
    obj = await create_object_from_value(["a", "b", "c", "a", "b"])
    allowed = await create_object_from_value(["a", "b"])
    mask = await obj.isin(allowed)
    total = await mask.sum()
    assert await total.data() == 4


async def test_isin_with_view_source(ctx):
    """isin() works when source is a View with WHERE constraint."""
    obj = await create_object_from_value(
        {
            "name": ["alice", "bob", "charlie", "dave"],
            "score": [90, 80, 70, 60],
        }
    )
    allowed = await create_object_from_value(["alice", "charlie", "dave"])
    # Filter to score >= 70, then check isin
    view = obj.where("score >= 70")
    result = await view["name"].isin(allowed)
    assert await result.data() == [1, 0, 1]


async def test_isin_with_view_allowed(ctx):
    """isin() works when the allowed set is a View (filtered Object)."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    allowed_obj = await create_object_from_value(
        {
            "value": [1, 2, 3, 10, 20],
            "active": [1, 0, 1, 1, 0],
        }
    )
    # Only allow values where active=1
    allowed_view = allowed_obj.where("active = 1")["value"]
    result = await obj.isin(allowed_view)
    assert await result.data() == [1, 0, 1, 0, 0]


# =============================================================================
# with_isin() domain helper tests — adds computed boolean column to View
# =============================================================================


async def test_with_isin_basic(ctx):
    """with_isin() adds a boolean column indicating membership."""
    obj = await create_object_from_value(
        {
            "category": ["a", "b", "c", "d"],
            "value": [1, 2, 3, 4],
        }
    )
    allowed = await create_object_from_value(["a", "c"])
    view = obj.with_isin("category", allowed)
    result = await view.data()
    assert result["category_isin"] == [1, 0, 1, 0]
    assert result["category"] == ["a", "b", "c", "d"]
    assert result["value"] == [1, 2, 3, 4]


async def test_with_isin_custom_alias(ctx):
    """with_isin() respects alias parameter."""
    obj = await create_object_from_value(
        {
            "name": ["alice", "bob"],
            "age": [30, 25],
        }
    )
    allowed = await create_object_from_value(["alice"])
    view = obj.with_isin("name", allowed, alias="is_allowed")
    result = await view.data()
    assert result["is_allowed"] == [1, 0]


async def test_with_isin_chained_with_where(ctx):
    """with_isin() result can be filtered with where()."""
    obj = await create_object_from_value(
        {
            "category": ["a", "b", "c", "d"],
            "value": [10, 20, 30, 40],
        }
    )
    allowed = await create_object_from_value(["a", "c"])
    view = obj.with_isin("category", allowed, alias="is_in")
    filtered = view.where("is_in = 1")
    result = await filtered.data()
    assert result["category"] == ["a", "c"]
    assert result["value"] == [10, 30]


async def test_with_isin_chained_with_group_by(ctx):
    """with_isin() column works with group_by()."""
    obj = await create_object_from_value(
        {
            "category": ["a", "b", "a", "c"],
            "amount": [10, 20, 30, 40],
        }
    )
    allowed = await create_object_from_value(["a"])
    view = obj.with_isin("category", allowed, alias="is_target")
    result = await view.group_by("is_target").sum("amount")
    data = await result.data(orient=ORIENT_RECORDS)
    by_flag = {row["is_target"]: row["amount"] for row in data}
    assert by_flag[1] == 40  # a: 10+30
    assert by_flag[0] == 60  # b: 20 + c: 40


async def test_with_isin_on_view(ctx):
    """with_isin() works on a View (not just Object)."""
    obj = await create_object_from_value(
        {
            "name": ["alice", "bob", "charlie"],
            "score": [90, 80, 70],
        }
    )
    allowed = await create_object_from_value(["alice", "charlie"])
    view = obj.where("score >= 80").with_isin("name", allowed, alias="in_list")
    result = await view.data()
    assert result["name"] == ["alice", "bob"]
    assert result["in_list"] == [1, 0]
