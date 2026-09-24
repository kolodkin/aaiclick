"""
Tests for chained WHERE clauses: ``where()`` / ``or_where()`` on Objects and Views.
"""

import pytest

from aaiclick import create_object_from_value


async def test_view_where_single(ctx):
    """where() on Object creates a View with WHERE condition."""
    obj = await create_object_from_value([1, 2, 3, 4, 5], aai_id=True)
    view = obj.where("value > 3")
    result = await view.data()
    assert result == [4, 5]


async def test_view_where_chained_and(ctx):
    """Multiple where() calls chain with AND."""
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], aai_id=True)
    view = obj.view(where="value > 2").where("value < 8")
    result = await view.data()
    assert result == [3, 4, 5, 6, 7]


async def test_view_or_where(ctx):
    """or_where() chains with OR."""
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], aai_id=True)
    view = obj.view(where="value <= 2").or_where("value >= 9")
    result = await view.data()
    assert result == [1, 2, 9, 10]


async def test_view_where_returns_new_view(ctx):
    """where() returns a new View, original is unchanged."""
    obj = await create_object_from_value([1, 2, 3, 4, 5], aai_id=True)
    view1 = obj.where("value > 1")
    view2 = view1.where("value < 5")
    assert view1 is not view2
    assert await view1.data() == [2, 3, 4, 5]
    assert await view2.data() == [2, 3, 4]


async def test_view_where_and_or_mixed(ctx):
    """Mixed where() and or_where() chaining."""
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], aai_id=True)
    view = obj.where("value > 3").where("value < 7").or_where("value = 10")
    result = await view.data()
    assert result == [4, 5, 6, 10]


async def test_view_or_where_without_where_raises(ctx):
    """or_where() without prior where() raises ValueError."""
    obj = await create_object_from_value([1, 2, 3], aai_id=True)
    view = obj.view(limit=2)
    with pytest.raises(ValueError, match="prior where"):
        view.or_where("value > 1")


async def test_view_where_empty_string_raises(ctx):
    """Empty string raises ValueError."""
    obj = await create_object_from_value([1, 2, 3], aai_id=True)
    view = obj.view(where="value > 1")
    with pytest.raises(ValueError, match="non-empty"):
        view.where("")


async def test_view_or_where_empty_string_raises(ctx):
    """or_where() with empty string raises ValueError."""
    obj = await create_object_from_value([1, 2, 3], aai_id=True)
    view = obj.view(where="value > 1")
    with pytest.raises(ValueError, match="non-empty"):
        view.or_where("")


async def test_object_or_where_raises(ctx):
    """or_where() on Object raises ValueError (no prior where)."""
    obj = await create_object_from_value([1, 2, 3], aai_id=True)
    with pytest.raises(ValueError, match="prior where"):
        obj.or_where("value > 1")


async def test_view_where_with_dict_object(ctx):
    """where() works with dict objects."""
    obj = await create_object_from_value(
        {
            "category": ["A", "B", "C", "A", "B"],
            "amount": [10, 20, 30, 40, 50],
        },
        aai_id=True,
    )
    view = obj.where("amount > 15").where("amount < 45")
    result = await view.data()
    assert result["category"] == ["B", "C", "A"]
    assert result["amount"] == [20, 30, 40]


async def test_view_where_getitem_count(ctx):
    """Chaining where() + __getitem__ + count() returns filtered count."""
    obj = await create_object_from_value(
        {
            "name": ["a", "b", "c", "d", "e"],
            "score": [10, 20, 30, 40, 50],
        },
        aai_id=True,
    )
    high = obj.where("score >= 30")
    count = await high["name"].count().data()
    assert count == 3


async def test_view_or_where_with_group_by(ctx):
    """where/or_where views work with group_by."""
    obj = await create_object_from_value(
        {
            "category": ["A", "A", "B", "B", "C"],
            "amount": [5, 15, 10, 20, 100],
        },
        aai_id=True,
    )
    view = obj.where("amount > 10").or_where("category = 'C'")
    result = await view.group_by("category").sum("amount")
    data = await result.data()
    pairs = dict(zip(data["category"], data["amount"], strict=False))
    assert pairs["A"] == 15
    assert pairs["B"] == 20
    assert pairs["C"] == 100
