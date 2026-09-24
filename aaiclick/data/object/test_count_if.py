"""Tests for count_if() — conditional counting via countIf()."""

import pytest

from aaiclick import create_object_from_value


@pytest.mark.parametrize(
    "value, condition, expected",
    [
        pytest.param([1, 2, 3, 4, 5], "value > 3", 2, id="basic"),
        pytest.param([10, 20, 30], "value > 0", 3, id="all-match"),
        pytest.param([1, 2, 3], "value > 100", 0, id="none-match"),
        # count_if("1") counts every row, matching count().
        pytest.param([10, 20, 30, 40], "1", 4, id="always-true"),
    ],
)
async def test_count_if_str(ctx, value, condition, expected):
    """count_if with a str condition returns a scalar Object."""
    obj = await create_object_from_value(value)
    result = await obj.count_if(condition)
    assert await result.data() == expected


@pytest.mark.parametrize(
    "conditions, expected",
    [
        pytest.param(
            {"small": "value <= 2", "large": "value >= 4"},
            {"small": 2, "large": 2},
            id="basic",
        ),
        # A '1' condition in the dict form acts as a total count.
        pytest.param(
            {"total": "1", "gt3": "value > 3"},
            {"total": 5, "gt3": 2},
            id="total-via-always-true",
        ),
    ],
)
async def test_count_if_dict(ctx, conditions, expected):
    """count_if with a dict returns a dict Object with one row."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    result = await obj.count_if(conditions)
    assert await result.data() == expected


async def test_count_if_dict_on_dict_object(ctx):
    """count_if works on dict Objects with named columns."""
    obj = await create_object_from_value(
        {"name": ["alice", "bob", "carol", "dave"], "score": [90.0, 45.0, 80.0, 30.0]},
    )

    result = await obj.count_if(
        {
            "passing": "score >= 50",
            "failing": "score < 50",
        }
    )
    assert await result.data() == {"passing": 2, "failing": 2}


async def test_count_if_on_view_with_where(ctx):
    """count_if works on a View (Object.where())."""
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    view = obj.where("value <= 6")
    result = await view.count_if("value > 3")
    assert await result.data() == 3
