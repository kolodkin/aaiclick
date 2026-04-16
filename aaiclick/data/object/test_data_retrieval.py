"""
Tests for Object.data() method — orient modes and return types.

Verifies that data() returns the correct Python types for scalars,
arrays, and dicts, and that orient=ORIENT_RECORDS works correctly.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.models import ORIENT_DICT, ORIENT_RECORDS

# =============================================================================
# Return type by Object type (default orient)
# =============================================================================


@pytest.mark.parametrize(
    "value,expected",
    [
        pytest.param(42, 42, id="int-scalar"),
        pytest.param(3.14, 3.14, id="float-scalar"),
        pytest.param(True, True, id="bool-scalar"),
        pytest.param("hello", "hello", id="str-scalar"),
    ],
)
async def test_scalar_returns_value(ctx, value, expected):
    """Scalar Object.data() returns the bare Python value."""
    obj = await create_object_from_value(value)
    assert await obj.data() == expected


@pytest.mark.parametrize(
    "values",
    [
        pytest.param([1, 2, 3], id="int-array"),
        pytest.param([1.1, 2.2, 3.3], id="float-array"),
        pytest.param(["a", "b", "c"], id="str-array"),
        pytest.param([True, False, True], id="bool-array"),
    ],
)
async def test_array_returns_list(ctx, values):
    """Array Object.data() returns a list."""
    obj = await create_object_from_value(values)
    result = await obj.data()
    assert isinstance(result, list)
    assert result == values


# =============================================================================
# Dict orient modes
# =============================================================================


async def test_dict_default_orient(ctx):
    """dict Object.data() default is ORIENT_DICT — dict of lists."""
    obj = await create_object_from_value({"x": [1, 2], "y": [3, 4]})
    result = await obj.data()
    assert isinstance(result, dict)
    assert result["x"] == [1, 2]
    assert result["y"] == [3, 4]


async def test_dict_explicit_orient_dict(ctx):
    """data(orient=ORIENT_DICT) returns dict of lists."""
    obj = await create_object_from_value({"a": [10, 20], "b": [30, 40]})
    result = await obj.data(orient=ORIENT_DICT)
    assert result == {"a": [10, 20], "b": [30, 40]}


async def test_dict_orient_records(ctx):
    """data(orient=ORIENT_RECORDS) returns list of dicts."""
    obj = await create_object_from_value({"a": [10, 20], "b": [30, 40]})
    result = await obj.data(orient=ORIENT_RECORDS)
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0] == {"a": 10, "b": 30}
    assert result[1] == {"a": 20, "b": 40}


@pytest.mark.parametrize(
    "rows",
    [
        pytest.param([{"name": "alice", "score": 90}, {"name": "bob", "score": 75}], id="str-int"),
        pytest.param([{"x": 1.0, "y": 2.0}, {"x": 3.0, "y": 4.0}], id="float-float"),
    ],
)
async def test_orient_records_round_trip(ctx, rows):
    """Round-trip: list-of-dicts → Object → data(orient=ORIENT_RECORDS)."""
    obj = await create_object_from_value(rows)
    result = await obj.data(orient=ORIENT_RECORDS)
    assert result == rows


# =============================================================================
# data() on Views
# =============================================================================


async def test_view_data_orient_records(ctx):
    """data(orient=ORIENT_RECORDS) works on Views too."""
    obj = await create_object_from_value({"x": [1, 2, 3, 4], "y": [10, 20, 30, 40]})
    view = obj.where("x > 2")
    result = await view.data(orient=ORIENT_RECORDS)
    assert result == [{"x": 3, "y": 30}, {"x": 4, "y": 40}]


async def test_view_data_orient_dict(ctx):
    """data(orient=ORIENT_DICT) works on Views."""
    obj = await create_object_from_value({"x": [1, 2, 3], "y": [10, 20, 30]})
    view = obj.view(limit=2)
    result = await view.data(orient=ORIENT_DICT)
    assert result == {"x": [1, 2], "y": [10, 20]}
