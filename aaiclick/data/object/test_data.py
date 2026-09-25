"""
Tests for Object.data() — return types, orient modes, and keyword-only kwargs.

Covers:
- the Python type data() returns for scalar, array, and dict Objects
- ``orient="dict"`` / ``orient="records"`` on Objects and Views
- ``limit=1000`` safety default for array reads
- ``order_by``, ``offset``, ``limit`` keyword-only args
- View kwargs override ``View._order_by`` / ``_offset`` / ``_limit`` only
  when the caller explicitly passes them.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.models import ORIENT_DICT, ORIENT_RECORDS

# =============================================================================
# Return type by Object type (default orient)
# =============================================================================


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(42, id="int-scalar"),
        pytest.param(3.14, id="float-scalar"),
        pytest.param(True, id="bool-scalar"),
        pytest.param("hello", id="str-scalar"),
    ],
)
async def test_scalar_returns_value(ctx, value):
    """Scalar Object.data() returns the bare Python value."""
    obj = await create_object_from_value(value)
    assert await obj.data() == value


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
# Dict orient modes — Objects and Views
# =============================================================================


@pytest.mark.parametrize(
    "value, select, kwargs, expected",
    [
        # Default orient is ORIENT_DICT — dict of lists.
        pytest.param(
            {"x": [1, 2], "y": [3, 4]}, lambda obj: obj, {}, {"x": [1, 2], "y": [3, 4]}, id="object-default-orient"
        ),
        pytest.param(
            {"a": [10, 20], "b": [30, 40]},
            lambda obj: obj,
            {"orient": ORIENT_DICT},
            {"a": [10, 20], "b": [30, 40]},
            id="object-orient-dict",
        ),
        # ORIENT_RECORDS — list of dicts.
        pytest.param(
            {"a": [10, 20], "b": [30, 40]},
            lambda obj: obj,
            {"orient": ORIENT_RECORDS},
            [{"a": 10, "b": 30}, {"a": 20, "b": 40}],
            id="object-orient-records",
        ),
        pytest.param(
            {"x": [1, 2, 3, 4], "y": [10, 20, 30, 40]},
            lambda obj: obj.where("x > 2"),
            {"orient": ORIENT_RECORDS},
            [{"x": 3, "y": 30}, {"x": 4, "y": 40}],
            id="view-orient-records",
        ),
        pytest.param(
            {"x": [1, 2, 3], "y": [10, 20, 30]},
            lambda obj: obj.view(limit=2),
            {"orient": ORIENT_DICT},
            {"x": [1, 2], "y": [10, 20]},
            id="view-orient-dict",
        ),
    ],
)
async def test_dict_data_orient(ctx, value, select, kwargs, expected):
    """dict Object / View data() honours ``orient``."""
    obj = await create_object_from_value(value)
    assert await select(obj).data(**kwargs) == expected


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
# Keyword-only kwargs: limit / order_by / offset
# =============================================================================


async def test_data_limit_default_caps_at_1000(ctx):
    obj = await create_object_from_value(list(range(2500)))
    rows = await obj.data()
    assert len(rows) == 1000


async def test_data_limit_none_returns_all(ctx):
    obj = await create_object_from_value(list(range(2500)))
    rows = await obj.data(limit=None)
    assert len(rows) == 2500


async def test_data_order_by_returns_deterministic_rows(ctx):
    obj = await create_object_from_value([3, 1, 2])
    assert await obj.data(order_by="value") == [1, 2, 3]


async def test_data_offset_and_limit(ctx):
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    assert await obj.data(order_by="value", offset=1, limit=2) == [2, 3]


async def test_data_without_order_by_does_not_raise(ctx):
    """Spec: .data() does not raise on missing order_by — limit=1000 is the safety cap."""
    obj = await create_object_from_value([1, 2, 3])
    rows = await obj.data()
    assert sorted(rows) == [1, 2, 3]


async def test_scalar_data_ignores_kwargs(ctx):
    s = await create_object_from_value(42)
    assert await s.data(order_by="value", offset=5, limit=3) == 42


async def test_view_kwargs_override_view_attrs(ctx):
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    v = obj.view(order_by="value", limit=2)
    assert await v.data() == [1, 2]
    assert await v.data(limit=3) == [1, 2, 3]


async def test_view_attrs_used_when_kwargs_absent(ctx):
    obj = await create_object_from_value([3, 1, 2])
    v = obj.view(order_by="value")
    assert await v.data() == [1, 2, 3]
