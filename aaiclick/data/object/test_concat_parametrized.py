"""
Parametrized tests for concat operations across different data types.

Tests array concatenation with objects, scalar values, and list values.
"""

import pytest

from aaiclick import create_object_from_value, delete_persistent_object
from aaiclick.data.models import Computed

THRESHOLD = 1e-5


# =============================================================================
# Basic Array Concat Tests
# =============================================================================


@pytest.mark.parametrize(
    "array_a,array_b,expected_result",
    [
        pytest.param([1, 2, 3], [4, 5, 6], [1, 2, 3, 4, 5, 6], id="int"),
        pytest.param([1.5, 2.5], [3.5, 4.5, 5.5], [1.5, 2.5, 3.5, 4.5, 5.5], id="float"),
        pytest.param(["hello", "world"], ["foo", "bar", "baz"], ["hello", "world", "foo", "bar", "baz"], id="str"),
    ],
)
async def test_array_concat(ctx, array_a, array_b, expected_result):
    """Test concatenating arrays of the same type preserves every row from both sides."""
    obj_a = await create_object_from_value(array_a)
    obj_b = await create_object_from_value(array_b)

    result = await obj_a.concat(obj_b)
    data = await result.data()

    assert data == pytest.approx(expected_result, abs=THRESHOLD)


# =============================================================================
# Concat with Scalar Value Tests
# =============================================================================


@pytest.mark.parametrize(
    "array,scalar_value,expected_result",
    [
        pytest.param([1, 2, 3], 42, [1, 2, 3, 42], id="int"),
        pytest.param([1.5, 2.5], 3.5, [1.5, 2.5, 3.5], id="float"),
        pytest.param(["hello", "world"], "test", ["hello", "world", "test"], id="str"),
    ],
)
async def test_array_concat_with_scalar_value(ctx, array, scalar_value, expected_result):
    """Test concatenating array with scalar value."""
    obj = await create_object_from_value(array)

    result = await obj.concat(scalar_value)
    data = await result.data()

    assert data == pytest.approx(expected_result, abs=THRESHOLD)


# =============================================================================
# Concat with List Value Tests
# =============================================================================


@pytest.mark.parametrize(
    "array,list_value,expected_result",
    [
        pytest.param([1, 2, 3], [4, 5, 6], [1, 2, 3, 4, 5, 6], id="int"),
        pytest.param([1.5, 2.5], [3.5, 4.5], [1.5, 2.5, 3.5, 4.5], id="float"),
        pytest.param(["hello"], ["world", "test"], ["hello", "world", "test"], id="str"),
        # Concatenating an empty list returns the same data
        pytest.param([1, 2, 3], [], [1, 2, 3], id="empty-list"),
    ],
)
async def test_array_concat_with_list_value(ctx, array, list_value, expected_result):
    """Test concatenating array with list value: self first, then the value."""
    obj = await create_object_from_value(array)

    result = await obj.concat(list_value)
    data = await result.data()

    assert data == pytest.approx(expected_result, abs=THRESHOLD)


# =============================================================================
# Scalar Concat Failure Tests
# =============================================================================


@pytest.mark.parametrize(
    "scalar_value,array_value",
    [
        pytest.param(42, [1, 2, 3], id="int"),
        pytest.param(3.14, [1.0, 2.0], id="float"),
        pytest.param(True, [1, 2, 3], id="bool"),
        pytest.param("hello", ["a", "b"], id="str"),
    ],
)
async def test_scalar_concat_fails(ctx, scalar_value, array_value):
    """Test that concat method on scalar fails."""
    scalar_obj = await create_object_from_value(scalar_value)
    array_obj = await create_object_from_value(array_value)

    with pytest.raises(ValueError, match="concat requires first source to have array fieldtype"):
        await scalar_obj.concat(array_obj)


# =============================================================================
# Multi-Argument Concat Tests (*args)
# =============================================================================


async def test_array_concat_mixed_types(ctx):
    """Test concatenating with mixed argument types (objects, scalars, lists)."""
    obj = await create_object_from_value([1, 2])

    result = await obj.concat(3, 4, [5, 6])
    data = await result.data()

    assert data == [1, 2, 3, 4, 5, 6]


async def test_array_concat_many_arguments(ctx):
    """Test concatenating many objects (4+) to verify variadic support."""
    obj_a = await create_object_from_value([1, 2])
    others = [await create_object_from_value([v]) for v in [3, 4, 5, 6]]

    result = await obj_a.concat(*others)
    data = await result.data()

    assert data == [1, 2, 3, 4, 5, 6]


# =============================================================================
# View Concat Tests
# =============================================================================


async def test_concat_view_with_where(ctx):
    """Concat a WHERE-filtered view."""
    obj_a = await create_object_from_value([1, 2, 3])
    obj_b = await create_object_from_value([10, 20, 30, 40])

    result = await obj_a.concat(obj_b.where("value > 25"))
    data = await result.data()

    assert sorted(data) == [1, 2, 3, 30, 40]


async def test_concat_view_with_limit(ctx):
    """Concat a LIMIT-constrained view."""
    obj_a = await create_object_from_value([1, 2])
    obj_b = await create_object_from_value([10, 20, 30])

    result = await obj_a.concat(obj_b.view(limit=2))
    data = await result.data()

    assert len(data) == 4
    assert 1 in data and 2 in data


async def test_concat_view_field_selection(ctx):
    """Concat a single-field view from a dict Object."""
    obj_a = await create_object_from_value([1, 2])
    obj_b = await create_object_from_value(
        {
            "x": [10, 20],
            "y": [100, 200],
        }
    )

    result = await obj_a.concat(obj_b["x"])
    data = await result.data()

    assert sorted(data) == [1, 2, 10, 20]


async def test_concat_view_with_computed_columns(ctx):
    """Concat a view with computed columns."""
    obj_a = await create_object_from_value(
        {
            "name": ["alice"],
            "active": [1],
        }
    )
    obj_b = await create_object_from_value(
        {
            "name": ["bob", "carol"],
        }
    )

    result = await obj_a.concat(
        obj_b.with_columns(
            {
                "active": Computed("UInt8", "1"),
            }
        )
    )
    data = await result.data()

    assert sorted(data["name"]) == ["alice", "bob", "carol"]
    assert data["active"] == [1, 1, 1]


async def test_concat_view_with_offset(ctx):
    """Concat a view with OFFSET."""
    obj_a = await create_object_from_value([1, 2])
    obj_b = await create_object_from_value([10, 20, 30])

    result = await obj_a.concat(obj_b.view(offset=1))
    data = await result.data()

    assert sorted(data) == [1, 2, 20, 30]


async def test_concat_view_with_order_by(ctx):
    """Concat a view with ORDER BY + LIMIT picks specific rows."""
    obj_a = await create_object_from_value([100])
    obj_b = await create_object_from_value([30, 10, 20])

    result = await obj_a.concat(obj_b.view(order_by="value ASC", limit=2))
    data = await result.data()

    assert sorted(data) == [10, 20, 100]


async def test_concat_view_chained_where(ctx):
    """Concat a view with chained WHERE conditions."""
    obj_a = await create_object_from_value([1])
    obj_b = await create_object_from_value([5, 10, 15, 20, 25])

    result = await obj_a.concat(obj_b.where("value > 5").where("value < 25"))
    data = await result.data()

    assert sorted(data) == [1, 10, 15, 20]


async def test_concat_preserves_dict_fieldtype(ctx):
    """concat of two DICT Objects stays DICT — the result table holds the
    user-named columns and ``data()`` returns a dict-of-arrays. Regression
    test for a bug where concat hardcoded ``fieldtype=ARRAY``, leaving the
    result schema inconsistent (claimed ARRAY but no ``"value"`` column),
    so any subsequent ``data()`` / ``markdown()`` failed with
    ``Unknown identifier value``.
    """
    a = await create_object_from_value({"id": ["A1"], "score": [10.0]})
    b = await create_object_from_value({"id": ["B1", "B2"], "score": [20.0, 30.0]})

    result = await a.concat(b)
    assert result.schema.fieldtype == "d"
    data = await result.data()
    assert isinstance(data, dict)
    assert sorted(data["id"]) == ["A1", "B1", "B2"]
    assert sorted(data["score"]) == [10.0, 20.0, 30.0]


async def test_concat_with_name_global_scope(ctx):
    """concat(name=..., scope='global') routes through the named-table path."""
    a = await create_object_from_value([1, 2])
    b = await create_object_from_value([3, 4])

    result = await a.concat(b, name="concat_named_global", scope="global")
    try:
        assert result.table == "p_concat_named_global"
        assert await result.data() == [1, 2, 3, 4]
    finally:
        await delete_persistent_object("concat_named_global", scope="global")


async def test_concat_with_name_temp_scope(ctx):
    """concat(name=...) defaults to temp_named scope (t_<name>_<id>)."""
    a = await create_object_from_value([1, 2])
    b = await create_object_from_value([3, 4])

    result = await a.concat(b, name="concat_named_temp")

    assert result.table.startswith("t_concat_named_temp_")
    assert await result.data() == [1, 2, 3, 4]


# =============================================================================
# Dot-notation column names (nested-dict ingest)
# =============================================================================


async def test_concat_nested_dot_column(ctx):
    """concat() builds its own CAST list — dotted names must be quoted there too."""
    left = await create_object_from_value([{"a": 1, "m": {"x": 10}}])
    right = await create_object_from_value([{"a": 2, "m": {"x": 20}}])

    result = await left.concat(right)

    assert await result.data() == {"a": [1, 2], "m": [{"x": 10}, {"x": 20}]}


# =============================================================================
# Concat Tests with Mixed Types
# =============================================================================


@pytest.mark.parametrize(
    "arr_a,arr_b",
    [
        pytest.param([1, 2, 3], [4.5, 5.5, 6.5], id="int-float"),
        pytest.param([1.5, 2.5, 3.5], [4, 5, 6], id="float-int"),
        pytest.param([1, 2, 3], ["a", "b", "c"], id="int-str"),
    ],
)
async def test_mixed_type_concat_fails(ctx, arr_a, arr_b):
    """Test that concatenating incompatible types fails with type error."""
    a = await create_object_from_value(arr_a, aai_id=True)
    b = await create_object_from_value(arr_b, aai_id=True)

    with pytest.raises(ValueError, match="incompatible type"):
        await a.concat(b)


# =============================================================================
# Argument order: self first, then args left-to-right
# =============================================================================


async def test_concat_follows_argument_order(ctx):
    """Concat puts self first even when self was created after the argument."""
    obj_a = await create_object_from_value([1, 2, 3])
    obj_b = await create_object_from_value([4, 5, 6])

    result = await obj_b.concat(obj_a)
    data = await result.data()
    assert data == [4, 5, 6, 1, 2, 3]


async def test_multiple_concat_preserves_argument_order(ctx):
    """Chained concat preserves argument order at each step."""
    obj1 = await create_object_from_value([1, 2])
    obj2 = await create_object_from_value([3, 4])
    obj3 = await create_object_from_value([5, 6])

    result = await obj1.concat(obj2)
    result = await result.concat(obj3)
    data = await result.data()
    assert data == [1, 2, 3, 4, 5, 6]


async def test_concat_multi_arg_order(ctx):
    """Multi-arg concat: self, then each arg in order, regardless of creation order."""
    obj1 = await create_object_from_value([1, 2])
    obj2 = await create_object_from_value([3, 4])
    obj3 = await create_object_from_value([5, 6])

    result = await obj3.concat(obj1, obj2)
    data = await result.data()
    assert data == [5, 6, 1, 2, 3, 4]


async def test_concat_same_source_twice_preserves_all_rows(ctx):
    """Concatenating the same source twice produces the full row set."""
    obj = await create_object_from_value([1, 2])

    result = await obj.concat(obj)
    data = await result.data()
    assert sorted(data) == [1, 1, 2, 2]
