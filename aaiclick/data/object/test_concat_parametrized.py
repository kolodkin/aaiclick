"""
Parametrized tests for concat operations across different data types.

Tests array concatenation with objects, scalar values, and list values.
"""

import pytest

from aaiclick import create_object_from_value
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
    """Test concatenating arrays of the same type."""
    obj_a = await create_object_from_value(array_a)
    obj_b = await create_object_from_value(array_b)

    result = await obj_a.concat(obj_b)
    data = await result.data()

    if isinstance(expected_result[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert data == expected_result


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

    if isinstance(expected_result[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert data == expected_result


# =============================================================================
# Concat with List Value Tests
# =============================================================================


@pytest.mark.parametrize(
    "array,list_value,expected_result",
    [
        pytest.param([1, 2, 3], [4, 5, 6], [1, 2, 3, 4, 5, 6], id="int"),
        pytest.param([1.5, 2.5], [3.5, 4.5], [1.5, 2.5, 3.5, 4.5], id="float"),
        pytest.param(["hello"], ["world", "test"], ["hello", "world", "test"], id="str"),
    ],
)
async def test_array_concat_with_list_value(ctx, array, list_value, expected_result):
    """Test concatenating array with list value."""
    obj = await create_object_from_value(array)

    result = await obj.concat(list_value)
    data = await result.data()

    if isinstance(expected_result[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert data == expected_result


# =============================================================================
# Concat with Empty List Tests
# =============================================================================


async def test_array_concat_with_empty_list(ctx):
    """Test concatenating array with empty list (should return same data)."""
    obj = await create_object_from_value([1, 2, 3])

    result = await obj.concat([])
    data = await result.data()

    assert data == [1, 2, 3]


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
# Order Preservation Tests
# =============================================================================


@pytest.mark.parametrize(
    "array_a,array_b",
    [
        pytest.param([1, 2, 3], [4, 5, 6], id="int"),
        pytest.param([5.5, 6.6], [7.7, 8.8], id="float"),
        pytest.param(["a", "b"], ["c", "d"], id="str"),
    ],
)
async def test_concat_preserves_data_integrity(ctx, array_a, array_b):
    """Test that concat preserves all data from both arrays."""
    obj_a = await create_object_from_value(array_a)
    obj_b = await create_object_from_value(array_b)

    result = await obj_a.concat(obj_b)
    data = await result.data()

    assert len(data) == len(array_a) + len(array_b)

    if isinstance(array_a[0], (int, float)):
        expected_sum = sum(array_a) + sum(array_b)
        actual_sum = sum(data)
        if isinstance(expected_sum, float):
            assert abs(actual_sum - expected_sum) < THRESHOLD
        else:
            assert actual_sum == expected_sum


# =============================================================================
# Multi-Argument Concat Tests (*args)
# =============================================================================


async def test_array_concat_multiple_objects(ctx):
    """Test concatenating multiple objects with *args."""
    obj_a = await create_object_from_value([1, 2])
    obj_b = await create_object_from_value([3, 4])
    obj_c = await create_object_from_value([5, 6])

    result = await obj_a.concat(obj_b, obj_c)
    data = await result.data()

    assert data == [1, 2, 3, 4, 5, 6]


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
    obj_b = await create_object_from_value({
        "x": [10, 20],
        "y": [100, 200],
    })

    result = await obj_a.concat(obj_b["x"])
    data = await result.data()

    assert sorted(data) == [1, 2, 10, 20]


async def test_concat_view_with_computed_columns(ctx):
    """Concat a view with computed columns."""
    obj_a = await create_object_from_value({
        "name": ["alice"],
        "active": [1],
    })
    obj_b = await create_object_from_value({
        "name": ["bob", "carol"],
    })

    result = await obj_a.concat(obj_b.with_columns({
        "active": Computed("UInt8", "1"),
    }))
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
