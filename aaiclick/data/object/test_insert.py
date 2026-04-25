"""
Tests for insert operations.

Covers type compatibility checking internals, parametrized insert across data types,
view inserts with filters/computed columns, and subset column handling.
"""

import pytest

from aaiclick import create_object, create_object_from_value
from aaiclick.data.data_context import get_ch_client
from aaiclick.data.models import FIELDTYPE_ARRAY, FIELDTYPE_DICT, FIELDTYPE_SCALAR, ColumnInfo, Computed, Schema
from aaiclick.data.object.ingest import _are_types_castable, _are_types_compatible, _get_table_schema

THRESHOLD = 1e-5


# =============================================================================
# _are_types_compatible (strict - for UNION ALL without CAST)
# =============================================================================


def test_compatible_same_type():
    """Same types are always compatible."""
    assert _are_types_compatible("Int64", "Int64")
    assert _are_types_compatible("Float64", "Float64")
    assert _are_types_compatible("String", "String")


def test_compatible_int_to_int():
    """Different integer types are compatible (ClickHouse UNION ALL allows this)."""
    assert _are_types_compatible("Int64", "Int32")
    assert _are_types_compatible("Int32", "Int64")
    assert _are_types_compatible("UInt8", "Int64")


def test_compatible_float_to_float():
    """Different float types are compatible."""
    assert _are_types_compatible("Float64", "Float32")
    assert _are_types_compatible("Float32", "Float64")


def test_incompatible_int_to_float():
    """Int and Float are NOT compatible for UNION ALL without CAST."""
    assert not _are_types_compatible("Int64", "Float64")
    assert not _are_types_compatible("Float64", "Int64")
    assert not _are_types_compatible("Int32", "Float32")
    assert not _are_types_compatible("Float32", "UInt64")


def test_incompatible_numeric_to_string():
    """Numeric and String types are never compatible."""
    assert not _are_types_compatible("Int64", "String")
    assert not _are_types_compatible("String", "Int64")
    assert not _are_types_compatible("Float64", "String")
    assert not _are_types_compatible("String", "Float64")


def test_incompatible_string_types():
    """String and FixedString are not compatible (different types)."""
    assert not _are_types_compatible("String", "FixedString")
    assert not _are_types_compatible("FixedString", "String")


# =============================================================================
# _are_types_castable (permissive - for INSERT with explicit CAST)
# =============================================================================


def test_castable_same_type():
    """Same types are always castable."""
    assert _are_types_castable("Int64", "Int64")
    assert _are_types_castable("Float64", "Float64")
    assert _are_types_castable("String", "String")


def test_castable_int_to_float():
    """Int to Float is castable (explicit CAST works)."""
    assert _are_types_castable("Float64", "Int64")
    assert _are_types_castable("Float32", "Int32")


def test_castable_float_to_int():
    """Float to Int is castable (explicit CAST truncates)."""
    assert _are_types_castable("Int64", "Float64")
    assert _are_types_castable("Int32", "Float32")


def test_castable_int_to_int():
    """Different integer types are castable."""
    assert _are_types_castable("Int64", "Int32")
    assert _are_types_castable("UInt8", "Int64")


def test_not_castable_numeric_to_string():
    """Numeric to String is not castable."""
    assert not _are_types_castable("Int64", "String")
    assert not _are_types_castable("String", "Int64")
    assert not _are_types_castable("Float64", "String")


# =============================================================================
# _get_table_schema — fieldtype recovery for ARRAY, DICT, SCALAR objects
# =============================================================================


async def test_get_table_schema_array_object(ctx):
    """ARRAY Objects (single-value list) round-trip as FIELDTYPE_ARRAY."""
    obj = await create_object_from_value([1, 2, 3])
    fieldtype, columns = await _get_table_schema(obj.table, get_ch_client())
    assert fieldtype == FIELDTYPE_ARRAY


async def test_get_table_schema_dict_object(ctx):
    """DICT Objects (multi-column) round-trip as FIELDTYPE_DICT."""
    obj = await create_object_from_value({"x": [1, 2], "y": [3, 4]})
    fieldtype, columns = await _get_table_schema(obj.table, get_ch_client())
    assert fieldtype == FIELDTYPE_DICT


async def test_get_table_schema_scalar_object(ctx):
    """SCALAR Objects round-trip as FIELDTYPE_SCALAR."""
    obj = await create_object_from_value(42)
    fieldtype, columns = await _get_table_schema(obj.table, get_ch_client())
    assert fieldtype == FIELDTYPE_SCALAR


async def test_get_table_schema_dict_columns_preserved(ctx):
    """Column names are preserved correctly for DICT objects."""
    obj = await create_object_from_value({"a": [1, 2], "b": ["x", "y"]})
    fieldtype, columns = await _get_table_schema(obj.table, get_ch_client())
    assert fieldtype == FIELDTYPE_DICT
    assert "a" in columns
    assert "b" in columns
    assert "aai_id" not in columns


# =============================================================================
# Basic Array Insert Tests
# =============================================================================


@pytest.mark.parametrize(
    "array_a,array_b,expected_result",
    [
        pytest.param([1, 2, 3], [4, 5, 6], [1, 2, 3, 4, 5, 6], id="int"),
        pytest.param([1.5, 2.5], [3.5, 4.5], [1.5, 2.5, 3.5, 4.5], id="float"),
        pytest.param(["hello", "world"], ["foo", "bar"], ["hello", "world", "foo", "bar"], id="str"),
    ],
)
async def test_array_insert(ctx, array_a, array_b, expected_result):
    """Test inserting arrays of the same type in place."""
    obj_a = await create_object_from_value(array_a)
    obj_b = await create_object_from_value(array_b)

    await obj_a.insert(obj_b)
    data = await obj_a.data()

    if isinstance(expected_result[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert data == expected_result


# =============================================================================
# Insert with Scalar Value Tests
# =============================================================================


@pytest.mark.parametrize(
    "array,scalar_value,expected_result",
    [
        pytest.param([1, 2, 3], 42, [1, 2, 3, 42], id="int"),
        pytest.param([1.5, 2.5], 3.5, [1.5, 2.5, 3.5], id="float"),
        pytest.param(["hello", "world"], "test", ["hello", "world", "test"], id="str"),
    ],
)
async def test_array_insert_with_scalar_value(ctx, array, scalar_value, expected_result):
    """Test inserting scalar value into array in place."""
    obj = await create_object_from_value(array)

    await obj.insert(scalar_value)
    data = await obj.data()

    if isinstance(expected_result[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert data == expected_result


# =============================================================================
# Insert with List Value Tests
# =============================================================================


@pytest.mark.parametrize(
    "array,list_value,expected_result",
    [
        pytest.param([1, 2, 3], [4, 5, 6], [1, 2, 3, 4, 5, 6], id="int"),
        pytest.param([1.5, 2.5], [3.5, 4.5], [1.5, 2.5, 3.5, 4.5], id="float"),
        pytest.param(["hello"], ["world", "test"], ["hello", "world", "test"], id="str"),
    ],
)
async def test_array_insert_with_list_value(ctx, array, list_value, expected_result):
    """Test inserting list value into array in place."""
    obj = await create_object_from_value(array)

    await obj.insert(list_value)
    data = await obj.data()

    if isinstance(expected_result[0], float):
        for i, val in enumerate(data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert data == expected_result


# =============================================================================
# Insert with Empty List Tests
# =============================================================================


async def test_array_insert_with_empty_list(ctx):
    """Test inserting empty list into array (should remain unchanged)."""
    obj = await create_object_from_value([1, 2, 3])

    await obj.insert([])
    data = await obj.data()

    assert data == [1, 2, 3]


# =============================================================================
# Insert Modifies In Place Tests
# =============================================================================


async def test_insert_modifies_in_place(ctx):
    """Test that insert modifies the original object in place."""
    obj = await create_object_from_value([1, 2, 3])
    original_table = obj.table

    await obj.insert([4, 5])
    data = await obj.data()

    assert data == [1, 2, 3, 4, 5]
    assert obj.table == original_table


# =============================================================================
# Multiple Inserts Tests
# =============================================================================


async def test_multiple_inserts(ctx):
    """Test multiple consecutive inserts."""
    obj = await create_object_from_value([1, 2])

    await obj.insert([3, 4])
    await obj.insert(5)
    await obj.insert([6])

    data = await obj.data()
    assert data == [1, 2, 3, 4, 5, 6]


# =============================================================================
# Scalar Insert Failure Tests
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
async def test_scalar_insert_fails(ctx, scalar_value, array_value):
    """Test that insert method on scalar fails."""
    scalar_obj = await create_object_from_value(scalar_value)
    array_obj = await create_object_from_value(array_value)

    with pytest.raises(ValueError, match="insert requires target table to have array fieldtype"):
        await scalar_obj.insert(array_obj)


# =============================================================================
# Data Integrity Tests
# =============================================================================


@pytest.mark.parametrize(
    "array_a,array_b",
    [
        pytest.param([1, 2, 3], [4, 5, 6], id="int"),
        pytest.param([5.5, 6.6], [7.7, 8.8], id="float"),
        pytest.param(["a", "b"], ["c", "d"], id="str"),
    ],
)
async def test_insert_preserves_data_integrity(ctx, array_a, array_b):
    """Test that insert preserves all data from both arrays."""
    obj_a = await create_object_from_value(array_a)
    obj_b = await create_object_from_value(array_b)

    await obj_a.insert(obj_b)
    data = await obj_a.data()

    assert len(data) == len(array_a) + len(array_b)

    if isinstance(array_a[0], (int, float)):
        expected_sum = sum(array_a) + sum(array_b)
        actual_sum = sum(data)
        if isinstance(expected_sum, float):
            assert abs(actual_sum - expected_sum) < THRESHOLD
        else:
            assert actual_sum == expected_sum


# =============================================================================
# Multi-Argument Insert Tests (*args)
# =============================================================================


async def test_array_insert_multiple_objects(ctx):
    """Test inserting multiple objects with *args."""
    obj_a = await create_object_from_value([1, 2])
    obj_b = await create_object_from_value([3, 4])
    obj_c = await create_object_from_value([5, 6])

    await obj_a.insert(obj_b, obj_c)
    data = await obj_a.data()

    assert data == [1, 2, 3, 4, 5, 6]


async def test_array_insert_mixed_types(ctx):
    """Test inserting with mixed argument types (objects, scalars, lists)."""
    obj = await create_object_from_value([1, 2])

    await obj.insert(3, 4, [5, 6])
    data = await obj.data()

    assert data == [1, 2, 3, 4, 5, 6]


async def test_array_insert_many_arguments(ctx):
    """Test inserting many objects (4+) to verify variadic support."""
    obj_a = await create_object_from_value([1, 2])
    others = [await create_object_from_value([v]) for v in [3, 4, 5, 6]]

    await obj_a.insert(*others)
    data = await obj_a.data()

    assert data == [1, 2, 3, 4, 5, 6]


# =============================================================================
# View Insert Tests (with_columns, subset columns, constraints)
# =============================================================================


async def test_insert_view_with_where(ctx):
    """Insert a WHERE-filtered view into a target object."""
    src = await create_object_from_value([10, 20, 30, 40, 50])
    target = await create_object_from_value([1, 2])

    await target.insert(src.where("value > 25"))
    data = await target.data()

    assert sorted(data) == [1, 2, 30, 40, 50]


async def test_insert_view_with_limit(ctx):
    """Insert a LIMIT-constrained view."""
    src = await create_object_from_value([10, 20, 30])
    target = await create_object_from_value([1])

    await target.insert(src.view(limit=2))
    data = await target.data()

    assert len(data) == 3
    assert 1 in data


async def test_insert_view_field_selection(ctx):
    """Insert a single-field view from a dict Object."""
    src = await create_object_from_value(
        {
            "x": [10, 20, 30],
            "y": [100, 200, 300],
        }
    )
    target = await create_object_from_value([1, 2])

    await target.insert(src["x"])
    data = await target.data()

    assert sorted(data) == [1, 2, 10, 20, 30]


async def test_insert_view_with_computed_columns(ctx):
    """Insert a view with computed columns into a wider target."""
    src = await create_object_from_value(
        {
            "name": ["alice", "bob"],
        }
    )
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "name": ColumnInfo("String"),
            "active": ColumnInfo("UInt8"),
        },
    )
    target = await create_object(schema)

    view = src.with_columns({"active": Computed("UInt8", "1")})
    await target.insert(view)

    data = await target.data()
    assert data["name"] == ["alice", "bob"]
    assert data["active"] == [1, 1]


async def test_insert_subset_columns_nullable_fill(ctx):
    """Insert source with fewer columns; missing nullable columns fill NULL."""
    src = await create_object_from_value(
        {
            "id": ["A", "B"],
            "val1": [10, 20],
        }
    )
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "id": ColumnInfo("String"),
            "val1": ColumnInfo("Int64", nullable=True),
            "val2": ColumnInfo("String", nullable=True),
        },
    )
    target = await create_object(schema)

    await target.insert(src)
    data = await target.data()

    assert data["id"] == ["A", "B"]
    assert data["val1"] == [10, 20]
    assert data["val2"] == [None, None]


async def test_insert_subset_non_nullable_gets_default(ctx):
    """Insert with missing non-nullable column uses ClickHouse default."""
    src = await create_object_from_value(
        {
            "id": ["A", "B"],
        }
    )
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "id": ColumnInfo("String"),
            "count": ColumnInfo("Int64"),
        },
    )
    target = await create_object(schema)

    await target.insert(src)
    data = await target.data()

    assert data["id"] == ["A", "B"]
    assert data["count"] == [0, 0]


async def test_insert_skips_extra_source_columns(ctx):
    """Insert with extra source columns silently skips them."""
    src = await create_object_from_value(
        {
            "id": ["A"],
            "extra": [999],
        }
    )
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "id": ColumnInfo("String"),
        },
    )
    target = await create_object(schema)

    await target.insert(src)
    data = await target.data()
    assert data["id"] == ["A"]


async def test_insert_view_with_offset(ctx):
    """Insert a view with OFFSET."""
    src = await create_object_from_value([10, 20, 30])
    target = await create_object_from_value([1, 2])

    await target.insert(src.view(offset=1))
    data = await target.data()

    assert sorted(data) == [1, 2, 20, 30]


async def test_insert_view_with_order_by(ctx):
    """Insert a view with ORDER BY + LIMIT picks specific rows."""
    src = await create_object_from_value([30, 10, 20])
    target = await create_object_from_value([100])

    await target.insert(src.view(order_by="value ASC", limit=2))
    data = await target.data()

    assert sorted(data) == [10, 20, 100]


async def test_insert_view_chained_where(ctx):
    """Insert a view with chained WHERE conditions."""
    src = await create_object_from_value([5, 10, 15, 20, 25])
    target = await create_object_from_value([1])

    await target.insert(src.where("value > 5").where("value < 25"))
    data = await target.data()

    assert sorted(data) == [1, 10, 15, 20]
