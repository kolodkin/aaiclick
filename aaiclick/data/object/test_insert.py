"""
Tests for insert operations.

Covers type compatibility contracts, parametrized insert across data types,
view inserts with filters/computed columns, and subset column handling.
"""

import pytest

from aaiclick import create_object, create_object_from_value
from aaiclick.data.models import FIELDTYPE_ARRAY, FIELDTYPE_DICT, ColumnInfo, Computed, Schema
from aaiclick.data.object.ingest import _are_types_castable, _are_types_compatible

THRESHOLD = 1e-5


# =============================================================================
# _are_types_compatible (strict - for UNION ALL without CAST)
# =============================================================================


@pytest.mark.parametrize(
    "left, right, expected",
    [
        # Same types are always compatible.
        pytest.param("Int64", "Int64", True, id="same-int"),
        pytest.param("Float64", "Float64", True, id="same-float"),
        pytest.param("String", "String", True, id="same-string"),
        # Different integer widths are compatible (ClickHouse UNION ALL allows this).
        pytest.param("Int64", "Int32", True, id="int64-int32"),
        pytest.param("Int32", "Int64", True, id="int32-int64"),
        pytest.param("UInt8", "Int64", True, id="uint8-int64"),
        # Different float widths are compatible.
        pytest.param("Float64", "Float32", True, id="float64-float32"),
        pytest.param("Float32", "Float64", True, id="float32-float64"),
        # Int and Float are NOT compatible for UNION ALL without CAST.
        pytest.param("Int64", "Float64", False, id="int64-float64"),
        pytest.param("Float64", "Int64", False, id="float64-int64"),
        pytest.param("Int32", "Float32", False, id="int32-float32"),
        pytest.param("Float32", "UInt64", False, id="float32-uint64"),
        # Numeric and String types are never compatible.
        pytest.param("Int64", "String", False, id="int64-string"),
        pytest.param("String", "Int64", False, id="string-int64"),
        pytest.param("Float64", "String", False, id="float64-string"),
        pytest.param("String", "Float64", False, id="string-float64"),
        # String and FixedString are different types.
        pytest.param("String", "FixedString", False, id="string-fixedstring"),
        pytest.param("FixedString", "String", False, id="fixedstring-string"),
    ],
)
def test_are_types_compatible(left, right, expected):
    """Pure function: the returned compatibility verdict is the contract."""
    assert _are_types_compatible(left, right) is expected


# =============================================================================
# _are_types_castable (permissive - for INSERT with explicit CAST)
# =============================================================================


@pytest.mark.parametrize(
    "left, right, expected",
    [
        # Same types are always castable.
        pytest.param("Int64", "Int64", True, id="same-int"),
        pytest.param("Float64", "Float64", True, id="same-float"),
        pytest.param("String", "String", True, id="same-string"),
        # Int to Float is castable (explicit CAST works).
        pytest.param("Float64", "Int64", True, id="float64-int64"),
        pytest.param("Float32", "Int32", True, id="float32-int32"),
        # Float to Int is castable (explicit CAST truncates).
        pytest.param("Int64", "Float64", True, id="int64-float64"),
        pytest.param("Int32", "Float32", True, id="int32-float32"),
        # Different integer widths are castable.
        pytest.param("Int64", "Int32", True, id="int64-int32"),
        pytest.param("UInt8", "Int64", True, id="uint8-int64"),
        # Numeric to String is not castable.
        pytest.param("Int64", "String", False, id="int64-string"),
        pytest.param("String", "Int64", False, id="string-int64"),
        pytest.param("Float64", "String", False, id="float64-string"),
    ],
)
def test_are_types_castable(left, right, expected):
    """Pure function: the returned castability verdict is the contract."""
    assert _are_types_castable(left, right) is expected


# =============================================================================
# Basic Array Insert Tests
# =============================================================================


@pytest.mark.parametrize(
    "array_a,array_b,expected_result",
    [
        pytest.param([1, 2, 3], [4, 5, 6], [1, 2, 3, 4, 5, 6], id="int"),
        pytest.param([1.5, 2.5], [3.5, 4.5], [1.5, 2.5, 3.5, 4.5], id="float"),
        pytest.param(["hello", "world"], ["foo", "bar"], ["hello", "world", "foo", "bar"], id="str"),
        # Target rows stay first even when the source holds smaller values.
        pytest.param([4, 5, 6], [1, 2, 3], [4, 5, 6, 1, 2, 3], id="reversed"),
    ],
)
async def test_array_insert(ctx, array_a, array_b, expected_result):
    """Inserting arrays of the same type in place keeps the target rows first, then the source rows."""
    obj_a = await create_object_from_value(array_a, aai_id=True)
    obj_b = await create_object_from_value(array_b, aai_id=True)

    await obj_a.insert(obj_b)
    data = await obj_a.data()

    assert data == pytest.approx(expected_result, abs=THRESHOLD)


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
    obj = await create_object_from_value(array, aai_id=True)

    await obj.insert(scalar_value)
    data = await obj.data()

    assert data == pytest.approx(expected_result, abs=THRESHOLD)


# =============================================================================
# Insert with List Value Tests
# =============================================================================


@pytest.mark.parametrize(
    "array,list_value,expected_result",
    [
        pytest.param([1, 2, 3], [4, 5, 6], [1, 2, 3, 4, 5, 6], id="int"),
        pytest.param([1.5, 2.5], [3.5, 4.5], [1.5, 2.5, 3.5, 4.5], id="float"),
        pytest.param(["hello"], ["world", "test"], ["hello", "world", "test"], id="str"),
        # Inserting an empty list leaves the array unchanged
        pytest.param([1, 2, 3], [], [1, 2, 3], id="empty-list"),
    ],
)
async def test_array_insert_with_list_value(ctx, array, list_value, expected_result):
    """Test inserting list value into array in place."""
    obj = await create_object_from_value(array, aai_id=True)

    await obj.insert(list_value)
    data = await obj.data()

    assert data == pytest.approx(expected_result, abs=THRESHOLD)


# =============================================================================
# Insert Modifies In Place Tests
# =============================================================================


async def test_insert_modifies_in_place(ctx):
    """Test that insert modifies the original object in place."""
    obj = await create_object_from_value([1, 2, 3], aai_id=True)
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
    obj = await create_object_from_value([1, 2], aai_id=True)

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
    scalar_obj = await create_object_from_value(scalar_value, aai_id=True)
    array_obj = await create_object_from_value(array_value, aai_id=True)

    with pytest.raises(ValueError, match="insert requires target table to have array fieldtype"):
        await scalar_obj.insert(array_obj)


# =============================================================================
# Multi-Argument Insert Tests (*args)
# =============================================================================


async def test_array_insert_multiple_objects(ctx):
    """Test inserting multiple objects with *args."""
    obj_a = await create_object_from_value([1, 2], aai_id=True)
    obj_b = await create_object_from_value([3, 4], aai_id=True)
    obj_c = await create_object_from_value([5, 6], aai_id=True)

    await obj_a.insert(obj_b, obj_c)
    data = await obj_a.data()

    assert data == [1, 2, 3, 4, 5, 6]


async def test_array_insert_mixed_types(ctx):
    """Test inserting with mixed argument types (objects, scalars, lists)."""
    obj = await create_object_from_value([1, 2], aai_id=True)

    await obj.insert(3, 4, [5, 6])
    data = await obj.data()

    assert data == [1, 2, 3, 4, 5, 6]


async def test_array_insert_many_arguments(ctx):
    """Test inserting many objects (4+) to verify variadic support."""
    obj_a = await create_object_from_value([1, 2], aai_id=True)
    others = [await create_object_from_value([v], aai_id=True) for v in [3, 4, 5, 6]]

    await obj_a.insert(*others)
    data = await obj_a.data()

    assert data == [1, 2, 3, 4, 5, 6]


# =============================================================================
# View Insert Tests (with_columns, subset columns, constraints)
# =============================================================================


async def test_insert_view_with_where(ctx):
    """Insert a WHERE-filtered view into a target object."""
    src = await create_object_from_value([10, 20, 30, 40, 50], aai_id=True)
    target = await create_object_from_value([1, 2], aai_id=True)

    await target.insert(src.where("value > 25"))
    data = await target.data()

    assert sorted(data) == [1, 2, 30, 40, 50]


async def test_insert_view_with_limit(ctx):
    """Insert a LIMIT-constrained view."""
    src = await create_object_from_value([10, 20, 30], aai_id=True)
    target = await create_object_from_value([1], aai_id=True)

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
        },
        aai_id=True,
    )
    target = await create_object_from_value([1, 2], aai_id=True)

    await target.insert(src["x"])
    data = await target.data()

    assert sorted(data) == [1, 2, 10, 20, 30]


async def test_insert_view_with_computed_columns(ctx):
    """Insert a view with computed columns into a wider target."""
    src = await create_object_from_value(
        {
            "name": ["alice", "bob"],
        },
        aai_id=True,
    )
    schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns={
            "name": ColumnInfo("String", fieldtype=FIELDTYPE_ARRAY),
            "active": ColumnInfo("UInt8", fieldtype=FIELDTYPE_ARRAY),
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
        },
        aai_id=True,
    )
    schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns={
            "id": ColumnInfo("String", fieldtype=FIELDTYPE_ARRAY),
            "val1": ColumnInfo("Int64", nullable=True, fieldtype=FIELDTYPE_ARRAY),
            "val2": ColumnInfo("String", nullable=True, fieldtype=FIELDTYPE_ARRAY),
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
        },
        aai_id=True,
    )
    schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns={
            "id": ColumnInfo("String", fieldtype=FIELDTYPE_ARRAY),
            "count": ColumnInfo("Int64", fieldtype=FIELDTYPE_ARRAY),
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
        },
        aai_id=True,
    )
    schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns={
            "id": ColumnInfo("String", fieldtype=FIELDTYPE_ARRAY),
        },
    )
    target = await create_object(schema)

    await target.insert(src)
    data = await target.data()
    assert data["id"] == ["A"]


async def test_insert_view_with_offset(ctx):
    """Insert a view with OFFSET."""
    src = await create_object_from_value([10, 20, 30], aai_id=True)
    target = await create_object_from_value([1, 2], aai_id=True)

    await target.insert(src.view(offset=1))
    data = await target.data()

    assert sorted(data) == [1, 2, 20, 30]


async def test_insert_view_with_order_by(ctx):
    """Insert a view with ORDER BY + LIMIT picks specific rows."""
    src = await create_object_from_value([30, 10, 20], aai_id=True)
    target = await create_object_from_value([100], aai_id=True)

    await target.insert(src.view(order_by="value ASC", limit=2))
    data = await target.data()

    assert sorted(data) == [10, 20, 100]


async def test_insert_view_chained_where(ctx):
    """Insert a view with chained WHERE conditions."""
    src = await create_object_from_value([5, 10, 15, 20, 25], aai_id=True)
    target = await create_object_from_value([1], aai_id=True)

    await target.insert(src.where("value > 5").where("value < 25"))
    data = await target.data()

    assert sorted(data) == [1, 10, 15, 20]


# =============================================================================
# Dot-notation column names (nested-dict ingest)
# =============================================================================


async def test_insert_nested_dot_column(ctx):
    """insert() must quote dotted column names produced by nested-dict ingest."""
    target = await create_object_from_value([{"a": 1, "m": {"x": 10}}])
    source = await create_object_from_value([{"a": 2, "m": {"x": 20}}])

    await target.insert(source)

    assert await target.data() == {"a": [1, 2], "m": [{"x": 10}, {"x": 20}]}


async def test_insert_dot_star_column(ctx):
    """insert() must quote dot-star column names produced by list-of-dicts ingest."""
    target = await create_object_from_value([{"a": 1, "b": [{"x": 10}]}])
    source = await create_object_from_value([{"a": 2, "b": [{"x": 20}]}])

    await target.insert(source)

    assert await target.data() == {"a": [1, 2], "b": [[{"x": 10}], [{"x": 20}]]}


# =============================================================================
# Subset insert from a schema-created source
# =============================================================================


async def test_insert_skips_extra_source_columns_of_schema_created_source(ctx):
    """insert() silently skips source columns not present in target."""
    src_schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns={
            "shared": ColumnInfo("Int32", fieldtype=FIELDTYPE_ARRAY),
            "extra_col": ColumnInfo("String", fieldtype=FIELDTYPE_ARRAY),
        },
    )
    src = await create_object(src_schema)
    ch = src.ch_client
    await ch.command(f"INSERT INTO {src.table} (shared, extra_col) VALUES (99, 'ignored')")

    tgt_schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns={
            "shared": ColumnInfo("Int32", fieldtype=FIELDTYPE_ARRAY),
        },
    )
    tgt = await create_object(tgt_schema)
    await tgt.insert(src)

    data = await tgt.data()
    assert data["shared"] == [99]


# =============================================================================
# Insert Tests with Mixed Types
# =============================================================================


@pytest.mark.parametrize(
    "target, source, expected",
    [
        # Float values get truncated when cast to int.
        pytest.param([1, 2, 3], [4.5, 5.5, 6.5], [1, 2, 3, 4, 5, 6], id="float-into-int"),
        # Int values get converted to float.
        pytest.param([1.5, 2.5, 3.5], [4, 5, 6], [1.5, 2.5, 3.5, 4.0, 5.0, 6.0], id="int-into-float"),
    ],
)
async def test_mixed_numeric_insert_succeeds(ctx, target, source, expected):
    """Inserting a numeric array of the other kind succeeds (ClickHouse allows casting)."""
    a = await create_object_from_value(target, aai_id=True)
    b = await create_object_from_value(source, aai_id=True)

    await a.insert(b)
    data = await a.data()

    assert data == expected


async def test_mixed_int_string_insert_fails(ctx):
    """Test that inserting string array into int array fails with type error."""
    a = await create_object_from_value([1, 2, 3], aai_id=True)
    b = await create_object_from_value(["a", "b", "c"], aai_id=True)

    with pytest.raises(ValueError, match="types are incompatible"):
        await a.insert(b)


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param(4.5, [1, 2, 3, 4], id="float-value"),
        pytest.param([4.5, 5.5], [1, 2, 3, 4, 5], id="float-list"),
    ],
)
async def test_mixed_insert_float_into_int_succeeds(ctx, value, expected):
    """Inserting a Python float value or list into an int array truncates when cast to int."""
    a = await create_object_from_value([1, 2, 3], aai_id=True)

    await a.insert(value)
    data = await a.data()

    assert data == expected


# =============================================================================
# Repeated source
# =============================================================================


async def test_insert_same_source_twice_preserves_all_rows(ctx):
    """Inserting the same source twice produces the full row set."""
    obj_a = await create_object_from_value([1, 2])
    obj_b = await create_object_from_value([3, 4])

    await obj_a.insert(obj_b)
    await obj_a.insert(obj_b)

    data = await obj_a.data()
    assert sorted(data) == [1, 2, 3, 3, 4, 4]
