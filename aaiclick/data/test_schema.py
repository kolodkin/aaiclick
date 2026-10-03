"""
Tests for Object.schema and View.schema properties.

This module tests the schema property that returns schema information
including table name, fieldtype, and column details.
"""

import pytest

from aaiclick import (
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    FIELDTYPE_SCALAR,
    ColumnInfo,
    ObjectNotFoundError,
    Schema,
    create_object,
    create_object_from_value,
    delete_persistent_object,
    open_object,
)
from aaiclick.data import Computed
from aaiclick.data.data_context import get_ch_client
from aaiclick.data.data_context.lifecycle import get_data_lifecycle
from aaiclick.data.models import ViewSchema

# =============================================================================
# Basic Schema Tests
# =============================================================================


async def test_schema_array(ctx):
    """Test schema for array object."""
    obj = await create_object_from_value([1, 2, 3])

    schema = obj.schema

    assert isinstance(schema, Schema)
    assert schema.fieldtype == FIELDTYPE_ARRAY
    assert list(schema.columns) == ["value"]
    assert schema.columns["value"].type == "Int64"


async def test_schema_dict(ctx):
    """Test schema for dict object."""
    obj = await create_object_from_value({"param1": [1, 2, 3], "param2": [4, 5, 6]})

    schema = obj.schema

    assert schema.fieldtype == FIELDTYPE_DICT
    assert "aai_id" not in schema.columns
    assert "param1" in schema.columns
    assert "param2" in schema.columns
    assert schema.columns["param1"].type == "Int64"
    assert schema.columns["param2"].type == "Int64"


async def test_schema_dict_mixed_types(ctx):
    """Test schema for dict with mixed column types."""
    obj = await create_object_from_value({"ints": [1, 2, 3], "floats": [1.5, 2.5, 3.5], "strings": ["a", "b", "c"]})

    schema = obj.schema

    assert schema.fieldtype == FIELDTYPE_DICT
    assert schema.columns["ints"].type == "Int64"
    assert schema.columns["floats"].type == "Float64"
    assert schema.columns["strings"].type == "String"


# =============================================================================
# ColumnInfo Tests
# =============================================================================


async def test_column_info_structure(ctx):
    """Test that ColumnInfo has expected structure."""
    obj = await create_object_from_value([1.5, 2.5, 3.5])

    value_col = obj.schema.columns["value"]

    assert isinstance(value_col, ColumnInfo)
    assert value_col.type == "Float64"


# =============================================================================
# View Schema Tests
# =============================================================================


async def test_view_schema_returns_view_schema(ctx):
    """Test that view.schema returns ViewSchema type."""
    obj = await create_object_from_value({"x": [1, 2, 3], "y": [4, 5, 6]})

    view = obj["x"]
    schema = view.schema

    assert isinstance(schema, ViewSchema)
    assert schema.table == obj.table
    assert schema.fieldtype == FIELDTYPE_DICT
    assert "x" in schema.columns
    assert "y" in schema.columns


async def test_view_schema_selected_fields(ctx):
    """Test that selected_fields is included in ViewSchema."""
    obj = await create_object_from_value({"param1": [1, 2, 3], "param2": [4, 5, 6]})

    view = obj["param1"]
    schema = view.schema

    assert schema.selected_fields == ["param1"]
    assert schema.where is None
    assert schema.limit is None
    assert schema.offset is None
    assert schema.order_by is None


async def test_view_schema_where_clause(ctx):
    """Test that where clause is included in ViewSchema."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])

    view = obj.view(where="value > 2")
    schema = view.schema

    assert isinstance(schema, ViewSchema)
    assert schema.where == "(value > 2)"
    assert schema.limit is None


async def test_view_schema_all_constraints(ctx):
    """Test ViewSchema with all constraints."""
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    view = obj.view(where="value > 2", limit=5, offset=1, order_by="value DESC")
    schema = view.schema

    assert schema.where == "(value > 2)"
    assert schema.limit == 5
    assert schema.offset == 1
    assert schema.order_by == "value DESC"
    assert schema.selected_fields is None


async def test_object_returns_schema(ctx):
    """Test that Object.schema returns Schema (not ViewSchema)."""
    obj = await create_object_from_value([1, 2, 3])

    schema = obj.schema

    assert isinstance(schema, Schema)
    assert not isinstance(schema, ViewSchema)


async def test_copied_view_schema(ctx):
    """Test schema after copying a view."""
    obj = await create_object_from_value({"x": [1, 2, 3], "y": [4, 5, 6]})

    view = obj["x"]
    cloned = await view.copy()
    schema = cloned.schema

    # Cloned object should be an array type (Schema, not ViewSchema)
    assert isinstance(schema, Schema)
    assert schema.fieldtype == FIELDTYPE_ARRAY
    assert "value" in schema.columns
    assert schema.columns["value"].type == "Int64"


# =============================================================================
# Type Tests (Parametrized)
# =============================================================================


@pytest.mark.parametrize(
    "value,expected_fieldtype,expected_type",
    [
        pytest.param(["hello", "world"], FIELDTYPE_ARRAY, "String", id="string-array"),
        pytest.param("hello", FIELDTYPE_SCALAR, "String", id="string-scalar"),
        pytest.param([1.1, 2.2, 3.3], FIELDTYPE_ARRAY, "Float64", id="float-array"),
        pytest.param(3.14, FIELDTYPE_SCALAR, "Float64", id="float-scalar"),
        pytest.param(42, FIELDTYPE_SCALAR, "Int64", id="int-scalar"),
    ],
)
async def test_schema_value_types(ctx, value, expected_fieldtype, expected_type):
    """Test schema correctly reports fieldtype and column type for various value types."""
    obj = await create_object_from_value(value)

    schema = obj.schema

    assert schema.fieldtype == expected_fieldtype
    assert schema.columns["value"].type == expected_type


# =============================================================================
# Registry-backed schema reads
# =============================================================================


@pytest.mark.parametrize(
    "value, name, expected_fieldtype, expected_columns",
    [
        pytest.param([1, 2, 3], "reopen_array", FIELDTYPE_ARRAY, {"value": FIELDTYPE_ARRAY}, id="array"),
        pytest.param(
            {"a": [1, 2], "b": ["x", "y"]},
            "reopen_dict",
            FIELDTYPE_DICT,
            {"a": FIELDTYPE_ARRAY, "b": FIELDTYPE_ARRAY},
            id="dict",
        ),
        pytest.param(42, "reopen_scalar", FIELDTYPE_SCALAR, {"value": FIELDTYPE_SCALAR}, id="scalar"),
    ],
)
async def test_open_object_reads_schema_from_registry(ctx, value, name, expected_fieldtype, expected_columns):
    """``open_object`` rebuilds the fieldtype and columns from ``table_registry.schema_doc``."""
    await create_object_from_value(value, name=name, scope="global")
    try:
        # Registry writes queue through DBLifecycleHandler; flush so the INSERT commits before the read.
        lifecycle = get_data_lifecycle()
        assert lifecycle is not None
        await lifecycle.flush()

        schema = (await open_object(name, scope="global")).schema

        assert schema.fieldtype == expected_fieldtype
        assert {col: info.fieldtype for col, info in schema.columns.items()} == expected_columns
    finally:
        await delete_persistent_object(name, scope="global")


_SINGLE_FIELD_SHAPES = [
    pytest.param(lambda obj: obj["a"], id="single-field"),
    pytest.param(lambda obj: obj.with_columns({"s": Computed("Int64", "a * 2")})["s"], id="computed+single-field"),
]

# Shapes whose data() is a dict, so its keys are the View's columns.
_DICT_SHAPES = [
    pytest.param(lambda obj: obj[["b", "a"]], id="multi-field"),
    pytest.param(lambda obj: obj.explode("tags"), id="explode"),
    pytest.param(
        lambda obj: obj.rename({"a": "x"}).with_columns({"s": Computed("Int64", "x + b")}).where("b > 3"),
        id="chained",
    ),
    # A field selection made after with_columns() drops the computed columns it omits...
    pytest.param(
        lambda obj: obj.with_columns({"s": Computed("Int64", "a + b")})[["a", "b"]], id="computed+multi-field"
    ),
    # ...while computed columns added after the selection join it.
    pytest.param(
        lambda obj: obj[["a", "b"]].with_columns({"s": Computed("Int64", "a + b")}), id="multi-field+computed"
    ),
]


@pytest.mark.parametrize("transform", _SINGLE_FIELD_SHAPES + _DICT_SHAPES)
async def test_view_materialized_schema_matches_copy(ctx, transform):
    """``View.materialized_schema`` is the plain ``Schema`` of the table ``copy()`` produces."""
    obj = await create_object_from_value({"a": [1, 2], "b": [3, 4], "tags": [["x", "y"], ["z"]]})
    view = transform(obj)
    copied = await view.copy()

    schema = view.materialized_schema

    assert type(schema) is Schema
    assert (schema.fieldtype, schema.columns) == (copied.schema.fieldtype, copied.schema.columns)


@pytest.mark.parametrize("transform", _SINGLE_FIELD_SHAPES + _DICT_SHAPES)
async def test_view_effective_columns_match_copy(ctx, transform):
    """The columns operators see on a View are the columns ``copy()`` materializes, ColumnInfo included."""
    obj = await create_object_from_value({"a": [1, 2], "b": [3, 4], "tags": [["x", "y"], ["z"]]})
    view = transform(obj)

    assert view._effective_columns == (await view.copy()).schema.columns


@pytest.mark.parametrize("transform", _DICT_SHAPES)
async def test_view_effective_columns_match_data(ctx, transform):
    """The columns operators see on a View are the keys ``data()`` returns, in order."""
    obj = await create_object_from_value({"a": [1, 2], "b": [3, 4], "tags": [["x", "y"], ["z"]]})
    view = transform(obj)

    assert list(view._effective_columns) == list((await view.data()).keys())


@pytest.mark.parametrize(
    "transform, expected_order_by",
    [
        # Same columns as the source: its sort key still applies.
        pytest.param(lambda obj: obj, "(b)", id="object"),
        pytest.param(lambda obj: obj.where("b > 3"), "(b)", id="where"),
        # Renamed columns: the source's key no longer names a column.
        pytest.param(lambda obj: obj.rename({"b": "x"}), None, id="rename"),
    ],
)
async def test_materialized_schema_keeps_sort_key_while_columns_match(ctx, transform, expected_order_by):
    columns = {"a": ColumnInfo("Int64"), "b": ColumnInfo("Int64")}
    obj = await create_object(Schema(fieldtype=FIELDTYPE_DICT, columns=columns, order_by="(b)", engine="MergeTree"))

    assert transform(obj).materialized_schema.order_by == expected_order_by


async def test_open_object_without_registry_row_raises(orch_ctx):
    """A table aaiclick did not register has no schema to read, so it is not an object."""
    await get_ch_client().command("CREATE TABLE p_orphan (v Int64) ENGINE = Memory")

    with pytest.raises(ObjectNotFoundError, match="does not exist"):
        await open_object("orphan", scope="global")
