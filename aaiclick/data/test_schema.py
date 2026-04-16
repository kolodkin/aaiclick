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
    Schema,
    create_object_from_value,
)
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
    assert "aai_id" in schema.columns
    assert "value" in schema.columns
    assert schema.columns["aai_id"].type == "UInt64"
    assert schema.columns["value"].type == "Int64"


async def test_schema_scalar(ctx):
    """Test schema for scalar object."""
    obj = await create_object_from_value(42)

    schema = obj.schema

    assert schema.fieldtype == FIELDTYPE_SCALAR
    assert schema.columns["value"].type == "Int64"


async def test_schema_dict(ctx):
    """Test schema for dict object."""
    obj = await create_object_from_value({'param1': [1, 2, 3], 'param2': [4, 5, 6]})

    schema = obj.schema

    assert schema.fieldtype == FIELDTYPE_DICT
    assert "aai_id" in schema.columns
    assert "param1" in schema.columns
    assert "param2" in schema.columns
    assert schema.columns["param1"].type == "Int64"
    assert schema.columns["param2"].type == "Int64"


async def test_schema_dict_mixed_types(ctx):
    """Test schema for dict with mixed column types."""
    obj = await create_object_from_value({
        'ints': [1, 2, 3],
        'floats': [1.5, 2.5, 3.5],
        'strings': ['a', 'b', 'c']
    })

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


async def test_column_info_aai_id(ctx):
    """Test aai_id column type."""
    obj = await create_object_from_value([1, 2, 3])

    aai_id_col = obj.schema.columns["aai_id"]

    assert aai_id_col.type == "UInt64"


# =============================================================================
# View Schema Tests
# =============================================================================

async def test_view_schema_returns_view_schema(ctx):
    """Test that view.schema returns ViewSchema type."""
    obj = await create_object_from_value({'x': [1, 2, 3], 'y': [4, 5, 6]})

    view = obj['x']
    schema = view.schema

    assert isinstance(schema, ViewSchema)
    assert schema.table == obj.table
    assert schema.fieldtype == FIELDTYPE_DICT
    assert "x" in schema.columns
    assert "y" in schema.columns


async def test_view_schema_selected_fields(ctx):
    """Test that selected_fields is included in ViewSchema."""
    obj = await create_object_from_value({'param1': [1, 2, 3], 'param2': [4, 5, 6]})

    view = obj['param1']
    schema = view.schema

    assert schema.selected_fields == ['param1']
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


async def test_view_schema_limit_offset(ctx):
    """Test that limit and offset are included in ViewSchema."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])

    view = obj.view(limit=3, offset=1)
    schema = view.schema

    assert schema.limit == 3
    assert schema.offset == 1
    assert schema.where is None


async def test_view_schema_order_by(ctx):
    """Test that order_by is included in ViewSchema."""
    obj = await create_object_from_value([5, 3, 1, 4, 2])

    view = obj.view(order_by="value DESC")
    schema = view.schema

    assert schema.order_by == "value DESC"


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
    obj = await create_object_from_value({'x': [1, 2, 3], 'y': [4, 5, 6]})

    view = obj['x']
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

@pytest.mark.parametrize("value,expected_fieldtype,expected_type", [
    # String types
    (["hello", "world"], FIELDTYPE_ARRAY, "String"),
    ("hello", FIELDTYPE_SCALAR, "String"),
    # Float types
    ([1.1, 2.2, 3.3], FIELDTYPE_ARRAY, "Float64"),
    (3.14, FIELDTYPE_SCALAR, "Float64"),
    # Int types
    ([1, 2, 3], FIELDTYPE_ARRAY, "Int64"),
    (42, FIELDTYPE_SCALAR, "Int64"),
])
async def test_schema_value_types(ctx, value, expected_fieldtype, expected_type):
    """Test schema correctly reports fieldtype and column type for various value types."""
    obj = await create_object_from_value(value)

    schema = obj.schema

    assert schema.fieldtype == expected_fieldtype
    assert schema.columns["value"].type == expected_type
