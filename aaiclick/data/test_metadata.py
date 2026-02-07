"""
Tests for Object.metadata() method.

This module tests the metadata() method that returns schema information
including table name, fieldtype, and column details.
"""

import pytest

from aaiclick import (
    create_object_from_value,
    ObjectMetadata,
    ViewMetadata,
    ColumnInfo,
    FIELDTYPE_SCALAR,
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
)


# =============================================================================
# Basic Metadata Tests
# =============================================================================

async def test_metadata_array(ctx):
    """Test metadata for array object."""
    obj = await create_object_from_value([1, 2, 3])

    meta = await obj.metadata()

    assert isinstance(meta, ObjectMetadata)
    assert meta.fieldtype == FIELDTYPE_ARRAY
    assert "aai_id" in meta.columns
    assert "value" in meta.columns
    assert meta.columns["aai_id"].type == "UInt64"
    assert meta.columns["value"].type == "Int64"


async def test_metadata_scalar(ctx):
    """Test metadata for scalar object."""
    obj = await create_object_from_value(42)

    meta = await obj.metadata()

    assert meta.fieldtype == FIELDTYPE_SCALAR
    assert meta.columns["value"].fieldtype == FIELDTYPE_SCALAR


async def test_metadata_dict(ctx):
    """Test metadata for dict object."""
    obj = await create_object_from_value({'param1': [1, 2, 3], 'param2': [4, 5, 6]})

    meta = await obj.metadata()

    assert meta.fieldtype == FIELDTYPE_DICT
    assert "aai_id" in meta.columns
    assert "param1" in meta.columns
    assert "param2" in meta.columns
    assert meta.columns["param1"].type == "Int64"
    assert meta.columns["param2"].type == "Int64"


async def test_metadata_dict_mixed_types(ctx):
    """Test metadata for dict with mixed column types."""
    obj = await create_object_from_value({
        'ints': [1, 2, 3],
        'floats': [1.5, 2.5, 3.5],
        'strings': ['a', 'b', 'c']
    })

    meta = await obj.metadata()

    assert meta.fieldtype == FIELDTYPE_DICT
    assert meta.columns["ints"].type == "Int64"
    assert meta.columns["floats"].type == "Float64"
    assert meta.columns["strings"].type == "String"


# =============================================================================
# ColumnInfo Tests
# =============================================================================

async def test_column_info_structure(ctx):
    """Test that ColumnInfo has expected structure."""
    obj = await create_object_from_value([1.5, 2.5, 3.5])

    meta = await obj.metadata()
    value_col = meta.columns["value"]

    assert isinstance(value_col, ColumnInfo)
    assert value_col.name == "value"
    assert value_col.type == "Float64"
    assert value_col.fieldtype == FIELDTYPE_ARRAY


async def test_column_info_aai_id(ctx):
    """Test that aai_id column has scalar fieldtype."""
    obj = await create_object_from_value([1, 2, 3])

    meta = await obj.metadata()
    aai_id_col = meta.columns["aai_id"]

    assert aai_id_col.name == "aai_id"
    assert aai_id_col.type == "UInt64"
    assert aai_id_col.fieldtype == FIELDTYPE_SCALAR


# =============================================================================
# View Metadata Tests
# =============================================================================

async def test_view_metadata_returns_view_metadata(ctx):
    """Test that view.metadata() returns ViewMetadata type."""
    obj = await create_object_from_value({'x': [1, 2, 3], 'y': [4, 5, 6]})

    view = obj['x']
    meta = await view.metadata()

    assert isinstance(meta, ViewMetadata)
    assert meta.table == obj.table
    assert meta.fieldtype == FIELDTYPE_DICT
    assert "x" in meta.columns
    assert "y" in meta.columns


async def test_view_metadata_selected_fields(ctx):
    """Test that selected_fields is included in ViewMetadata."""
    obj = await create_object_from_value({'param1': [1, 2, 3], 'param2': [4, 5, 6]})

    view = obj['param1']
    meta = await view.metadata()

    assert meta.selected_fields == ['param1']
    assert meta.where is None
    assert meta.limit is None
    assert meta.offset is None
    assert meta.order_by is None


async def test_view_metadata_where_clause(ctx):
    """Test that where clause is included in ViewMetadata."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])

    view = obj.view(where="value > 2")
    meta = await view.metadata()

    assert isinstance(meta, ViewMetadata)
    assert meta.where == "value > 2"
    assert meta.limit is None


async def test_view_metadata_limit_offset(ctx):
    """Test that limit and offset are included in ViewMetadata."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])

    view = obj.view(limit=3, offset=1)
    meta = await view.metadata()

    assert meta.limit == 3
    assert meta.offset == 1
    assert meta.where is None


async def test_view_metadata_order_by(ctx):
    """Test that order_by is included in ViewMetadata."""
    obj = await create_object_from_value([5, 3, 1, 4, 2])

    view = obj.view(order_by="value DESC")
    meta = await view.metadata()

    assert meta.order_by == "value DESC"


async def test_view_metadata_all_constraints(ctx):
    """Test ViewMetadata with all constraints."""
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    view = obj.view(where="value > 2", limit=5, offset=1, order_by="value DESC")
    meta = await view.metadata()

    assert meta.where == "value > 2"
    assert meta.limit == 5
    assert meta.offset == 1
    assert meta.order_by == "value DESC"
    assert meta.selected_fields is None


async def test_object_returns_object_metadata(ctx):
    """Test that Object.metadata() returns ObjectMetadata (not ViewMetadata)."""
    obj = await create_object_from_value([1, 2, 3])

    meta = await obj.metadata()

    assert isinstance(meta, ObjectMetadata)
    assert not isinstance(meta, ViewMetadata)


async def test_copied_view_metadata(ctx):
    """Test metadata after copying a view."""
    obj = await create_object_from_value({'x': [1, 2, 3], 'y': [4, 5, 6]})

    view = obj['x']
    cloned = await view.copy()
    meta = await cloned.metadata()

    # Cloned object should be an array type (ObjectMetadata, not ViewMetadata)
    assert isinstance(meta, ObjectMetadata)
    assert meta.fieldtype == FIELDTYPE_ARRAY
    assert "value" in meta.columns
    assert meta.columns["value"].type == "Int64"


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
async def test_metadata_value_types(ctx, value, expected_fieldtype, expected_type):
    """Test metadata correctly reports fieldtype and column type for various value types."""
    obj = await create_object_from_value(value)

    meta = await obj.metadata()

    assert meta.fieldtype == expected_fieldtype
    assert meta.columns["value"].type == expected_type
