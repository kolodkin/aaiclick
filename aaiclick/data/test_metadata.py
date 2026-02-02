"""
Tests for Object.metadata() method.

This module tests the metadata() method that returns schema information
including table name, fieldtype, and column details.
"""

from aaiclick import (
    create_object_from_value,
    ObjectMetadata,
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

async def test_view_metadata(ctx):
    """Test that view metadata returns source table metadata."""
    obj = await create_object_from_value({'x': [1, 2, 3], 'y': [4, 5, 6]})

    view = obj['x']
    meta = await view.metadata()

    # View metadata should be same as source (references same table)
    assert meta.table == obj.table
    assert meta.fieldtype == FIELDTYPE_DICT
    assert "x" in meta.columns
    assert "y" in meta.columns


async def test_cloned_view_metadata(ctx):
    """Test metadata after cloning a view."""
    obj = await create_object_from_value({'x': [1, 2, 3], 'y': [4, 5, 6]})

    view = obj['x']
    cloned = await view.clone()
    meta = await cloned.metadata()

    # Cloned object should be an array type
    assert meta.fieldtype == FIELDTYPE_ARRAY
    assert "value" in meta.columns
    assert meta.columns["value"].type == "Int64"


# =============================================================================
# Table Name Tests
# =============================================================================

async def test_metadata_table_name(ctx):
    """Test that metadata includes table name."""
    obj = await create_object_from_value([1, 2, 3])

    meta = await obj.metadata()

    assert meta.table == obj.table
    assert meta.table.startswith("t")  # Snowflake ID table names start with 't'


# =============================================================================
# String Type Tests
# =============================================================================

async def test_metadata_string_array(ctx):
    """Test metadata for string array."""
    obj = await create_object_from_value(["hello", "world"])

    meta = await obj.metadata()

    assert meta.fieldtype == FIELDTYPE_ARRAY
    assert meta.columns["value"].type == "String"


async def test_metadata_string_scalar(ctx):
    """Test metadata for string scalar."""
    obj = await create_object_from_value("hello")

    meta = await obj.metadata()

    assert meta.fieldtype == FIELDTYPE_SCALAR
    assert meta.columns["value"].type == "String"


# =============================================================================
# Float Type Tests
# =============================================================================

async def test_metadata_float_array(ctx):
    """Test metadata for float array."""
    obj = await create_object_from_value([1.1, 2.2, 3.3])

    meta = await obj.metadata()

    assert meta.columns["value"].type == "Float64"


async def test_metadata_float_scalar(ctx):
    """Test metadata for float scalar."""
    obj = await create_object_from_value(3.14)

    meta = await obj.metadata()

    assert meta.columns["value"].type == "Float64"
