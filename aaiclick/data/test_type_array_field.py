"""
Tests for array field support - creating Objects from list-of-dicts (records format)
where fields can contain arrays stored as ClickHouse Array(T) columns.
"""

import pytest

from aaiclick import ORIENT_DICT, ORIENT_RECORDS, create_object_from_value


# =============================================================================
# Creation Tests
# =============================================================================


async def test_records_with_array_and_scalar_fields(ctx):
    """List of dicts where some fields are arrays, some are scalars."""
    obj = await create_object_from_value([
        {"a": [1, 2, 3], "b": 10},
        {"a": [4, 5, 6], "b": 20},
    ])

    data = await obj.data()

    assert isinstance(data, dict)
    assert data["a"] == [[1, 2, 3], [4, 5, 6]]
    assert data["b"] == [10, 20]


async def test_records_with_all_array_fields(ctx):
    """List of dicts where all fields are arrays."""
    obj = await create_object_from_value([
        {"x": [1, 2], "y": [3, 4]},
        {"x": [5, 6], "y": [7, 8]},
    ])

    data = await obj.data()

    assert data["x"] == [[1, 2], [5, 6]]
    assert data["y"] == [[3, 4], [7, 8]]


async def test_records_with_all_scalar_fields(ctx):
    """List of dicts where all fields are scalars (no Array columns)."""
    obj = await create_object_from_value([
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
    ])

    data = await obj.data()

    assert data["name"] == ["Alice", "Bob"]
    assert data["age"] == [30, 25]


async def test_records_single_record(ctx):
    """Single record in the list."""
    obj = await create_object_from_value([
        {"a": [1, 2, 3], "b": 42},
    ])

    data = await obj.data()

    assert data["a"] == [[1, 2, 3]]
    assert data["b"] == [42]


# =============================================================================
# Orient Tests
# =============================================================================


async def test_records_orient_dict(ctx):
    """data() with orient=ORIENT_DICT returns dict of lists."""
    obj = await create_object_from_value([
        {"a": [1, 2], "b": 10},
        {"a": [3, 4], "b": 20},
    ])

    data = await obj.data(orient=ORIENT_DICT)

    assert isinstance(data, dict)
    assert data["a"] == [[1, 2], [3, 4]]
    assert data["b"] == [10, 20]


async def test_records_orient_records(ctx):
    """data() with orient=ORIENT_RECORDS returns list of dicts."""
    obj = await create_object_from_value([
        {"a": [1, 2], "b": 10},
        {"a": [3, 4], "b": 20},
    ])

    data = await obj.data(orient=ORIENT_RECORDS)

    assert isinstance(data, list)
    assert len(data) == 2
    assert data[0] == {"a": [1, 2], "b": 10}
    assert data[1] == {"a": [3, 4], "b": 20}


# =============================================================================
# Variable Length Arrays
# =============================================================================


async def test_records_different_array_lengths(ctx):
    """Different records can have arrays of different lengths."""
    obj = await create_object_from_value([
        {"a": [1, 2, 3], "b": 10},
        {"a": [4, 5], "b": 20},
        {"a": [6], "b": 30},
    ])

    data = await obj.data(orient=ORIENT_RECORDS)

    assert data[0] == {"a": [1, 2, 3], "b": 10}
    assert data[1] == {"a": [4, 5], "b": 20}
    assert data[2] == {"a": [6], "b": 30}


async def test_records_empty_array_field(ctx):
    """Record with an empty list field."""
    obj = await create_object_from_value([
        {"a": [], "b": 10},
        {"a": [1, 2], "b": 20},
    ])

    data = await obj.data(orient=ORIENT_RECORDS)

    assert data[0] == {"a": [], "b": 10}
    assert data[1] == {"a": [1, 2], "b": 20}


# =============================================================================
# Type Inference Tests
# =============================================================================


async def test_records_int_array_field(ctx):
    """Array field with int values creates Array(Int64)."""
    obj = await create_object_from_value([
        {"values": [1, 2, 3]},
    ])

    meta = await obj.metadata()
    assert "Array(Int64)" in meta.columns["values"].type


async def test_records_float_array_field(ctx):
    """Array field with float values creates Array(Float64)."""
    obj = await create_object_from_value([
        {"values": [1.5, 2.5, 3.5]},
    ])

    meta = await obj.metadata()
    assert "Array(Float64)" in meta.columns["values"].type


async def test_records_string_array_field(ctx):
    """Array field with string values creates Array(String)."""
    obj = await create_object_from_value([
        {"tags": ["hello", "world"]},
    ])

    meta = await obj.metadata()
    assert "Array(String)" in meta.columns["tags"].type


# =============================================================================
# Validation Tests
# =============================================================================


async def test_records_inconsistent_keys_raises(ctx):
    """Records with different keys should raise ValueError."""
    with pytest.raises(ValueError, match="identical keys"):
        await create_object_from_value([
            {"a": [1, 2], "b": 10},
            {"a": [3, 4], "c": 20},
        ])


# =============================================================================
# Metadata Tests
# =============================================================================


async def test_records_metadata_fieldtype(ctx):
    """metadata() reports correct fieldtype for records."""
    obj = await create_object_from_value([
        {"a": [1, 2], "b": 10},
        {"a": [3, 4], "b": 20},
    ])

    meta = await obj.metadata()

    assert meta.fieldtype == "d"
    assert "a" in meta.columns
    assert "b" in meta.columns
    assert "Array" in meta.columns["a"].type
    assert meta.columns["b"].type == "Int64"
