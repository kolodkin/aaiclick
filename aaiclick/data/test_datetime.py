"""
Tests for UTC datetime type support in object creation.

Tests datetime values across scalar, array, dict, and records formats,
verifying automatic type inference in create_object_from_value and
explicit schema usage in create_object.
"""

from datetime import datetime, timezone

import pytest

from aaiclick import (
    ColumnInfo,
    Schema,
    create_object,
    create_object_from_value,
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    FIELDTYPE_SCALAR,
    get_ch_client,
)


DT_2024 = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
DT_2025 = datetime(2025, 6, 20, 14, 45, 30, tzinfo=timezone.utc)
DT_EPOCH = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
DT_MILLIS = datetime(2024, 3, 10, 8, 15, 30, 123000, tzinfo=timezone.utc)


# =============================================================================
# Scalar Creation Tests
# =============================================================================


async def test_scalar_datetime(ctx):
    """Datetime scalar is auto-detected and round-trips correctly."""
    obj = await create_object_from_value(DT_2024)
    data = await obj.data()
    assert data == DT_2024


async def test_scalar_datetime_with_millis(ctx):
    """Datetime with millisecond precision is preserved."""
    obj = await create_object_from_value(DT_MILLIS)
    data = await obj.data()
    assert data == DT_MILLIS


async def test_scalar_datetime_epoch(ctx):
    """Unix epoch datetime round-trips correctly."""
    obj = await create_object_from_value(DT_EPOCH)
    data = await obj.data()
    assert data == DT_EPOCH


# =============================================================================
# Array Creation Tests
# =============================================================================


async def test_array_datetime(ctx):
    """List of datetimes is auto-detected and preserves order."""
    values = [DT_2024, DT_2025, DT_EPOCH]
    obj = await create_object_from_value(values)
    data = await obj.data()
    assert data == values


async def test_array_datetime_with_millis(ctx):
    """List of datetimes with millisecond precision is preserved."""
    values = [DT_MILLIS, DT_2024]
    obj = await create_object_from_value(values)
    data = await obj.data()
    assert data == values


async def test_array_datetime_single(ctx):
    """Single-element datetime list works."""
    values = [DT_2024]
    obj = await create_object_from_value(values)
    data = await obj.data()
    assert data == values


# =============================================================================
# Dict Creation Tests
# =============================================================================


async def test_dict_of_datetime_arrays(ctx):
    """Dict with datetime array columns is auto-detected."""
    val = {
        "event_time": [DT_2024, DT_2025],
        "label": ["start", "end"],
    }
    obj = await create_object_from_value(val)
    data = await obj.data()
    assert data["event_time"] == [DT_2024, DT_2025]
    assert data["label"] == ["start", "end"]


async def test_dict_of_datetime_scalars(ctx):
    """Dict with datetime scalar values is auto-detected."""
    val = {
        "created_at": DT_2024,
        "name": "test",
    }
    obj = await create_object_from_value(val)
    data = await obj.data()
    assert data["created_at"] == DT_2024
    assert data["name"] == "test"


# =============================================================================
# Records Creation Tests
# =============================================================================


async def test_records_with_datetime(ctx):
    """List of dicts with datetime fields is auto-detected."""
    val = [
        {"ts": DT_2024, "value": 10},
        {"ts": DT_2025, "value": 20},
    ]
    obj = await create_object_from_value(val)
    data = await obj.data(orient="records")
    assert data[0]["ts"] == DT_2024
    assert data[1]["ts"] == DT_2025
    assert data[0]["value"] == 10
    assert data[1]["value"] == 20


# =============================================================================
# Explicit Schema Tests (create_object with DateTime64 type)
# =============================================================================


async def test_explicit_datetime_schema(ctx):
    """Explicit DateTime64 schema works with create_object + insert."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "value": ColumnInfo("DateTime64(3, 'UTC')"),
        },
    )
    obj = await create_object(schema)
    ch = get_ch_client()
    data = [[DT_2024], [DT_2025]]
    await ch.insert(obj.table, data, column_names=["value"])

    result = await obj.data()
    assert result == [DT_2024, DT_2025]


async def test_explicit_datetime_dict_schema(ctx):
    """Explicit DateTime64 in dict schema works with create_object + insert."""
    schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "event_time": ColumnInfo("DateTime64(3, 'UTC')"),
            "count": ColumnInfo("Int64"),
        },
        col_fieldtype=FIELDTYPE_ARRAY,
    )
    obj = await create_object(schema)
    ch = get_ch_client()
    data = [[DT_2024, 100], [DT_2025, 200]]
    await ch.insert(obj.table, data, column_names=["event_time", "count"])

    result = await obj.data()
    assert result["event_time"] == [DT_2024, DT_2025]
    assert result["count"] == [100, 200]
