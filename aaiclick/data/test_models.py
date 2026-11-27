"""Tests for ``ColumnInfo`` / ``Schema`` JSON round-tripping.

The full ``ObjectDetail`` API surface is exercised by
``aaiclick/server/routers/test_objects.py`` and
``aaiclick/internal_api/test_objects.py``.
"""

import pytest
from pydantic import ValidationError

from .models import FIELDTYPE_ARRAY, FIELDTYPE_DICT, FIELDTYPE_SCALAR, ColumnInfo, QueryStats, Schema


def test_schema_round_trip_with_fieldtype():
    schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns={
            "title": ColumnInfo(type="String", fieldtype=FIELDTYPE_SCALAR),
            "votes": ColumnInfo(type="Int64", array=1, fieldtype=FIELDTYPE_ARRAY),
        },
        order_by="(title)",
        engine="MergeTree",
    )
    restored = Schema.model_validate_json(schema.model_dump_json())
    assert restored == schema
    assert restored.fieldtype == FIELDTYPE_DICT
    assert restored.columns["title"].fieldtype == FIELDTYPE_SCALAR
    assert restored.columns["votes"].fieldtype == FIELDTYPE_ARRAY


def test_column_info_fieldtype_rejects_invalid():
    with pytest.raises(ValidationError):
        ColumnInfo(type="Int64", fieldtype=FIELDTYPE_DICT)


def test_column_info_positional_type():
    """Historical dataclass call sites use positional ``type=``."""
    info = ColumnInfo("Int64", nullable=True)
    assert info.type == "Int64"
    assert info.nullable is True


def test_column_info_with_fieldtype_returns_copy():
    info = ColumnInfo(type="Int64", fieldtype=FIELDTYPE_SCALAR)
    promoted = info.with_fieldtype(FIELDTYPE_ARRAY)
    assert promoted.fieldtype == FIELDTYPE_ARRAY
    assert info.fieldtype == FIELDTYPE_SCALAR  # frozen — original untouched


# req: user_guide/object/query-stats-fields
def test_query_stats_from_http_summary():
    """Pure function: the clickhouse-connect summary mapping is the HTTP column of the field table."""
    summary = {
        "read_rows": "10",
        "read_bytes": "80",
        "elapsed_ns": "2500000000",
        "result_rows": "4",
        "written_rows": "4",
        "written_bytes": "32",
    }
    assert QueryStats.from_clickhouse_summary(summary) == QueryStats(
        read_rows=10, read_bytes=80, elapsed_s=2.5, result_rows=4, written_rows=4, written_bytes=32
    )
