"""Tests for the private parsing helper in ``aaiclick.data.view_models``.

The ``*_to_view`` adapters are exercised end-to-end by
``aaiclick/internal_api/test_objects.py``; ``scope_of`` has its own
tests in ``aaiclick/data/test_scope.py``. Only ``_object_name_from_table``
needs dedicated tests for its parsing branches.
"""

import pytest
from pydantic import ValidationError

from .models import FIELDTYPE_ARRAY, FIELDTYPE_DICT, FIELDTYPE_SCALAR, ColumnInfo, Schema
from .view_models import (
    ColumnView,
    SchemaView,
    _object_name_from_table,
    column_info_to_view,
    schema_to_view,
    view_to_schema,
)


def test_object_name_from_table_global():
    assert _object_name_from_table("p_orders") == "orders"


def test_object_name_from_table_job_scoped():
    assert _object_name_from_table("j_12345_staging") == "staging"
    assert _object_name_from_table("j_12345_multi_part_name") == "multi_part_name"


def test_object_name_from_table_temp_falls_back_to_table():
    assert _object_name_from_table("t_9999999999") == "t_9999999999"


def test_schema_view_round_trip_with_fieldtype():
    sv = SchemaView(
        columns=[
            ColumnView(name="title", type="String", fieldtype=FIELDTYPE_SCALAR),
            ColumnView(name="votes", type="Int64", fieldtype=FIELDTYPE_ARRAY),
        ],
        order_by="(title)",
        engine="MergeTree",
        fieldtype=FIELDTYPE_DICT,
    )
    dumped = sv.model_dump_json()
    restored = SchemaView.model_validate_json(dumped)
    assert restored == sv
    assert restored.fieldtype == FIELDTYPE_DICT
    assert restored.columns[0].fieldtype == FIELDTYPE_SCALAR
    assert restored.columns[1].fieldtype == FIELDTYPE_ARRAY


def test_column_view_fieldtype_rejects_invalid():
    with pytest.raises(ValidationError):
        ColumnView(name="x", type="Int64", fieldtype=FIELDTYPE_DICT)


def test_schema_view_fieldtype_rejects_invalid():
    with pytest.raises(ValidationError):
        SchemaView(fieldtype="x")


def test_column_info_to_view_threads_fieldtype():
    info = ColumnInfo(type="Int64", array=1, fieldtype=FIELDTYPE_ARRAY)
    view = column_info_to_view("votes", info)
    assert view.name == "votes"
    assert view.type == "Int64"
    assert view.array_depth == 1
    assert view.fieldtype == FIELDTYPE_ARRAY


def test_schema_to_view_round_trip_with_fieldtypes():
    schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns={
            "title": ColumnInfo(type="String", fieldtype=FIELDTYPE_SCALAR),
            "votes": ColumnInfo(type="Int64", array=1, fieldtype=FIELDTYPE_ARRAY),
        },
        table="t_123",
        order_by="(title)",
        engine="MergeTree",
    )
    view = schema_to_view(schema)
    assert view.fieldtype == FIELDTYPE_DICT
    assert [c.name for c in view.columns] == ["title", "votes"]
    assert view.columns[1].array_depth == 1
    assert view.columns[1].fieldtype == FIELDTYPE_ARRAY


def test_view_to_schema_round_trip():
    view = SchemaView(
        columns=[
            ColumnView(name="x", type="Int64", array_depth=1, fieldtype=FIELDTYPE_ARRAY),
            ColumnView(name="y", type="String", nullable=True, fieldtype=FIELDTYPE_SCALAR),
        ],
        order_by="(x)",
        engine="MergeTree",
        fieldtype=FIELDTYPE_DICT,
    )
    schema = view_to_schema(view, table="t_987")
    assert schema.table == "t_987"
    assert schema.fieldtype == FIELDTYPE_DICT
    assert list(schema.columns) == ["x", "y"]
    assert schema.columns["x"].array == 1
    assert schema.columns["x"].fieldtype == FIELDTYPE_ARRAY
    assert schema.columns["y"].nullable is True


def test_schema_view_schema_round_trip_is_identity():
    original = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={"value": ColumnInfo(type="Float64", fieldtype=FIELDTYPE_ARRAY)},
        table="t_rt",
        order_by=None,
        engine=None,
    )
    restored = view_to_schema(schema_to_view(original), table="t_rt")
    assert restored.fieldtype == original.fieldtype
    assert restored.order_by == original.order_by
    assert restored.engine == original.engine
    assert list(restored.columns) == list(original.columns)
    for k in original.columns:
        assert restored.columns[k].type == original.columns[k].type
        assert restored.columns[k].fieldtype == original.columns[k].fieldtype
