"""Tests for the private parsing helper in ``aaiclick.data.view_models``.

The ``*_to_view`` adapters are exercised end-to-end by
``aaiclick/internal_api/test_objects.py``; ``scope_of`` has its own
tests in ``aaiclick/data/test_scope.py``. Only ``_object_name_from_table``
needs dedicated tests for its parsing branches.
"""

import pytest
from pydantic import ValidationError

from .models import FIELDTYPE_ARRAY, FIELDTYPE_DICT, FIELDTYPE_SCALAR
from .view_models import ColumnView, SchemaView, _object_name_from_table


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
