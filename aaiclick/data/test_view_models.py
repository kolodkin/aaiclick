"""Tests for data view models and Object adapters."""

from datetime import datetime

from .models import ColumnInfo, Schema
from .object.object import Object
from .view_models import (
    ColumnView,
    ObjectDetail,
    ObjectView,
    SchemaView,
    _object_name_from_table,
    column_info_to_view,
    object_to_detail,
    object_to_view,
    schema_to_view,
)


def _schema(table: str) -> Schema:
    return Schema(
        fieldtype="s",
        col_fieldtype="s",
        columns={
            "aai_id": ColumnInfo(type="Int64"),
            "name": ColumnInfo(type="String", nullable=True, low_cardinality=True),
            "tags": ColumnInfo(type="String", array=1),
        },
        table=table,
        engine="MergeTree",
        order_by="(aai_id)",
    )


def test_column_info_to_view_plain():
    view = column_info_to_view("id", ColumnInfo(type="Int64"))
    assert view == ColumnView(name="id", type="Int64", nullable=False, array_depth=0, low_cardinality=False)


def test_column_info_to_view_nested_array():
    view = column_info_to_view("matrix", ColumnInfo(type="Int64", array=2))
    assert view.array_depth == 2


def test_column_info_to_view_low_cardinality_nullable():
    view = column_info_to_view(
        "label",
        ColumnInfo(type="String", nullable=True, low_cardinality=True),
    )
    assert view.nullable is True
    assert view.low_cardinality is True


def test_schema_to_view_preserves_columns_and_metadata():
    schema = _schema("p_orders")
    view = schema_to_view(schema)
    assert isinstance(view, SchemaView)
    names = [c.name for c in view.columns]
    assert names == ["aai_id", "name", "tags"]
    tags_col = next(c for c in view.columns if c.name == "tags")
    assert tags_col.array_depth == 1
    assert view.engine == "MergeTree"
    assert view.order_by == "(aai_id)"


def test_object_name_from_table_global():
    assert _object_name_from_table("p_orders") == "orders"


def test_object_name_from_table_job_scoped():
    assert _object_name_from_table("j_12345_staging") == "staging"
    assert _object_name_from_table("j_12345_multi_part_name") == "multi_part_name"


def test_object_name_from_table_temp_falls_back_to_table():
    assert _object_name_from_table("t_9999999999") == "t_9999999999"


def test_object_to_view_global():
    obj = Object(table="p_orders", schema=_schema("p_orders"))
    view = object_to_view(obj, row_count=42, size_bytes=1024)
    assert isinstance(view, ObjectView)
    assert view.name == "orders"
    assert view.table == "p_orders"
    assert view.scope == "global"
    assert view.persistent is True
    assert view.row_count == 42
    assert view.size_bytes == 1024
    assert view.created_at is None


def test_object_to_view_job_scoped():
    obj = Object(table="j_111_staging", schema=_schema("j_111_staging"))
    view = object_to_view(obj)
    assert view.scope == "job"
    assert view.persistent is True
    assert view.name == "staging"


def test_object_to_view_temp():
    obj = Object(table="t_42", schema=_schema("t_42"))
    view = object_to_view(obj)
    assert view.scope == "temp"
    assert view.persistent is False
    assert view.name == "t_42"


def test_object_to_view_json_serialization_keys():
    obj = Object(table="p_orders", schema=_schema("p_orders"))
    payload = object_to_view(obj, row_count=1).model_dump(mode="json")
    assert payload["name"] == "orders"
    assert payload["scope"] == "global"
    assert payload["persistent"] is True
    assert payload["row_count"] == 1


def test_object_to_detail_embeds_schema():
    obj = Object(table="p_orders", schema=_schema("p_orders"))
    created = datetime(2025, 1, 1, 12, 0, 0)
    detail = object_to_detail(
        obj,
        row_count=10,
        size_bytes=2048,
        created_at=created,
        lineage_summary="derived from `raw_events`",
    )
    assert isinstance(detail, ObjectDetail)
    assert detail.name == "orders"
    assert detail.row_count == 10
    assert detail.size_bytes == 2048
    assert detail.created_at == created
    assert detail.lineage_summary == "derived from `raw_events`"
    # ObjectDetail is-a ObjectView
    assert detail.table_schema.engine == "MergeTree"
    assert [c.name for c in detail.table_schema.columns] == ["aai_id", "name", "tags"]
