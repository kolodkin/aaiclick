"""
Tests for custom order_by support on Object creation.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data import ENGINE_MERGE_TREE, ColumnInfo, Object, Schema
from aaiclick.data.data_context import data_context, get_ch_client
from aaiclick.data.models import FIELDTYPE_ARRAY, FIELDTYPE_SCALAR, build_order_by_clause


def test_build_order_by_clause_empty_returns_tuple():
    """Empty input yields tuple() — no implicit key."""
    assert build_order_by_clause([]) == "tuple()"


def test_build_order_by_clause_single_column():
    """build_order_by_clause preserves a single user column verbatim."""
    assert build_order_by_clause(["date"]) == "(date)"


def test_build_order_by_clause_multiple_columns():
    """build_order_by_clause preserves multiple user columns verbatim."""
    assert build_order_by_clause(["date", "category"]) == "(date, category)"


def test_build_order_by_clause_does_not_append_aai_id():
    """build_order_by_clause never injects aai_id — the column does not exist."""
    assert "aai_id" not in build_order_by_clause(["date"])
    assert "aai_id" not in build_order_by_clause([])


def test_object_init_order_by_sets_schema():
    """Object.__init__ with order_by sets schema.order_by."""
    schema = Schema(
        fieldtype="d",
        columns={"date": ColumnInfo("String"), "category": ColumnInfo("String")},
    )
    obj = Object(schema=schema, order_by=["date"])
    assert obj._schema.order_by == "(date)"


def test_object_init_no_order_by():
    """Object.__init__ without order_by leaves schema.order_by as None."""
    schema = Schema(
        fieldtype="d",
        columns={"date": ColumnInfo("String"), "category": ColumnInfo("String")},
    )
    obj = Object(schema=schema)
    assert obj._schema.order_by is None


async def test_order_by_warns_on_memory(ctx):
    """order_by on a Memory-default context emits a UserWarning."""
    with pytest.warns(UserWarning, match="order_by=.*has no effect with Memory engine"):
        await create_object_from_value(
            {"date": ["2024-01-03", "2024-01-01", "2024-01-02"], "val": [30, 10, 20]},
            order_by=["date"],
        )


async def test_order_by_mergetree():
    """order_by with MergeTree engine sets correct sorting_key."""
    async with data_context(engine=ENGINE_MERGE_TREE):
        ch = get_ch_client()
        obj = await create_object_from_value(
            {"date": ["2024-01-03", "2024-01-01", "2024-01-02"], "val": [30, 10, 20]},
            order_by=["date"],
        )

        result = await ch.query(f"SELECT engine, sorting_key FROM system.tables WHERE name = '{obj.table}'")
        assert result.result_rows[0][0] == "MergeTree"
        assert result.result_rows[0][1] == "date"


async def test_order_by_multi_column_mergetree():
    """Multiple order_by columns with MergeTree."""
    async with data_context(engine=ENGINE_MERGE_TREE):
        ch = get_ch_client()
        obj = await create_object_from_value(
            {"category": ["b", "a", "a"], "date": ["2024-01-01", "2024-01-02", "2024-01-01"], "val": [1, 2, 3]},
            order_by=["category", "date"],
        )

        result = await ch.query(f"SELECT sorting_key FROM system.tables WHERE name = '{obj.table}'")
        assert result.result_rows[0][0] == "category, date"


async def test_no_order_by_stays_memory(ctx):
    """Without order_by the default Memory engine is used."""
    ch = get_ch_client()
    obj = await create_object_from_value(
        {"date": ["2024-01-01", "2024-01-02"], "val": [10, 20]},
    )

    result = await ch.query(f"SELECT engine FROM system.tables WHERE name = '{obj.table}'")
    assert result.result_rows[0][0] == "Memory"


def test_column_info_carries_fieldtype():
    ci = ColumnInfo(type="Int64", nullable=False, array=0, fieldtype=FIELDTYPE_ARRAY)
    assert ci.fieldtype == FIELDTYPE_ARRAY


def test_column_info_fieldtype_defaults_to_scalar():
    ci = ColumnInfo(type="String")
    assert ci.fieldtype == FIELDTYPE_SCALAR
