"""
Tests for custom order_by support on Object creation.
"""

from aaiclick import create_object_from_value
from aaiclick.data import Object, Schema, ColumnInfo, create_object, ENGINE_MERGE_TREE
from aaiclick.data.data_context import data_context, get_ch_client
from aaiclick.data.models import build_order_by_clause


def test_build_order_by_clause_single_column():
    """build_order_by_clause with a single column appends aai_id."""
    assert build_order_by_clause(["date"]) == "(date, aai_id)"


def test_build_order_by_clause_multiple_columns():
    """build_order_by_clause with multiple columns appends aai_id."""
    assert build_order_by_clause(["date", "category"]) == "(date, category, aai_id)"


def test_build_order_by_clause_dedup_aai_id():
    """build_order_by_clause deduplicates aai_id if already present."""
    assert build_order_by_clause(["date", "aai_id"]) == "(date, aai_id)"


def test_build_order_by_clause_only_aai_id():
    """build_order_by_clause with only aai_id returns just (aai_id)."""
    assert build_order_by_clause(["aai_id"]) == "(aai_id)"


def test_object_init_order_by_sets_schema():
    """Object.__init__ with order_by sets schema.order_by."""
    schema = Schema(
        fieldtype="d",
        columns={"aai_id": ColumnInfo("UInt64"), "date": ColumnInfo("String")},
    )
    obj = Object(schema=schema, order_by=["date"])
    assert obj._schema.order_by == "(date, aai_id)"


def test_object_init_no_order_by():
    """Object.__init__ without order_by leaves schema.order_by as None."""
    schema = Schema(
        fieldtype="d",
        columns={"aai_id": ColumnInfo("UInt64"), "date": ColumnInfo("String")},
    )
    obj = Object(schema=schema)
    assert obj._schema.order_by is None


async def test_create_object_order_by_table():
    """Object created with order_by has correct sorting_key in ClickHouse."""
    async with data_context(engine=ENGINE_MERGE_TREE):
        ch = get_ch_client()
        schema = Schema(
            fieldtype="d",
            columns={
                "aai_id": ColumnInfo("UInt64"),
                "date": ColumnInfo("String"),
                "value": ColumnInfo("Int64"),
            },
            order_by="(date, aai_id)",
        )
        obj = await create_object(schema)

        result = await ch.query(
            f"SELECT sorting_key FROM system.tables WHERE name = '{obj.table}'"
        )
        assert result.result_rows[0][0] == "date, aai_id"


async def test_create_object_from_value_order_by():
    """create_object_from_value with order_by creates table with correct sorting_key."""
    async with data_context(engine=ENGINE_MERGE_TREE):
        ch = get_ch_client()
        obj = await create_object_from_value(
            {"date": ["2024-01-03", "2024-01-01", "2024-01-02"], "val": [30, 10, 20]},
            order_by=["date"],
        )

        result = await ch.query(
            f"SELECT sorting_key FROM system.tables WHERE name = '{obj.table}'"
        )
        assert result.result_rows[0][0] == "date, aai_id"


async def test_create_object_from_value_multi_order_by():
    """create_object_from_value with multiple order_by columns."""
    async with data_context(engine=ENGINE_MERGE_TREE):
        ch = get_ch_client()
        obj = await create_object_from_value(
            {"category": ["b", "a", "a"], "date": ["2024-01-01", "2024-01-02", "2024-01-01"], "val": [1, 2, 3]},
            order_by=["category", "date"],
        )

        result = await ch.query(
            f"SELECT sorting_key FROM system.tables WHERE name = '{obj.table}'"
        )
        assert result.result_rows[0][0] == "category, date, aai_id"


async def test_create_object_from_value_no_order_by():
    """create_object_from_value without order_by uses default tuple()."""
    async with data_context(engine=ENGINE_MERGE_TREE):
        ch = get_ch_client()
        obj = await create_object_from_value(
            {"date": ["2024-01-01", "2024-01-02"], "val": [10, 20]},
        )

        result = await ch.query(
            f"SELECT sorting_key FROM system.tables WHERE name = '{obj.table}'"
        )
        assert result.result_rows[0][0] == ""
