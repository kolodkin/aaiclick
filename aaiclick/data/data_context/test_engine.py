"""Tests for configurable table engine support."""

from aaiclick import create_object, create_object_from_value
from aaiclick.data.data_context import data_context, get_ch_client
from aaiclick.data.models import (
    AAI_ID_INFO,
    ENGINE_AGGREGATING_MERGE_TREE,
    ENGINE_MEMORY,
    ENGINE_MERGE_TREE,
    ColumnInfo,
    Schema,
)


async def test_context_with_memory_engine():
    """Test non-default engine (Memory) set on data_context."""
    async with data_context(engine=ENGINE_MEMORY):
        ch_client = get_ch_client()
        obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
        obj_b = await create_object_from_value([4, 5, 6], aai_id=True)

        # Verify source tables use Memory engine
        result = await ch_client.query(f"""
            SELECT engine FROM system.tables WHERE name = '{obj_a.table}'
        """)
        assert result.result_rows[0][0] == "Memory"

        # Operator should also create Memory table
        obj_c = await (obj_a + obj_b)

        result = await ch_client.query(f"""
            SELECT engine FROM system.tables WHERE name = '{obj_c.table}'
        """)
        assert result.result_rows[0][0] == "Memory"

        # Verify data is correct
        assert await obj_c.data() == [5, 7, 9]


async def test_create_object_with_engine_override():
    """Test engine override passed directly to create_object."""
    async with data_context():  # Default Memory
        ch_client = get_ch_client()
        # Override to MergeTree
        schema = Schema(fieldtype="a", columns={"value": ColumnInfo("Int64")})
        obj_a = await create_object(schema, engine=ENGINE_MERGE_TREE)

        result = await ch_client.query(f"""
            SELECT engine FROM system.tables WHERE name = '{obj_a.table}'
        """)
        assert result.result_rows[0][0] == "MergeTree"

        # Without override, should use context default (Memory)
        obj_b = await create_object(schema)

        result = await ch_client.query(f"""
            SELECT engine FROM system.tables WHERE name = '{obj_b.table}'
        """)
        assert result.result_rows[0][0] == "Memory"


async def test_mixed_engine_scenario():
    """Test mix of context engine and per-object override."""
    async with data_context(engine=ENGINE_MEMORY):
        ch_client = get_ch_client()
        # Create with context default (Memory)
        obj_a = await create_object_from_value([10, 20, 30], aai_id=True)

        result = await ch_client.query(f"""
            SELECT engine FROM system.tables WHERE name = '{obj_a.table}'
        """)
        assert result.result_rows[0][0] == "Memory"

        # Override to MergeTree for specific object. Add aai_id so the
        # operator below has stable cross-table ordering without needing
        # an explicit view(order_by=...).
        schema = Schema(
            fieldtype="a",
            columns={"value": ColumnInfo("Int64"), "aai_id": AAI_ID_INFO},
        )
        obj_b = await create_object(schema, engine=ENGINE_MERGE_TREE)

        result = await ch_client.query(f"""
            SELECT engine FROM system.tables WHERE name = '{obj_b.table}'
        """)
        assert result.result_rows[0][0] == "MergeTree"

        # Insert data into obj_b — aai_id is auto-filled by DEFAULT generateSnowflakeID().
        await ch_client.insert(obj_b.table, [[100], [200], [300]], column_names=["value"], column_type_names=["Int64"])

        # Operator between Memory and MergeTree objects.
        # Both carry aai_id, so order_by is implicit and the result uses
        # the context default (Memory).
        obj_c = await (obj_a + obj_b)

        result = await ch_client.query(f"""
            SELECT engine FROM system.tables WHERE name = '{obj_c.table}'
        """)
        assert result.result_rows[0][0] == "Memory"

        # Verify data is correct
        assert await obj_c.data() == [110, 220, 330]


async def test_schema_engine():
    """Test engine set on Schema instead of create_object param."""
    async with data_context():
        ch_client = get_ch_client()
        schema = Schema(
            fieldtype="a",
            columns={"value": ColumnInfo("Int64")},
            engine=ENGINE_MEMORY,
        )
        obj = await create_object(schema)

        result = await ch_client.query(f"""
            SELECT engine FROM system.tables WHERE name = '{obj.table}'
        """)
        assert result.result_rows[0][0] == "Memory"


async def test_schema_engine_param_precedence():
    """Test that engine param takes precedence over schema.engine."""
    async with data_context():
        ch_client = get_ch_client()
        schema = Schema(
            fieldtype="a",
            columns={"value": ColumnInfo("Int64")},
            engine=ENGINE_MEMORY,
        )
        obj = await create_object(schema, engine=ENGINE_MERGE_TREE)

        result = await ch_client.query(f"""
            SELECT engine FROM system.tables WHERE name = '{obj.table}'
        """)
        assert result.result_rows[0][0] == "MergeTree"


async def test_aggregating_merge_tree_engine():
    """Test AggregatingMergeTree with custom ORDER BY."""
    async with data_context(engine=ENGINE_MERGE_TREE):
        ch_client = get_ch_client()
        schema = Schema(
            fieldtype="a",
            columns={
                "key": ColumnInfo("String"),
                "value": ColumnInfo("Int64"),
            },
            engine=ENGINE_AGGREGATING_MERGE_TREE,
            order_by="key",
        )
        obj = await create_object(schema)

        result = await ch_client.query(f"""
            SELECT engine, sorting_key
            FROM system.tables WHERE name = '{obj.table}'
        """)
        assert result.result_rows[0][0] == "AggregatingMergeTree"
        assert result.result_rows[0][1] == "key"


async def test_schema_order_by_merge_tree():
    """Test custom ORDER BY on MergeTree."""
    async with data_context(engine=ENGINE_MERGE_TREE):
        ch_client = get_ch_client()
        schema = Schema(
            fieldtype="a",
            columns={
                "key": ColumnInfo("String"),
                "value": ColumnInfo("Int64"),
            },
            order_by="key",
        )
        obj = await create_object(schema)

        result = await ch_client.query(f"""
            SELECT engine, sorting_key
            FROM system.tables WHERE name = '{obj.table}'
        """)
        assert result.result_rows[0][0] == "MergeTree"
        assert result.result_rows[0][1] == "key"
