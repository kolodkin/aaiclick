"""Tests for configurable table engine support."""

from aaiclick import DataContext, create_object, create_object_from_value
from aaiclick.data.models import ENGINE_MEMORY, ENGINE_MERGE_TREE, Schema


async def test_context_with_memory_engine():
    """Test non-default engine (Memory) set on DataContext."""
    async with DataContext(engine=ENGINE_MEMORY) as ctx:
        obj_a = await create_object_from_value([1, 2, 3])
        obj_b = await create_object_from_value([4, 5, 6])

        # Verify source tables use Memory engine
        result = await ctx.ch_client.query(f"""
            SELECT engine FROM system.tables WHERE name = '{obj_a._table_name}'
        """)
        assert result.result_rows[0][0] == "Memory"

        # Operator should also create Memory table
        obj_c = await (obj_a + obj_b)

        result = await ctx.ch_client.query(f"""
            SELECT engine FROM system.tables WHERE name = '{obj_c._table_name}'
        """)
        assert result.result_rows[0][0] == "Memory"

        # Verify data is correct
        assert await obj_c.data() == [5, 7, 9]


async def test_create_object_with_engine_override():
    """Test non-default engine passed directly to create_object."""
    async with DataContext() as ctx:  # Default MergeTree
        # Override with Memory engine
        schema = Schema(fieldtype="a", columns={"aai_id": "UInt64", "value": "Int64"})
        obj_a = await create_object(schema, engine=ENGINE_MEMORY)

        result = await ctx.ch_client.query(f"""
            SELECT engine FROM system.tables WHERE name = '{obj_a._table_name}'
        """)
        assert result.result_rows[0][0] == "Memory"

        # Without override, should use context default (MergeTree)
        obj_b = await create_object(schema)

        result = await ctx.ch_client.query(f"""
            SELECT engine FROM system.tables WHERE name = '{obj_b._table_name}'
        """)
        assert result.result_rows[0][0] == "MergeTree"


async def test_mixed_engine_scenario():
    """Test mix of context engine and per-object override."""
    async with DataContext(engine=ENGINE_MEMORY) as ctx:
        # Create with context default (Memory)
        obj_a = await create_object_from_value([10, 20, 30])

        result = await ctx.ch_client.query(f"""
            SELECT engine FROM system.tables WHERE name = '{obj_a._table_name}'
        """)
        assert result.result_rows[0][0] == "Memory"

        # Override to MergeTree for specific object
        schema = Schema(fieldtype="a", columns={"aai_id": "UInt64", "value": "Int64"})
        obj_b = await create_object(schema, engine=ENGINE_MERGE_TREE)

        result = await ctx.ch_client.query(f"""
            SELECT engine FROM system.tables WHERE name = '{obj_b._table_name}'
        """)
        assert result.result_rows[0][0] == "MergeTree"

        # Insert data into obj_b
        await ctx.ch_client.insert(obj_b._table_name, [[1, 100], [2, 200], [3, 300]])

        # Operator between Memory and MergeTree objects
        # Result should use context default (Memory)
        obj_c = await (obj_a + obj_b)

        result = await ctx.ch_client.query(f"""
            SELECT engine FROM system.tables WHERE name = '{obj_c._table_name}'
        """)
        assert result.result_rows[0][0] == "Memory"

        # Verify data is correct
        assert await obj_c.data() == [110, 220, 330]
