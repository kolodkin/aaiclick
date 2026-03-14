"""
Tests for lineage graph queries — backward_explain and forward_impact.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context
from aaiclick.lineage.graph import backward_explain, forward_impact


async def test_backward_explain_simple():
    """backward_explain traces a single operation."""
    async with data_context(lineage=True):
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([10, 20, 30])
        result = await (a + b)

        ctx = await backward_explain(result.table)

        # Should find the add operation and the two create_from_value ops
        op_names = [op.operation for op in ctx.graph.operations]
        assert "+" in op_names
        assert "create_from_value" in op_names

        # Should have schemas
        assert result.table in ctx.schemas

        # Should have samples
        assert result.table in ctx.samples


async def test_backward_explain_chain():
    """backward_explain traces a chain of operations."""
    async with data_context(lineage=True):
        a = await create_object_from_value([1, 2, 3])
        b = await (a * 2)
        c = await (b + 10)

        ctx = await backward_explain(c.table)
        op_names = [op.operation for op in ctx.graph.operations]

        # Should trace back through: + -> * -> create_from_value
        assert "+" in op_names
        assert "*" in op_names
        assert "create_from_value" in op_names


async def test_backward_explain_to_prompt_context():
    """to_prompt_context() returns non-empty formatted string."""
    async with data_context(lineage=True):
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([10, 20, 30])
        result = await (a + b)

        ctx = await backward_explain(result.table)
        prompt = ctx.to_prompt_context()

        assert "Operation Lineage" in prompt
        assert "Schemas" in prompt
        assert result.table in prompt


async def test_forward_impact_simple():
    """forward_impact traces downstream operations."""
    async with data_context(lineage=True):
        a = await create_object_from_value([1, 2, 3])
        b = await (a * 2)
        c = await (a + 10)

        ops = await forward_impact(a.table)
        result_tables = [op.result_table for op in ops]

        # a was used to create b and c
        assert b.table in result_tables
        assert c.table in result_tables


async def test_forward_impact_chain():
    """forward_impact traces through a chain of downstream ops."""
    async with data_context(lineage=True):
        a = await create_object_from_value([1, 2, 3])
        b = await (a * 2)
        c = await (b + 10)

        ops = await forward_impact(a.table)
        result_tables = [op.result_table for op in ops]

        # a -> b -> c
        assert b.table in result_tables
        assert c.table in result_tables


async def test_backward_explain_requires_lineage():
    """backward_explain raises if lineage is not enabled."""
    async with data_context():
        a = await create_object_from_value([1, 2, 3])
        with pytest.raises(RuntimeError, match="No active LineageCollector"):
            await backward_explain(a.table)


async def test_forward_impact_requires_lineage():
    """forward_impact raises if lineage is not enabled."""
    async with data_context():
        a = await create_object_from_value([1, 2, 3])
        with pytest.raises(RuntimeError, match="No active LineageCollector"):
            await forward_impact(a.table)


async def test_backward_explain_no_lineage_for_table():
    """backward_explain returns empty graph for table with no recorded lineage."""
    async with data_context(lineage=True):
        ctx = await backward_explain("nonexistent_table")
        assert ctx.graph.operations == []
        assert ctx.graph.nodes == []


async def test_aggregation_lineage_graph():
    """Aggregation results appear in the lineage graph."""
    async with data_context(lineage=True):
        a = await create_object_from_value([1, 2, 3, 4, 5])
        total = await a.sum()
        normalized = await (a / total)

        ctx = await backward_explain(normalized.table)
        op_names = [op.operation for op in ctx.graph.operations]

        # Should trace: / -> a (create_from_value), / -> total (sum -> a)
        assert "/" in op_names
        assert "sum" in op_names
        assert "create_from_value" in op_names
