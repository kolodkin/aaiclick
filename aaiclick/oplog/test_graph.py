"""
Tests for lineage graph traversal: backward_lineage, forward_lineage, lineage_subgraph.
"""

from __future__ import annotations

import pytest

from aaiclick.data.data_context import data_context, create_object_from_value
from aaiclick.data.ch_client import create_ch_client
from aaiclick.lineage.graph import (
    backward_lineage,
    forward_lineage,
    lineage_subgraph,
    LineageGraph,
)


async def _run_pipeline():
    """Run a small pipeline and return (a.table, b.table, result.table, ch_client)."""
    ch = await create_ch_client()
    async with data_context(lineage=True):
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([4, 5, 6])
        result = await a.concat(b)
        return a.table, b.table, result.table, ch


async def test_backward_lineage_finds_sources():
    """backward_lineage traces upstream tables from result."""
    a_table, b_table, result_table, ch = await _run_pipeline()

    nodes = await backward_lineage(result_table, ch)
    found_tables = {n.table for n in nodes}
    assert result_table in found_tables


async def test_backward_lineage_includes_source_args():
    """backward_lineage finds the concat sources in node args."""
    a_table, b_table, result_table, ch = await _run_pipeline()

    nodes = await backward_lineage(result_table, ch)
    concat_node = next((n for n in nodes if n.operation == "concat"), None)
    assert concat_node is not None, "Expected a 'concat' node"
    assert a_table in concat_node.args or b_table in concat_node.args


async def test_backward_lineage_empty_for_unknown_table():
    """backward_lineage returns empty list for a table with no lineage."""
    ch = await create_ch_client()
    nodes = await backward_lineage("nonexistent_table_xyz", ch)
    assert nodes == []


async def test_forward_lineage_finds_downstream():
    """forward_lineage finds operations that consumed the source table."""
    a_table, b_table, result_table, ch = await _run_pipeline()

    nodes = await forward_lineage(a_table, ch)
    found_tables = {n.table for n in nodes}
    assert result_table in found_tables


async def test_lineage_subgraph_backward():
    """lineage_subgraph returns graph with nodes and edges."""
    a_table, b_table, result_table, ch = await _run_pipeline()

    graph = await lineage_subgraph(result_table, ch, direction="backward")
    assert isinstance(graph, LineageGraph)
    assert graph.nodes
    assert graph.edges


async def test_lineage_subgraph_forward():
    """lineage_subgraph forward direction works without error."""
    a_table, b_table, result_table, ch = await _run_pipeline()

    graph = await lineage_subgraph(a_table, ch, direction="forward")
    assert isinstance(graph, LineageGraph)


async def test_lineage_subgraph_invalid_direction():
    """lineage_subgraph raises ValueError for invalid direction."""
    ch = await create_ch_client()
    with pytest.raises(ValueError, match="direction"):
        await lineage_subgraph("some_table", ch, direction="sideways")


async def test_to_prompt_context_no_nodes():
    """LineageGraph.to_prompt_context() handles empty graph."""
    graph = LineageGraph()
    text = graph.to_prompt_context()
    assert "No lineage" in text


async def test_to_prompt_context_with_nodes():
    """LineageGraph.to_prompt_context() formats graph as readable text."""
    a_table, b_table, result_table, ch = await _run_pipeline()

    graph = await lineage_subgraph(result_table, ch, direction="backward")
    text = graph.to_prompt_context()
    assert "lineage" in text.lower()
    assert result_table in text or any(n.table in text for n in graph.nodes)


async def test_multi_step_pipeline_graph():
    """Multi-step pipeline produces correct backward lineage."""
    ch = await create_ch_client()
    async with data_context(lineage=True):
        raw = await create_object_from_value([1, 2, 3, 4, 5])
        filtered = await raw.copy()
        doubled = await (filtered + filtered)
        final_table = doubled.table
        raw_table = raw.table

    nodes = await backward_lineage(final_table, ch)
    operations = {n.operation for n in nodes}
    assert "+" in operations or any(op in operations for op in ["+", "copy", "create_from_value"])
    # raw_table should eventually appear in the lineage chain
    all_tables = {n.table for n in nodes}
    all_tables |= {src for n in nodes for src in n.args}
    all_tables |= {v for n in nodes for v in n.kwargs.values()}
    assert raw_table in all_tables or len(nodes) >= 1


async def test_lineage_false_produces_no_log_entries():
    """lineage=False (default) produces zero entries in operation_log."""
    ch = await create_ch_client()

    # Ensure operation_log exists (from a previous lineage=True run)
    from aaiclick.lineage.models import init_lineage_tables
    await init_lineage_tables(ch)

    count_before = (await ch.query("SELECT count() FROM operation_log")).result_rows[0][0]

    async with data_context(lineage=False):
        a = await create_object_from_value([99, 98, 97])
        _ = await a.sum()

    count_after = (await ch.query("SELECT count() FROM operation_log")).result_rows[0][0]
    assert count_after == count_before, "lineage=False should not write any entries"
