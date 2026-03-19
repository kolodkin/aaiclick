"""
Tests for oplog graph traversal: backward_oplog, forward_oplog, oplog_subgraph.
"""

from __future__ import annotations

import pytest

from aaiclick.data.data_context import data_context, create_object_from_value
from aaiclick.data.ch_client import create_ch_client
from aaiclick.oplog.models import init_oplog_tables
from aaiclick.oplog.lineage import (
    lineage_context,
    backward_oplog,
    forward_oplog,
    oplog_subgraph,
    OplogGraph,
)


async def _run_pipeline():
    """Run a small pipeline and return (a.table, b.table, result.table, ch_client)."""
    ch = await create_ch_client()
    async with data_context():
        async with lineage_context():
            a = await create_object_from_value([1, 2, 3])
            b = await create_object_from_value([4, 5, 6])
            result = await a.concat(b)
            return a.table, b.table, result.table, ch


async def test_backward_oplog_finds_sources():
    """backward_oplog traces upstream tables from result."""
    a_table, b_table, result_table, ch = await _run_pipeline()

    nodes = await backward_oplog(result_table, ch)
    found_tables = {n.table for n in nodes}
    assert result_table in found_tables


async def test_backward_oplog_includes_source_args():
    """backward_oplog finds the concat sources in node args."""
    a_table, b_table, result_table, ch = await _run_pipeline()

    nodes = await backward_oplog(result_table, ch)
    concat_node = next((n for n in nodes if n.operation == "concat"), None)
    assert concat_node is not None, "Expected a 'concat' node"
    assert a_table in concat_node.args or b_table in concat_node.args


async def test_backward_oplog_empty_for_unknown_table():
    """backward_oplog returns empty list for a table with no oplog entries."""
    ch = await create_ch_client()
    nodes = await backward_oplog("nonexistent_table_xyz", ch)
    assert nodes == []


async def test_forward_oplog_finds_downstream():
    """forward_oplog finds operations that consumed the source table."""
    a_table, b_table, result_table, ch = await _run_pipeline()

    nodes = await forward_oplog(a_table, ch)
    found_tables = {n.table for n in nodes}
    assert result_table in found_tables


async def test_oplog_subgraph_backward():
    """oplog_subgraph returns graph with nodes and edges."""
    a_table, b_table, result_table, ch = await _run_pipeline()

    graph = await oplog_subgraph(result_table, ch, direction="backward")
    assert isinstance(graph, OplogGraph)
    assert graph.nodes
    assert graph.edges


async def test_oplog_subgraph_forward():
    """oplog_subgraph forward direction works without error."""
    a_table, b_table, result_table, ch = await _run_pipeline()

    graph = await oplog_subgraph(a_table, ch, direction="forward")
    assert isinstance(graph, OplogGraph)


async def test_oplog_subgraph_invalid_direction():
    """oplog_subgraph raises ValueError for invalid direction."""
    ch = await create_ch_client()
    with pytest.raises(ValueError, match="direction"):
        await oplog_subgraph("some_table", ch, direction="sideways")



async def test_multi_step_pipeline_graph():
    """Multi-step pipeline produces correct backward oplog."""
    ch = await create_ch_client()
    async with data_context():
        async with lineage_context():
            raw = await create_object_from_value([1, 2, 3, 4, 5])
            filtered = await raw.copy()
            doubled = await (filtered + filtered)
            final_table = doubled.table
            raw_table = raw.table

    nodes = await backward_oplog(final_table, ch)
    operations = {n.operation for n in nodes}
    assert {"create_from_value", "copy", "+"} <= operations
    all_tables = {n.table for n in nodes}
    all_tables |= {src for n in nodes for src in n.args}
    all_tables |= {v for n in nodes for v in n.kwargs.values()}
    assert raw_table in all_tables


async def test_oplog_false_produces_no_log_entries():
    """oplog=False (default) produces zero entries in operation_log."""
    ch = await create_ch_client()

    await init_oplog_tables(ch)

    count_before = (await ch.query("SELECT count() FROM operation_log")).result_rows[0][0]

    async with data_context():
        a = await create_object_from_value([99, 98, 97])
        _ = await a.sum()

    count_after = (await ch.query("SELECT count() FROM operation_log")).result_rows[0][0]
    assert count_after == count_before, "oplog=False should not write any entries"
