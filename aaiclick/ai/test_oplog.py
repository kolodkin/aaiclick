"""
Tests for aaiclick.ai.oplog - oplog graph formatting for LLM consumption.
"""

from aaiclick.ai.oplog import oplog_graph_to_prompt_context
from aaiclick.data.data_context import data_context, create_object_from_value
from aaiclick.data.ch_client import create_ch_client
from aaiclick.oplog.graph import OplogGraph, oplog_subgraph


async def test_oplog_graph_to_prompt_context_empty():
    """Returns fallback message for empty graph."""
    graph = OplogGraph()
    text = oplog_graph_to_prompt_context(graph)
    assert "No operation log" in text


async def test_oplog_graph_to_prompt_context_with_nodes():
    """Formats graph nodes and edges as readable text."""
    ch = await create_ch_client()
    async with data_context(oplog=True):
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([4, 5, 6])
        result = await a.concat(b)
        result_table = result.table

    graph = await oplog_subgraph(result_table, ch, direction="backward")
    text = oplog_graph_to_prompt_context(graph)
    assert "operation" in text.lower()
    assert result_table in text or any(n.table in text for n in graph.nodes)
