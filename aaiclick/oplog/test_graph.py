"""
Tests for oplog graph traversal: backward_oplog, forward_oplog, oplog_subgraph.
"""

from __future__ import annotations

import pytest

from aaiclick.data.data_context import create_object_from_value
from aaiclick.oplog.lineage import (
    OplogGraph, OplogNode, OplogEdge,
    lineage_context, backward_oplog, forward_oplog, oplog_subgraph,
)
from aaiclick.orchestration.orch_context import task_scope


async def _run_pipeline():
    """Run a create/concat pipeline and return (a.table, b.table, result.table).

    Must be called inside an active orch_context.
    """
    async with task_scope(task_id=1, job_id=1, run_id=100):
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([4, 5, 6])
        result = await a.concat(b)
        return a.table, b.table, result.table


async def test_backward_oplog(orch_ctx):
    """backward_oplog returns the 3 upstream nodes with exact structure and edges."""
    a_table, b_table, result_table = await _run_pipeline()

    async with lineage_context():
        nodes = await backward_oplog(result_table)
        graph = await oplog_subgraph(result_table, direction="backward")

    by_table = {n.table: n for n in nodes}
    assert set(by_table) == {result_table, a_table, b_table}

    concat_node = by_table[result_table]
    assert concat_node.operation == "concat"
    assert set(concat_node.kwargs.values()) == {a_table, b_table}

    for t in (a_table, b_table):
        assert by_table[t].operation == "create_from_value"

    assert {(e.source, e.target) for e in graph.edges} == {
        (a_table, result_table),
        (b_table, result_table),
    }


async def test_forward_oplog(orch_ctx):
    """forward_oplog from a source finds exactly the concat node."""
    a_table, b_table, result_table = await _run_pipeline()

    async with lineage_context():
        nodes = await forward_oplog(a_table)

    assert len(nodes) == 1
    assert nodes[0].table == result_table
    assert nodes[0].operation == "concat"


async def test_invalid_direction(orch_ctx):
    """oplog_subgraph raises ValueError for unknown direction."""
    async with lineage_context():
        with pytest.raises(ValueError, match="direction"):
            await oplog_subgraph("some_table", direction="sideways")


def _make_node(table: str, operation: str, kwargs: dict[str, str] | None = None) -> OplogNode:
    return OplogNode(
        table=table,
        operation=operation,
        kwargs=kwargs or {},
        kwargs_aai_ids={},
        result_aai_ids=[],
        sql_template=None,
        task_id=None,
        job_id=None,
    )


def test_prompt_context_insert_warning():
    """insert operations get an aai_id freshness warning in the prompt context."""
    node = _make_node("target", "insert", {"source": "src", "target": "target"})
    graph = OplogGraph(nodes=[node], edges=[OplogEdge(source="src", target="target", operation="insert")])
    context = graph.to_prompt_context()
    assert "fresh aai_id" in context
    assert "do NOT match" in context


def test_prompt_context_concat_warning():
    """concat operations get an aai_id freshness warning in the prompt context."""
    node = _make_node("result", "concat", {"source_0": "a", "source_1": "b"})
    graph = OplogGraph(nodes=[node], edges=[])
    context = graph.to_prompt_context()
    assert "fresh aai_id" in context


def test_prompt_context_no_warning_for_other_ops():
    """Non-insert/concat operations do NOT get the aai_id warning."""
    node = _make_node("result", "add", {"source_0": "a", "source_1": "b"})
    graph = OplogGraph(nodes=[node], edges=[])
    context = graph.to_prompt_context()
    assert "fresh aai_id" not in context
