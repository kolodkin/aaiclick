"""
Tests for oplog graph traversal: backward_oplog, forward_oplog, oplog_subgraph.
"""

from __future__ import annotations

import pytest

from aaiclick.conftest import make_oplog_node
from aaiclick.data.data_context import create_object_from_value
from aaiclick.oplog.lineage import (
    OplogGraph,
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
    """forward_oplog includes seed table, downstream consumers, and sibling inputs."""
    a_table, b_table, result_table = await _run_pipeline()

    async with lineage_context():
        nodes = await forward_oplog(a_table)

    by_table = {n.table: n for n in nodes}
    # Seed (a), downstream consumer (result), sibling input (b)
    assert set(by_table) == {a_table, b_table, result_table}
    assert by_table[a_table].operation == "create_from_value"
    assert by_table[b_table].operation == "create_from_value"
    assert by_table[result_table].operation == "concat"


async def test_forward_subgraph_labels_all_edges(orch_ctx):
    """Forward subgraph labels every edge endpoint (seed + consumers + siblings)."""
    a_table, b_table, result_table = await _run_pipeline()

    async with lineage_context():
        graph = await oplog_subgraph(a_table, direction="forward")

    labels = graph.build_labels()
    assert labels[a_table].startswith("source_")
    assert labels[b_table].startswith("source_")
    assert labels[result_table] == "concat_result"
    # Every edge endpoint must resolve to a label
    for edge in graph.edges:
        assert edge.source in labels, f"unlabeled source {edge.source}"
        assert edge.target in labels, f"unlabeled target {edge.target}"


async def test_invalid_direction(orch_ctx):
    """oplog_subgraph raises ValueError for unknown direction."""
    async with lineage_context():
        with pytest.raises(ValueError, match="direction"):
            await oplog_subgraph("some_table", direction="sideways")


def test_prompt_context_id_breaking_ops_warning():
    """insert and concat get an aai_id freshness warning; other ops do not."""
    for op in ("insert", "concat"):
        node = make_oplog_node("target", op, {"source": "src"})
        context = OplogGraph(nodes=[node], edges=[]).to_prompt_context()
        assert "fresh aai_id" in context, f"{op} should warn"
        assert "do NOT match" in context

    node = make_oplog_node("result", "add", {"source_0": "a"})
    context = OplogGraph(nodes=[node], edges=[]).to_prompt_context()
    assert "fresh aai_id" not in context
