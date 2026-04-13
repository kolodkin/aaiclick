"""
Tests for oplog graph traversal: backward_oplog, forward_oplog, oplog_subgraph.
"""

from __future__ import annotations

import pytest

from aaiclick.conftest import make_oplog_node
from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.data_context.ch_client import create_ch_client
from aaiclick.oplog.lineage import (
    OplogGraph,
    backward_oplog,
    backward_oplog_row,
    forward_oplog,
    lineage_context,
    oplog_subgraph,
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
    """forward_oplog includes the seed table plus its downstream consumers."""
    a_table, b_table, result_table = await _run_pipeline()

    async with lineage_context():
        nodes = await forward_oplog(a_table)

    by_table = {n.table: n for n in nodes}
    assert set(by_table) == {a_table, result_table}
    assert by_table[a_table].operation == "create_from_value"
    assert by_table[result_table].operation == "concat"


async def test_forward_subgraph_labels_all_edges(orch_ctx):
    """build_labels covers every edge endpoint, including sibling inputs
    not visited by the forward traversal."""
    a_table, b_table, result_table = await _run_pipeline()

    async with lineage_context():
        graph = await oplog_subgraph(a_table, direction="forward")

    labels = graph.build_labels()
    # a is a node (create_from_value) → source_*
    # result is a node (concat) → concat_result
    # b is NOT a node but IS an edge source → generic source_*
    assert labels[a_table].startswith("source_")
    assert labels[b_table].startswith("source_")
    assert labels[result_table] == "concat_result"
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


async def test_backward_oplog_row_strategy_populated(orch_ctx):
    """Row trace walks one hop backward for a STRATEGY-populated operation."""
    left_table = "p_row_trace_left"
    right_table = "p_row_trace_right"

    async with task_scope(
        task_id=1,
        job_id=1,
        run_id=100,
        sampling_strategy={left_table: "value = 20"},
    ):
        a = await create_object_from_value([10, 20, 30], name="row_trace_left")
        b = await create_object_from_value([1, 2, 3], name="row_trace_right")
        result = await (a + b)
        result_table = result.table

    ch = await create_ch_client()
    rows = (await ch.query(
        f"SELECT aai_id FROM {result_table} WHERE value = 22"
    )).result_rows
    assert rows, f"expected value=22 in {result_table}"
    target_id = rows[0][0]

    async with lineage_context():
        steps = await backward_oplog_row(result_table, target_id)

    assert steps, "expected at least one step"
    first = steps[0]
    assert first.table == result_table
    assert first.aai_id == target_id
    assert first.operation == "+"
    assert set(first.source_aai_ids.keys()) == {"left", "right"}

    await ch.command(f"DROP TABLE IF EXISTS {left_table}")
    await ch.command(f"DROP TABLE IF EXISTS {right_table}")


async def test_backward_oplog_row_none_mode_returns_empty(orch_ctx):
    """Row trace returns [] when the job ran under NONE mode (empty lineage arrays)."""
    a_table, b_table, result_table = await _run_pipeline()

    async with lineage_context():
        steps = await backward_oplog_row(result_table, 1)

    assert steps == []
