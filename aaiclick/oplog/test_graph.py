"""
Tests for oplog graph traversal: backward_oplog, forward_oplog, oplog_subgraph.
"""

from __future__ import annotations

import pytest

from aaiclick.data.data_context import create_object_from_value
from aaiclick.oplog.lineage import (
    OplogGraph,
    backward_oplog,
    classify_nodes,
    forward_oplog,
    lineage_context,
    oplog_subgraph,
)
from aaiclick.orchestration.orch_context import task_scope
from aaiclick.testing import make_oplog_node


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


@pytest.mark.parametrize(
    "direction, table",
    [
        # Closes the IN list of the forward frontier query and widens it to every row.
        pytest.param("forward", "x') OR true OR arrayExists(v -> v IN ('x", id="forward-quote"),
        # A trailing backslash neutralises quote-only escaping; the rest is live SQL.
        pytest.param("forward", "x\\' OR true --", id="forward-backslash"),
        pytest.param("backward", "x\\' OR true --", id="backward-backslash"),
    ],
)
async def test_oplog_subgraph_treats_table_name_as_data(orch_ctx, direction, table):
    """A hostile table name matches nothing instead of widening the query to the whole log."""
    await _run_pipeline()

    async with lineage_context():
        graph = await oplog_subgraph(table, direction=direction)

    assert graph.nodes == []


async def test_invalid_direction(orch_ctx):
    """oplog_subgraph raises ValueError for unknown direction."""
    async with lineage_context():
        with pytest.raises(ValueError, match="direction"):
            await oplog_subgraph("some_table", direction="sideways")  # type: ignore[arg-type]


def test_classify_nodes_labels_input_intermediate_target():
    nodes = [
        make_oplog_node("t_1", "filter", {"input": "p_raw"}),
        make_oplog_node("t_2", "aggregate", {"input": "t_1"}),
    ]
    graph = OplogGraph(nodes=nodes, edges=[])
    assert classify_nodes(graph) == {"p_raw": "input", "t_1": "intermediate", "t_2": "target"}
