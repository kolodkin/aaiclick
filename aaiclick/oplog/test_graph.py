"""
Tests for oplog graph traversal: backward_oplog, forward_oplog, oplog_subgraph.
"""

from __future__ import annotations

import pytest

from aaiclick.data.data_context import create_object_from_value
from aaiclick.oplog.lineage import (
    OplogGraph,
    OplogNode,
    backward_oplog,
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


def test_graph_nodes_carry_kind_operation_and_liveness():
    """p_raw -> t_1 (filter) -> t_2 (aggregate); p_raw is only read, so it has no operation."""
    graph = OplogGraph(
        nodes=[
            OplogNode(table="t_1", operation="filter", kwargs={"input": "p_raw"}),
            OplogNode(table="t_2", operation="aggregate", kwargs={"input": "t_1"}),
        ],
        edges=[],
    )

    assert graph.node_kinds() == {"p_raw": "input", "t_1": "intermediate", "t_2": "target"}
    nodes = graph.graph_nodes({"t_1": True, "t_2": True})
    assert [(n.table, n.kind, n.operation, n.live) for n in nodes] == [
        ("p_raw", "input", None, False),
        ("t_1", "intermediate", "filter", True),
        ("t_2", "target", "aggregate", True),
    ]


def test_node_kinds_persistent_target_is_a_target():
    """The p_* rule marks persistent tables the graph only reads as inputs; a
    persistent table the graph produces and nothing consumes is still the target."""
    graph = OplogGraph(nodes=[OplogNode(table="p_total", operation="sum", kwargs={"input": "p_raw"})], edges=[])
    assert graph.node_kinds() == {"p_raw": "input", "p_total": "target"}
