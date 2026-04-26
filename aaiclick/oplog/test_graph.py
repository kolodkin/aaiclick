"""
Tests for oplog graph traversal: backward_oplog, forward_oplog, oplog_subgraph.
"""

from __future__ import annotations

import pytest

from aaiclick.data.data_context import create_object_from_value
from aaiclick.oplog.lineage import (
    OplogGraph,
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
            await oplog_subgraph("some_table", direction="sideways")  # type: ignore[arg-type]


def test_replace_labels_job_scoped_table():
    """Job-scoped names (j_<job_id>_<name>) get replaced even though they
    aren't snowflake-shaped tokens."""
    text = "Step 1: read `j_42_basic_lineage_prices`, then write `j_42_basic_lineage_total`."
    labels = {
        "j_42_basic_lineage_prices": "source_A",
        "j_42_basic_lineage_total": "add_result",
    }
    out = OplogGraph.replace_labels(text, labels)
    assert "j_42_basic_lineage_prices" not in out
    assert "j_42_basic_lineage_total" not in out
    assert "source_A" in out
    assert "add_result" in out


def test_replace_labels_longest_key_wins():
    """When one label key is a prefix of another, the longer key matches
    the longer token rather than partially-replacing it."""
    labels = {
        "j_42_revenue": "short_label",
        "j_42_revenue_total": "long_label",
    }
    text = "Inputs: `j_42_revenue` and `j_42_revenue_total`."
    out = OplogGraph.replace_labels(text, labels)
    assert "long_label" in out
    assert "short_label" in out
    # The long token must NOT have been mangled into short_label + "_total".
    assert "short_label_total" not in out


def test_replace_labels_t_prefix_and_bare_snowflake():
    """A `t_<id>` label also rewrites the bare-digit form an LLM may emit."""
    labels = {"t_12345678901234567": "source_A"}
    text = "table t_12345678901234567 produced row 12345678901234567"
    out = OplogGraph.replace_labels(text, labels)
    assert out == "table source_A produced row source_A"


def test_replace_labels_unknown_token_untouched():
    """Tokens that aren't in the labels dict pass through unchanged."""
    labels = {"j_42_known": "source_A"}
    text = "known: j_42_known, unknown: j_99_other, snowflake: 12345678901234567"
    out = OplogGraph.replace_labels(text, labels)
    assert "source_A" in out
    assert "j_99_other" in out
    assert "12345678901234567" in out


def test_replace_labels_empty_dict_returns_input():
    """Empty labels dict short-circuits — no regex compiled, text unchanged."""
    text = "anything goes here including t_12345678901234567"
    assert OplogGraph.replace_labels(text, {}) == text
