"""
Tests for aaiclick.ai.agents.lineage_tools — the LLM-facing toolbox:
argument coercion, graph-scoped tool results, and text formatting.

The sandbox itself (scope and read-only enforcement, row caps) is tested in
``aaiclick/oplog/test_query_sandbox.py``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aaiclick.ai.agents.lineage_tools import LINEAGE_TOOL_DEFINITIONS, LineageToolbox
from aaiclick.data.data_context import get_ch_client
from aaiclick.oplog.lineage import OplogEdge, OplogGraph
from aaiclick.oplog.query_sandbox import QueryResult, ToolError
from aaiclick.testing import make_oplog_node

INTERMEDIATE_TABLE = "t_11111111111111111111"
TARGET_TABLE = "t_22222222222222222222"
PERSISTENT_INPUT = "p_raw_sales"


def _sample_graph() -> OplogGraph:
    """p_raw_sales -> t_1 (intermediate, filter) -> t_2 (target, aggregate)."""
    nodes = [
        make_oplog_node(INTERMEDIATE_TABLE, "filter", {"input": PERSISTENT_INPUT}),
        make_oplog_node(TARGET_TABLE, "aggregate", {"input": INTERMEDIATE_TABLE}),
    ]
    nodes[0].sql_template = f"SELECT * FROM {PERSISTENT_INPUT} WHERE active"
    nodes[1].sql_template = f"SELECT sum(x) FROM {INTERMEDIATE_TABLE}"
    edges = [
        OplogEdge(source=PERSISTENT_INPUT, target=INTERMEDIATE_TABLE, operation="filter"),
        OplogEdge(source=INTERMEDIATE_TABLE, target=TARGET_TABLE, operation="aggregate"),
    ]
    return OplogGraph(nodes=nodes, edges=edges)


def _mock_query_result(rows, column_names=None):
    result = MagicMock()
    result.result_rows = rows
    result.column_names = column_names or []
    return result


def _ch_client_with_real_parser(rows, column_names=None):
    """Client double whose ``EXPLAIN`` goes to real ClickHouse; data queries return ``rows``."""
    client = MagicMock()
    client.raw_query = get_ch_client().raw_query
    client.query = AsyncMock(return_value=_mock_query_result(rows, column_names))
    return client


async def test_query_table_out_of_scope_error_points_at_list_graph_nodes(orch_ctx):
    toolbox = LineageToolbox(_sample_graph())
    err = await toolbox.query_table("SELECT * FROM t_99999999999999999999")
    assert isinstance(err, ToolError)
    assert err.kind == "out_of_scope"
    assert "list_graph_nodes" in err.message


async def test_query_table_settings_error_has_no_scope_hint(orch_ctx):
    toolbox = LineageToolbox(_sample_graph())
    err = await toolbox.query_table(f"SELECT id FROM {TARGET_TABLE} SETTINGS max_execution_time = 0")
    assert isinstance(err, ToolError)
    assert err.kind == "invalid_argument"
    assert "list_graph_nodes" not in err.message


async def test_query_table_coerces_string_row_limit(orch_ctx):
    """Llama-3.1 sometimes emits row_limit as a JSON string. Coerce to int
    instead of crashing in run_select's ``min(row_limit, ROW_LIMIT_CEILING)``.
    """
    toolbox = LineageToolbox(_sample_graph())
    mock_client = _ch_client_with_real_parser([(1,)], ["id"])

    with patch("aaiclick.oplog.query_sandbox.get_ch_client", return_value=mock_client):
        result = await toolbox.query_table(f"SELECT id FROM {TARGET_TABLE}", row_limit="5")

    assert isinstance(result, QueryResult)
    assert mock_client.query.call_args.kwargs["settings"]["limit"] == 6


async def test_query_table_rejects_non_numeric_row_limit():
    toolbox = LineageToolbox(_sample_graph())
    err = await toolbox.query_table(f"SELECT id FROM {TARGET_TABLE}", row_limit="not-a-number")
    assert isinstance(err, ToolError)
    assert err.kind == "invalid_argument"


async def test_get_op_sql_unknown_table_not_found():
    toolbox = LineageToolbox(_sample_graph())
    err = await toolbox.get_op_sql("t_99999999999999999999")
    assert isinstance(err, ToolError)
    assert err.kind == "not_found"


async def test_list_graph_nodes_classifies_kinds_and_liveness():
    toolbox = LineageToolbox(_sample_graph())
    # Only intermediate and target exist; persistent input is gone.
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([(INTERMEDIATE_TABLE,), (TARGET_TABLE,)], ["name"]))

    with patch("aaiclick.oplog.query_sandbox.get_ch_client", return_value=mock_client):
        nodes = await toolbox.list_graph_nodes()

    by_table = {n.table: n for n in nodes}
    assert by_table[PERSISTENT_INPUT].kind == "input"
    assert by_table[INTERMEDIATE_TABLE].kind == "intermediate"
    assert by_table[TARGET_TABLE].kind == "target"
    assert by_table[PERSISTENT_INPUT].live is False
    assert by_table[INTERMEDIATE_TABLE].live is True
    assert by_table[TARGET_TABLE].live is True


async def test_list_graph_nodes_caches_liveness_within_session():
    """Repeat calls reuse the cached liveness map — no second system.tables round-trip."""
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([(TARGET_TABLE,)], ["name"]))

    with patch("aaiclick.oplog.query_sandbox.get_ch_client", return_value=mock_client):
        await toolbox.list_graph_nodes()
        await toolbox.list_graph_nodes()

    assert mock_client.query.await_count == 1


async def test_get_schema_rejects_out_of_scope():
    toolbox = LineageToolbox(_sample_graph())
    err = await toolbox.get_schema("t_99999999999999999999")
    assert isinstance(err, ToolError)
    assert err.kind == "out_of_scope"


async def test_get_schema_not_live_when_describe_fails():
    """DESCRIBE TABLE raising (e.g., table dropped mid-session) → ToolError('not_live')."""
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(side_effect=RuntimeError("UNKNOWN_TABLE"))

    with patch("aaiclick.oplog.query_sandbox.get_ch_client", return_value=mock_client):
        err = await toolbox.get_schema(TARGET_TABLE)

    assert isinstance(err, ToolError)
    assert err.kind == "not_live"
    assert TARGET_TABLE in err.message


def test_lineage_tool_definitions_cover_every_toolbox_method():
    """LINEAGE_TOOL_DEFINITIONS must mention exactly the tools dispatch_tool handles."""
    declared = {t["function"]["name"] for t in LINEAGE_TOOL_DEFINITIONS}
    assert declared == {"list_graph_nodes", "get_op_sql", "get_schema", "query_table"}


@pytest.mark.parametrize(
    "tool, arguments, expected",
    [
        pytest.param("does_not_exist", {}, "unknown tool", id="unknown-tool"),
        pytest.param("query_table", {}, "missing required argument", id="missing-required-arg"),
    ],
)
async def test_dispatch_tool_returns_error_string(tool, arguments, expected):
    toolbox = LineageToolbox(_sample_graph())
    result = await toolbox.dispatch_tool(tool, arguments)
    assert expected in result


async def test_dispatch_tool_formats_query_result_as_table(orch_ctx):
    toolbox = LineageToolbox(_sample_graph())
    mock_client = _ch_client_with_real_parser([(1, "a"), (2, "b")], ["id", "name"])

    with patch("aaiclick.oplog.query_sandbox.get_ch_client", return_value=mock_client):
        result = await toolbox.dispatch_tool("query_table", {"sql": f"SELECT id, name FROM {TARGET_TABLE}"})

    assert "id | name" in result
    assert "1 | a" in result
    assert "2 | b" in result


async def test_dispatch_tool_formats_tool_error_with_kind():
    toolbox = LineageToolbox(_sample_graph())
    result = await toolbox.dispatch_tool("query_table", {"sql": "SELECT * FROM t_99999999999999999999"})
    assert '"kind": "out_of_scope"' in result


async def test_dispatch_tool_formats_list_graph_nodes():
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([(TARGET_TABLE,)], ["name"]))

    with patch("aaiclick.oplog.query_sandbox.get_ch_client", return_value=mock_client):
        result = await toolbox.dispatch_tool("list_graph_nodes", {})

    assert TARGET_TABLE in result
    assert "[target]" in result
    assert "live=True" in result


async def test_dispatch_tool_formats_get_op_sql():
    toolbox = LineageToolbox(_sample_graph())
    result = await toolbox.dispatch_tool("get_op_sql", {"table": TARGET_TABLE})
    assert f"SELECT sum(x) FROM {INTERMEDIATE_TABLE}" == result


async def test_dispatch_tool_formats_get_schema():
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([("id", "UInt64"), ("val", "Float64")]))

    with patch("aaiclick.oplog.query_sandbox.get_ch_client", return_value=mock_client):
        result = await toolbox.dispatch_tool("get_schema", {"table": TARGET_TABLE})

    assert "id: UInt64" in result
    assert "val: Float64" in result
