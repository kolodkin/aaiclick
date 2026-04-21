"""
Tests for agent tools — mocks ClickHouse client for get_column_stats and get_schemas_for_nodes.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from aaiclick.ai.agents.tools import (
    dispatch_tool,
    get_column_stats,
    get_schemas_for_nodes,
)
from aaiclick.testing import make_oplog_node


def _mock_query_result(rows, column_names=None):
    result = MagicMock()
    result.result_rows = rows
    result.column_names = column_names or []
    return result


async def test_get_column_stats_returns_all_columns():
    """get_column_stats discovers columns and returns stats for each one."""
    describe_result = _mock_query_result([("aai_id", "UInt64"), ("val", "Float64")])
    stats_result = _mock_query_result([(10, 10, 1, 100, 10, 8, 1.5, 9.9)])

    mock_client = MagicMock()
    mock_client.query = AsyncMock(side_effect=[describe_result, stats_result])

    with patch("aaiclick.ai.agents.tools.get_ch_client", return_value=mock_client):
        result = await get_column_stats("test_table")

    assert "aai_id: count=10" in result
    assert "val: count=10" in result
    assert "min=1.5" in result


async def test_get_column_stats_empty_table():
    """get_column_stats returns a message when the table has no columns."""
    describe_result = _mock_query_result([])

    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=describe_result)

    with patch("aaiclick.ai.agents.tools.get_ch_client", return_value=mock_client):
        result = await get_column_stats("empty_table")

    assert "not found or has no columns" in result


async def test_get_schemas_for_nodes_fetches_all_tables():
    """get_schemas_for_nodes fetches DESCRIBE TABLE for every unique table in nodes."""
    nodes = [
        make_oplog_node("result", "add", {"source_0": "a", "source_1": "b"}),
        make_oplog_node("a", "create_from_value"),
    ]

    with patch("aaiclick.ai.agents.tools.get_schema", new=AsyncMock(return_value="aai_id: UInt64\nval: Float64")):
        result = await get_schemas_for_nodes(nodes)

    assert "# Table Schemas" in result
    assert "`result`:" in result
    assert "`a`:" in result
    assert "`b`:" in result
    assert "val: Float64" in result


async def test_get_schemas_for_nodes_empty_and_errors():
    """Empty nodes returns empty string; get_schema failures produce 'unavailable'."""
    assert await get_schemas_for_nodes([]) == ""

    nodes = [make_oplog_node("broken_table", "add")]

    with patch("aaiclick.ai.agents.tools.get_schema", new=AsyncMock(side_effect=RuntimeError("fail"))):
        result = await get_schemas_for_nodes(nodes)

    assert "schema unavailable" in result


async def test_dispatch_tool_missing_required_arg_returns_readable_error():
    """A tool call with missing required arg must not crash the loop — it gets a
    retryable error string so the model can correct course."""
    result = await dispatch_tool("sample_table", {})
    assert "missing required argument" in result


async def test_dispatch_tool_unknown_tool_returns_label():
    result = await dispatch_tool("does_not_exist", {})
    assert "unknown tool" in result
