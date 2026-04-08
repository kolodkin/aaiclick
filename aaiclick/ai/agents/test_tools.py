"""
Tests for agent tools — mocks ClickHouse client for get_column_stats and get_schemas_for_nodes.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from aaiclick.oplog.lineage import OplogNode
from aaiclick.ai.agents.tools import get_column_stats, get_schemas_for_nodes


def _node(table: str, operation: str, kwargs: dict[str, str] | None = None) -> OplogNode:
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
        _node("result", "add", {"source_0": "a", "source_1": "b"}),
        _node("a", "create_from_value"),
    ]

    describe_results = {
        "result": _mock_query_result([("aai_id", "UInt64"), ("val", "Float64")]),
        "a": _mock_query_result([("aai_id", "UInt64"), ("val", "Float64")]),
        "b": _mock_query_result([("aai_id", "UInt64"), ("val", "Float64")]),
    }

    async def mock_query(sql):
        for tbl_name, res in describe_results.items():
            if tbl_name in sql:
                return res
        return _mock_query_result([])

    mock_client = MagicMock()
    mock_client.query = mock_query

    with patch("aaiclick.ai.agents.tools.get_ch_client", return_value=mock_client):
        result = await get_schemas_for_nodes(nodes)

    assert "# Table Schemas" in result
    assert "`result`:" in result
    assert "`a`:" in result
    assert "`b`:" in result
    assert "val: Float64" in result


async def test_get_schemas_for_nodes_empty_and_errors():
    """Empty nodes returns empty string; DESCRIBE failures produce 'unavailable'."""
    assert await get_schemas_for_nodes([]) == ""

    nodes = [_node("broken_table", "add")]

    async def mock_query(sql):
        raise RuntimeError("table not found")

    mock_client = MagicMock()
    mock_client.query = mock_query

    with patch("aaiclick.ai.agents.tools.get_ch_client", return_value=mock_client):
        result = await get_schemas_for_nodes(nodes)

    assert "schema unavailable" in result
