"""
Tests for agent tools — mocks ClickHouse client for get_schemas_for_nodes.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from aaiclick.ai.agents.tools import get_schemas_for_nodes
from aaiclick.testing import make_oplog_node


async def test_get_schemas_for_nodes_fetches_all_tables():
    """get_schemas_for_nodes fetches DESCRIBE TABLE for every unique table in nodes."""
    nodes = [
        make_oplog_node("result", "add", {"source_0": "a", "source_1": "b"}),
        make_oplog_node("a", "create_from_value"),
    ]

    with patch("aaiclick.ai.agents.tools.get_schema", new=AsyncMock(return_value="id: UInt64\nval: Float64")):
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
