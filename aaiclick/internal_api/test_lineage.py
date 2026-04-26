"""
Tests for the lineage internal_api wrappers.

The underlying ``oplog_subgraph`` / ``explain_lineage`` / ``debug_result`` are
covered in their own test modules; here we only assert the wrappers correctly
delegate, pass kwargs through, and wrap plain-text agent answers in
``LineageAnswer``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from aaiclick.internal_api import lineage as lineage_api
from aaiclick.oplog.lineage import OplogGraph
from aaiclick.oplog.view_models import LineageAnswer
from aaiclick.testing import make_oplog_node


async def test_oplog_subgraph_returns_graph_and_passes_kwargs():
    graph = OplogGraph(nodes=[make_oplog_node("result", "add")], edges=[])
    mock_subgraph = AsyncMock(return_value=graph)

    with patch("aaiclick.internal_api.lineage._oplog_subgraph", new=mock_subgraph):
        result = await lineage_api.oplog_subgraph("result", direction="forward", max_depth=3)

    assert result is graph
    mock_subgraph.assert_awaited_once_with("result", direction="forward", max_depth=3)


async def test_explain_lineage_wraps_string_in_answer():
    mock_explain = AsyncMock(return_value="Explanation text.")

    with patch("aaiclick.internal_api.lineage._explain_lineage", new=mock_explain):
        result = await lineage_api.explain_lineage("result", question="Why?")

    assert isinstance(result, LineageAnswer)
    assert result.text == "Explanation text."
    mock_explain.assert_awaited_once_with("result", question="Why?")


async def test_debug_result_wraps_string_in_answer():
    mock_debug = AsyncMock(return_value="Debug answer.")

    with patch("aaiclick.internal_api.lineage._debug_result", new=mock_debug):
        result = await lineage_api.debug_result("result", question="Why?", max_iterations=3)

    assert isinstance(result, LineageAnswer)
    assert result.text == "Debug answer."
    mock_debug.assert_awaited_once_with("result", question="Why?", max_iterations=3)
