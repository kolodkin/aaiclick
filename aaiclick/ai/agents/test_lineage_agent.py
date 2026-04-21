"""
Tests for explain_lineage — mocks oplog_subgraph, schema fetch, and AIProvider.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from aaiclick.ai.agents.lineage_agent import explain_lineage
from aaiclick.oplog.lineage import OplogGraph
from aaiclick.test_utils import make_oplog_node


def _mock_provider(answer: str = "Explanation") -> MagicMock:
    provider = MagicMock()
    provider.query = AsyncMock(return_value=answer)
    return provider


def _mock_graph(*nodes):
    return OplogGraph(nodes=list(nodes), edges=[])


async def test_explain_lineage_returns_string_and_calls_oplog_subgraph():
    """explain_lineage() returns the AI answer and calls oplog_subgraph with the target table."""
    graph = _mock_graph(make_oplog_node("result", "add", {"source_0": "a", "source_1": "b"}))
    mock_subgraph = AsyncMock(return_value=graph)

    with (
        patch("aaiclick.ai.agents.lineage_agent.oplog_subgraph", new=mock_subgraph),
        patch("aaiclick.ai.agents.lineage_agent.get_ai_provider", return_value=_mock_provider("Result")),
        patch("aaiclick.ai.agents.lineage_agent.get_schemas_for_nodes", new=AsyncMock(return_value="")),
    ):
        result = await explain_lineage("result")

    assert result == "Result"
    mock_subgraph.assert_called_once_with("result", direction="backward")


async def test_explain_lineage_context_and_custom_question():
    """Graph is passed as context; custom question= overrides the default prompt."""
    graph = _mock_graph(make_oplog_node("result", "add"))
    captured_context: list[str] = []
    captured_prompts: list[str] = []

    async def mock_query(prompt, context="", system=""):
        captured_context.append(context)
        captured_prompts.append(prompt)
        return "ok"

    mock_provider = MagicMock()
    mock_provider.query = mock_query

    with (
        patch("aaiclick.ai.agents.lineage_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.lineage_agent.get_ai_provider", return_value=mock_provider),
        patch("aaiclick.ai.agents.lineage_agent.get_schemas_for_nodes", new=AsyncMock(return_value="")),
    ):
        await explain_lineage("result", question="Why is this table empty?")

    assert "add" in captured_context[0]
    assert "result" in captured_context[0]
    assert "Why is this table empty?" in captured_prompts[0]


async def test_explain_lineage_context_excludes_row_samples():
    """No row-sample text is injected — only graph structure + schemas."""
    graph = _mock_graph(make_oplog_node("result", "add", {"source_0": "a"}))
    captured_context: list[str] = []

    async def mock_query(prompt, context="", system=""):
        captured_context.append(context)
        return "ok"

    mock_provider = MagicMock()
    mock_provider.query = mock_query

    with (
        patch("aaiclick.ai.agents.lineage_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.lineage_agent.get_ai_provider", return_value=mock_provider),
        patch("aaiclick.ai.agents.lineage_agent.get_schemas_for_nodes", new=AsyncMock(return_value="")),
    ):
        await explain_lineage("result")

    assert "Sample rows" not in captured_context[0]


async def test_explain_lineage_with_prebuilt_graph_skips_subgraph():
    """Passing graph= skips the oplog_subgraph call, avoiding duplicate traversal."""
    graph = _mock_graph(make_oplog_node("result", "add"))
    mock_subgraph = AsyncMock(return_value=graph)

    with (
        patch("aaiclick.ai.agents.lineage_agent.oplog_subgraph", new=mock_subgraph),
        patch("aaiclick.ai.agents.lineage_agent.get_ai_provider", return_value=_mock_provider("ok")),
        patch("aaiclick.ai.agents.lineage_agent.get_schemas_for_nodes", new=AsyncMock(return_value="")),
    ):
        result = await explain_lineage("result", graph=graph)

    assert result == "ok"
    mock_subgraph.assert_not_called()


async def test_explain_lineage_context_includes_schemas():
    """When schemas are available, they appear in the context sent to the LLM."""
    graph = _mock_graph(make_oplog_node("result", "add"))
    captured_context: list[str] = []

    async def mock_query(prompt, context="", system=""):
        captured_context.append(context)
        return "ok"

    mock_provider = MagicMock()
    mock_provider.query = mock_query
    schema_text = "# Table Schemas\n\n`result`:\n  aai_id: UInt64\n  val: Float64"

    with (
        patch("aaiclick.ai.agents.lineage_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.lineage_agent.get_ai_provider", return_value=mock_provider),
        patch("aaiclick.ai.agents.lineage_agent.get_schemas_for_nodes", new=AsyncMock(return_value=schema_text)),
    ):
        await explain_lineage("result")

    assert "Table Schemas" in captured_context[0]
    assert "val: Float64" in captured_context[0]
