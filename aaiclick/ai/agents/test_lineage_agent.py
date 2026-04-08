"""
Tests for explain_lineage — mocks oplog_subgraph, sample_table, and AIProvider.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from aaiclick.conftest import make_oplog_node
from aaiclick.oplog.lineage import OplogGraph
from aaiclick.ai.agents.lineage_agent import explain_lineage


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
        patch("aaiclick.ai.agents.lineage_agent.sample_table", new=AsyncMock(return_value="c1\nv1")),
        patch("aaiclick.ai.agents.lineage_agent.get_ai_provider", return_value=_mock_provider("Result")),
        patch("aaiclick.ai.agents.lineage_agent.get_schemas_for_nodes", new=AsyncMock(return_value="")),
    ):
        result = await explain_lineage("result")

    assert result == "Result"
    mock_subgraph.assert_called_once_with("result", direction="backward")


async def test_explain_lineage_context_and_custom_question():
    """lineage graph is passed as context; custom question= overrides the default prompt."""
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
        patch("aaiclick.ai.agents.lineage_agent.sample_table", new=AsyncMock(return_value="sample")),
        patch("aaiclick.ai.agents.lineage_agent.get_ai_provider", return_value=mock_provider),
        patch("aaiclick.ai.agents.lineage_agent.get_schemas_for_nodes", new=AsyncMock(return_value="")),
    ):
        await explain_lineage("result", question="Why is this table empty?")

    assert "add" in captured_context[0]
    assert "result" in captured_context[0]
    assert "Why is this table empty?" in captured_prompts[0]


async def test_explain_lineage_sample_error_does_not_raise():
    """sample_table() errors are swallowed so explain_lineage() still returns an answer."""
    graph = _mock_graph(make_oplog_node("result", "copy"))

    async def failing_sample(*args, **kwargs):
        raise RuntimeError("Table not found")

    with (
        patch("aaiclick.ai.agents.lineage_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.lineage_agent.sample_table", new=failing_sample),
        patch("aaiclick.ai.agents.lineage_agent.get_ai_provider", return_value=_mock_provider("ok")),
        patch("aaiclick.ai.agents.lineage_agent.get_schemas_for_nodes", new=AsyncMock(return_value="")),
    ):
        result = await explain_lineage("result")

    assert result == "ok"


async def test_explain_lineage_samples_each_node():
    """sample_table() is called once per unique node in the lineage graph."""
    graph = _mock_graph(
        make_oplog_node("result", "concat", {"source_0": "a", "source_1": "b"}),
        make_oplog_node("a", "create_from_value"),
    )
    sampled_tables: list[str] = []

    async def mock_sample(table, limit=3):
        sampled_tables.append(table)
        return f"sample_{table}"

    with (
        patch("aaiclick.ai.agents.lineage_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.lineage_agent.sample_table", new=mock_sample),
        patch("aaiclick.ai.agents.lineage_agent.get_ai_provider", return_value=_mock_provider()),
        patch("aaiclick.ai.agents.lineage_agent.get_schemas_for_nodes", new=AsyncMock(return_value="")),
    ):
        await explain_lineage("result")

    assert set(sampled_tables) == {"result", "a"}


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
        patch("aaiclick.ai.agents.lineage_agent.sample_table", new=AsyncMock(return_value="sample")),
        patch("aaiclick.ai.agents.lineage_agent.get_ai_provider", return_value=mock_provider),
        patch("aaiclick.ai.agents.lineage_agent.get_schemas_for_nodes", new=AsyncMock(return_value=schema_text)),
    ):
        await explain_lineage("result")

    assert "Table Schemas" in captured_context[0]
    assert "val: Float64" in captured_context[0]
