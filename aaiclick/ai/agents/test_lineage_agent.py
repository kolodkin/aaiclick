"""
Tests for explain_lineage — mocks backward_oplog, sample_table, and AIProvider.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from aaiclick.oplog.lineage import OplogNode
from aaiclick.ai.agents.lineage_agent import explain_lineage


def _node(table: str, operation: str, args: list[str] | None = None) -> OplogNode:
    return OplogNode(
        table=table,
        operation=operation,
        args=args or [],
        kwargs={},
        sql_template=None,
        task_id=None,
        job_id=None,
    )


def _mock_provider(answer: str = "Explanation") -> MagicMock:
    provider = MagicMock()
    provider.query = AsyncMock(return_value=answer)
    return provider


async def test_explain_lineage_returns_string_and_calls_backward_oplog():
    """explain_lineage() returns the AI answer and calls backward_oplog with the target table."""
    nodes = [_node("result", "add", ["a", "b"])]
    mock_backward = AsyncMock(return_value=nodes)

    with (
        patch("aaiclick.ai.agents.lineage_agent.backward_oplog", new=mock_backward),
        patch("aaiclick.ai.agents.lineage_agent.sample_table", new=AsyncMock(return_value="c1\nv1")),
        patch("aaiclick.ai.agents.lineage_agent.get_ai_provider", return_value=_mock_provider("Result")),
    ):
        result = await explain_lineage("result")

    assert result == "Result"
    mock_backward.assert_called_once_with("result")


async def test_explain_lineage_context_and_custom_question():
    """lineage graph is passed as context; custom question= overrides the default prompt."""
    nodes = [_node("result", "add")]
    captured_context: list[str] = []
    captured_prompts: list[str] = []

    async def mock_query(prompt, context="", system=""):
        captured_context.append(context)
        captured_prompts.append(prompt)
        return "ok"

    mock_provider = MagicMock()
    mock_provider.query = mock_query

    with (
        patch("aaiclick.ai.agents.lineage_agent.backward_oplog", new=AsyncMock(return_value=nodes)),
        patch("aaiclick.ai.agents.lineage_agent.sample_table", new=AsyncMock(return_value="sample")),
        patch("aaiclick.ai.agents.lineage_agent.get_ai_provider", return_value=mock_provider),
    ):
        await explain_lineage("result", question="Why is this table empty?")

    assert "add" in captured_context[0]
    assert "result" in captured_context[0]
    assert "Why is this table empty?" in captured_prompts[0]


async def test_explain_lineage_sample_error_does_not_raise():
    """sample_table() errors are swallowed so explain_lineage() still returns an answer."""
    nodes = [_node("result", "copy")]

    async def failing_sample(*args, **kwargs):
        raise RuntimeError("Table not found")

    with (
        patch("aaiclick.ai.agents.lineage_agent.backward_oplog", new=AsyncMock(return_value=nodes)),
        patch("aaiclick.ai.agents.lineage_agent.sample_table", new=failing_sample),
        patch("aaiclick.ai.agents.lineage_agent.get_ai_provider", return_value=_mock_provider("ok")),
    ):
        result = await explain_lineage("result")

    assert result == "ok"


async def test_explain_lineage_samples_each_node():
    """sample_table() is called once per unique node in the lineage graph."""
    nodes = [_node("result", "concat", ["a", "b"]), _node("a", "create_from_value")]
    sampled_tables: list[str] = []

    async def mock_sample(table, limit=3):
        sampled_tables.append(table)
        return f"sample_{table}"

    with (
        patch("aaiclick.ai.agents.lineage_agent.backward_oplog", new=AsyncMock(return_value=nodes)),
        patch("aaiclick.ai.agents.lineage_agent.sample_table", new=mock_sample),
        patch("aaiclick.ai.agents.lineage_agent.get_ai_provider", return_value=_mock_provider()),
    ):
        await explain_lineage("result")

    assert set(sampled_tables) == {"result", "a"}
