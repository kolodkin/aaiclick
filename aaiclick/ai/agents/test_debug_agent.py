"""
Tests for debug_result — mocks oplog_subgraph, provider.complete, and dispatch_tool.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from aaiclick.conftest import make_oplog_node
from aaiclick.oplog.lineage import OplogGraph, OplogNode
from aaiclick.ai.agents.debug_agent import debug_result


def _mock_provider(*responses):
    """Create a mock AIProvider whose complete() returns responses in order."""
    provider = MagicMock()
    if len(responses) == 1:
        provider.complete = AsyncMock(return_value=responses[0])
    else:
        provider.complete = AsyncMock(side_effect=list(responses))
    return provider


def _stop_response(content: str) -> MagicMock:
    resp = MagicMock()
    resp.choices[0].message.content = content
    resp.choices[0].message.tool_calls = None
    resp.choices[0].finish_reason = "stop"
    return resp


def _tool_response(tool_name: str, tool_args: str, tool_id: str = "call_1") -> MagicMock:
    tc = MagicMock()
    tc.id = tool_id
    tc.function.name = tool_name
    tc.function.arguments = tool_args
    resp = MagicMock()
    resp.choices[0].message.content = None
    resp.choices[0].message.tool_calls = [tc]
    resp.choices[0].finish_reason = "tool_calls"
    return resp


def _mock_graph(*nodes):
    return OplogGraph(nodes=list(nodes), edges=[])


async def test_debug_result_direct_answer():
    """Model answers without tools: oplog_subgraph is called and result contains the AI answer."""
    graph = _mock_graph(make_oplog_node("result", "add"))
    mock_subgraph = AsyncMock(return_value=graph)
    provider = _mock_provider(_stop_response("Because input was negative"))

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=mock_subgraph),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=provider),
        patch("aaiclick.ai.agents.debug_agent.get_schemas_for_nodes", new=AsyncMock(return_value="")),
        patch("aaiclick.ai.agents.debug_agent.produce_strategy", new=AsyncMock(return_value={})),
    ):
        result = await debug_result("result", "Why is this value negative?")

    assert result == "Because input was negative"
    mock_subgraph.assert_called_once_with("result", direction="backward")
    messages = provider.complete.call_args[0][0]
    all_content = " ".join(str(m.get("content") or "") for m in messages)
    assert "Why is this value negative?" in all_content


async def test_debug_result_with_one_tool_call():
    """Tool call loop: model calls sample_table, receives result, then gives final answer."""
    graph = _mock_graph(make_oplog_node("result", "filter"))
    tool_resp = _tool_response("sample_table", '{"table": "result"}')
    final_resp = _stop_response("After sampling: 3 rows found")

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.dispatch_tool", new=AsyncMock(return_value="id | val\n1 | x")),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=_mock_provider(tool_resp, final_resp)),
        patch("aaiclick.ai.agents.debug_agent.get_schemas_for_nodes", new=AsyncMock(return_value="")),
        patch("aaiclick.ai.agents.debug_agent.produce_strategy", new=AsyncMock(return_value={})),
    ):
        result = await debug_result("result", "Why are there only 3 rows?")

    assert "3 rows" in result


async def test_debug_result_dispatches_correct_tool():
    """dispatch_tool() is called with the exact tool name and parsed arguments from the model."""
    graph = _mock_graph(make_oplog_node("result", "add"))
    tool_resp = _tool_response("get_schema", '{"table": "result"}')
    final_resp = _stop_response("Schema analysis done")

    mock_dispatch = AsyncMock(return_value="id: UInt64\nval: Float64")

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.dispatch_tool", new=mock_dispatch),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=_mock_provider(tool_resp, final_resp)),
        patch("aaiclick.ai.agents.debug_agent.get_schemas_for_nodes", new=AsyncMock(return_value="")),
        patch("aaiclick.ai.agents.debug_agent.produce_strategy", new=AsyncMock(return_value={})),
    ):
        await debug_result("result", "What is the schema?")

    mock_dispatch.assert_called_once_with("get_schema", {"table": "result"})


async def test_debug_result_replays_when_strategy_is_non_empty():
    """Non-empty strategy → replay_and_trace runs and its output lands in context."""
    target_node = OplogNode(
        table="result",
        operation="add",
        kwargs={},
        kwargs_aai_ids={},
        result_aai_ids=[],
        sql_template=None,
        task_id=42,
        job_id=99,
    )
    graph = _mock_graph(target_node)
    strategy = {"result": "value < 0"}
    provider = _mock_provider(_stop_response("done"))
    replay_trace = AsyncMock(return_value="result.aai_id=7  <- add(left=1, right=2)")

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=provider),
        patch("aaiclick.ai.agents.debug_agent.get_schemas_for_nodes", new=AsyncMock(return_value="")),
        patch("aaiclick.ai.agents.debug_agent.produce_strategy", new=AsyncMock(return_value=strategy)),
        patch("aaiclick.ai.agents.debug_agent._replay_and_trace", new=replay_trace),
    ):
        await debug_result("result", "Why negative?")

    replay_trace.assert_awaited_once_with("result", graph, strategy)
    user_msg = provider.complete.call_args[0][0][1]["content"]
    assert "Row-level lineage (from strategy replay)" in user_msg
    assert "result.aai_id=7" in user_msg


async def test_debug_result_skips_replay_when_strategy_is_empty():
    """Empty strategy → replay_and_trace is never called."""
    graph = _mock_graph(make_oplog_node("result", "add"))
    provider = _mock_provider(_stop_response("done"))
    replay_trace = AsyncMock(return_value="should not be used")

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=provider),
        patch("aaiclick.ai.agents.debug_agent.get_schemas_for_nodes", new=AsyncMock(return_value="")),
        patch("aaiclick.ai.agents.debug_agent.produce_strategy", new=AsyncMock(return_value={})),
        patch("aaiclick.ai.agents.debug_agent._replay_and_trace", new=replay_trace),
    ):
        await debug_result("result", "Why?")

    replay_trace.assert_not_awaited()
    user_msg = provider.complete.call_args[0][0][1]["content"]
    assert "Row-level lineage" not in user_msg


async def test_debug_result_context_includes_schemas():
    """When schemas are available, they appear in the context sent to the LLM."""
    graph = _mock_graph(make_oplog_node("result", "add"))
    schema_text = "# Table Schemas\n\n`result`:\n  aai_id: UInt64\n  val: Float64"
    provider = _mock_provider(_stop_response("done"))

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=provider),
        patch("aaiclick.ai.agents.debug_agent.get_schemas_for_nodes", new=AsyncMock(return_value=schema_text)),
        patch("aaiclick.ai.agents.debug_agent.produce_strategy", new=AsyncMock(return_value={})),
    ):
        await debug_result("result", "Why?")

    messages = provider.complete.call_args[0][0]
    user_msg = messages[1]["content"]
    assert "Table Schemas" in user_msg
    assert "val: Float64" in user_msg
