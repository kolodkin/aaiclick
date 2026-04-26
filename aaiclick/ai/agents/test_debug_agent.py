"""
Tests for debug_result — the Tier 1 lineage tool loop.

Mocks oplog_subgraph, the AI provider, and LineageToolbox so tests exercise
the loop contract without hitting ClickHouse or an LLM.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from aaiclick.ai.agents.debug_agent import debug_result
from aaiclick.ai.agents.lineage_tools import GraphNode
from aaiclick.oplog.lineage import OplogGraph
from aaiclick.testing import make_oplog_node

TARGET = "t_22222222222222222222"
INPUT = "p_sales"


def _mock_provider(*responses):
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


def _mock_toolbox(list_nodes=None, dispatch_side_effect=None):
    """Patch LineageToolbox with a MagicMock whose list_graph_nodes / dispatch_tool are AsyncMocks."""
    toolbox = MagicMock()
    toolbox.list_graph_nodes = AsyncMock(
        return_value=list_nodes
        or [GraphNode(table=TARGET, kind="target", operation="filter", live=True, task_id=1, job_id=1)]
    )
    toolbox.dispatch_tool = AsyncMock(side_effect=dispatch_side_effect or ["tool-result"])
    return toolbox


async def test_debug_result_direct_answer():
    """Model answers without tool calls: returns the model content verbatim (after label replacement)."""
    graph = _mock_graph(make_oplog_node(TARGET, "filter", {"input": INPUT}))
    provider = _mock_provider(_stop_response("Because the filter removed negatives."))

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=provider),
        patch("aaiclick.ai.agents.debug_agent.LineageToolbox", return_value=_mock_toolbox()),
    ):
        result = await debug_result(TARGET, "Why is row count low?")

    assert result == "Because the filter removed negatives."


async def test_debug_result_invokes_lineage_tool():
    """Tool call loop: model calls query_table, receives result, then gives final answer."""
    graph = _mock_graph(make_oplog_node(TARGET, "filter", {"input": INPUT}))
    tool_resp = _tool_response("query_table", f'{{"sql": "SELECT count() FROM {TARGET}"}}')
    final_resp = _stop_response("3 rows remain after filter.")

    toolbox = _mock_toolbox(dispatch_side_effect=["count\n3"])
    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=_mock_provider(tool_resp, final_resp)),
        patch("aaiclick.ai.agents.debug_agent.LineageToolbox", return_value=toolbox),
    ):
        result = await debug_result(TARGET, "Why so few rows?")

    assert "3 rows" in result
    toolbox.dispatch_tool.assert_awaited_once_with("query_table", {"sql": f"SELECT count() FROM {TARGET}"})


async def test_debug_result_dispatches_tool_with_parsed_arguments():
    """The loop parses JSON arguments before calling dispatch_tool."""
    graph = _mock_graph(make_oplog_node(TARGET, "filter", {"input": INPUT}))
    tool_resp = _tool_response("get_schema", f'{{"table": "{TARGET}"}}')
    final_resp = _stop_response("Schema analyzed.")
    toolbox = _mock_toolbox(dispatch_side_effect=["id: UInt64\nval: Float64"])

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=_mock_provider(tool_resp, final_resp)),
        patch("aaiclick.ai.agents.debug_agent.LineageToolbox", return_value=toolbox),
    ):
        await debug_result(TARGET, "What's the schema?")

    toolbox.dispatch_tool.assert_awaited_once_with("get_schema", {"table": TARGET})


async def test_debug_result_handles_empty_arguments_for_zero_arg_tool():
    """A zero-arg tool like list_graph_nodes is dispatched with {} when arguments are empty."""
    graph = _mock_graph(make_oplog_node(TARGET, "filter", {"input": INPUT}))
    tool_resp = _tool_response("list_graph_nodes", "")
    final_resp = _stop_response("Done.")
    toolbox = _mock_toolbox(dispatch_side_effect=["- t_... [target]"])

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=_mock_provider(tool_resp, final_resp)),
        patch("aaiclick.ai.agents.debug_agent.LineageToolbox", return_value=toolbox),
    ):
        await debug_result(TARGET, "What's in the graph?")

    toolbox.dispatch_tool.assert_awaited_once_with("list_graph_nodes", {})


async def test_debug_result_context_includes_liveness():
    """The liveness of each graph node is surfaced in the user context so the agent
    knows which tables to query versus which require escalation."""
    graph = _mock_graph(make_oplog_node(TARGET, "filter", {"input": INPUT}))
    provider = _mock_provider(_stop_response("ok"))
    toolbox = _mock_toolbox(
        list_nodes=[
            GraphNode(table=INPUT, kind="input", operation="(input)", live=True, task_id=None, job_id=None),
            GraphNode(table=TARGET, kind="target", operation="filter", live=False, task_id=1, job_id=1),
        ]
    )

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=provider),
        patch("aaiclick.ai.agents.debug_agent.LineageToolbox", return_value=toolbox),
    ):
        await debug_result(TARGET, "Why?")

    user_msg = provider.complete.call_args[0][0][1]["content"]
    assert "Graph Node Liveness" in user_msg
    assert f"{INPUT} [input] live=True" in user_msg
    assert f"{TARGET} [target] live=False" in user_msg


async def test_debug_result_respects_max_iterations():
    """A model that keeps calling tools indefinitely is cut off after max_iterations."""
    graph = _mock_graph(make_oplog_node(TARGET, "filter", {"input": INPUT}))
    tool_resp = _tool_response("query_table", f'{{"sql": "SELECT 1 FROM {TARGET}"}}')
    final_resp = _stop_response("forced-final")

    max_iterations = 3
    toolbox = _mock_toolbox(dispatch_side_effect=["r1", "r2", "r3"])
    provider = _mock_provider(tool_resp, tool_resp, tool_resp, final_resp)

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=provider),
        patch("aaiclick.ai.agents.debug_agent.LineageToolbox", return_value=toolbox),
    ):
        result = await debug_result(TARGET, "loop forever?", max_iterations=max_iterations)

    assert result == "forced-final"
    assert provider.complete.await_count == max_iterations + 1
    assert toolbox.dispatch_tool.await_count == max_iterations


async def test_debug_result_provider_failure_returns_graceful_message():
    """A provider exception (e.g. LLM timeout) is caught and surfaced as a string
    instead of crashing the caller."""
    graph = _mock_graph(make_oplog_node(TARGET, "filter", {"input": INPUT}))
    provider = MagicMock()
    provider.complete = AsyncMock(side_effect=RuntimeError("upstream timeout"))

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=provider),
        patch("aaiclick.ai.agents.debug_agent.LineageToolbox", return_value=_mock_toolbox()),
    ):
        result = await debug_result(TARGET, "Why?")

    assert "did not converge" in result
    assert "upstream timeout" in result


async def test_debug_result_recovers_inline_tool_call_in_content():
    """Some Ollama models emit a tool call as JSON text in `content` instead of
    the structured `tool_calls` field. The loop must parse and dispatch it
    instead of returning the JSON as a final answer.
    """
    graph = _mock_graph(make_oplog_node(TARGET, "filter", {"input": INPUT}))
    inline_resp = _stop_response(
        '{"id": "call_inline", "type": "function", "function": '
        '{"name": "get_schema", "arguments": {"table": "' + TARGET + '"}}}'
    )
    final_resp = _stop_response("Schema inspected.")
    toolbox = _mock_toolbox(dispatch_side_effect=["id: UInt64\nval: Float64"])

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=_mock_provider(inline_resp, final_resp)),
        patch("aaiclick.ai.agents.debug_agent.LineageToolbox", return_value=toolbox),
    ):
        result = await debug_result(TARGET, "Schema?")

    assert result == "Schema inspected."
    toolbox.dispatch_tool.assert_awaited_once_with("get_schema", {"table": TARGET})


async def test_debug_result_inline_tool_calls_list_form():
    """A model may emit multiple inline tool calls as a JSON array in `content`."""
    graph = _mock_graph(make_oplog_node(TARGET, "filter", {"input": INPUT}))
    inline_resp = _stop_response(
        '[{"function": {"name": "get_schema", "arguments": {"table": "' + TARGET + '"}}},'
        ' {"function": {"name": "list_graph_nodes", "arguments": {}}}]'
    )
    final_resp = _stop_response("done")
    toolbox = _mock_toolbox(dispatch_side_effect=["schema-result", "nodes-result"])

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=_mock_provider(inline_resp, final_resp)),
        patch("aaiclick.ai.agents.debug_agent.LineageToolbox", return_value=toolbox),
    ):
        await debug_result(TARGET, "Inspect.")

    assert toolbox.dispatch_tool.await_count == 2


async def test_debug_result_plain_text_content_is_final_answer():
    """Content that isn't JSON or doesn't match the tool-call shape is returned
    verbatim as the final answer — the inline-parser fallback must not over-trigger.
    """
    graph = _mock_graph(make_oplog_node(TARGET, "filter", {"input": INPUT}))
    json_but_not_a_tool_call = _stop_response('{"answer": "42"}')

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=_mock_provider(json_but_not_a_tool_call)),
        patch("aaiclick.ai.agents.debug_agent.LineageToolbox", return_value=_mock_toolbox()),
    ):
        result = await debug_result(TARGET, "Why?")

    assert result == '{"answer": "42"}'


async def test_debug_result_with_prebuilt_graph_skips_subgraph():
    """Passing graph= avoids the backward_oplog traversal."""
    graph = _mock_graph(make_oplog_node(TARGET, "filter", {"input": INPUT}))
    mock_subgraph = AsyncMock(return_value=graph)

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=mock_subgraph),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=_mock_provider(_stop_response("ok"))),
        patch("aaiclick.ai.agents.debug_agent.LineageToolbox", return_value=_mock_toolbox()),
    ):
        result = await debug_result(TARGET, "Why?", graph=graph)

    assert result == "ok"
    mock_subgraph.assert_not_called()
