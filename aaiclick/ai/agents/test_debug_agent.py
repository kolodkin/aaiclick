"""
Tests for debug_result — mocks backward_oplog, acompletion, and dispatch_tool.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from aaiclick.ai.agents.debug_agent import debug_result
from aaiclick.oplog.lineage import OplogNode


def _node(table: str, operation: str) -> OplogNode:
    return OplogNode(
        table=table,
        operation=operation,
        args=[],
        kwargs={},
        sql_template=None,
        task_id=None,
        job_id=None,
    )


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


async def test_debug_result_direct_answer():
    """Model answers without tools: backward_oplog is called and result contains the AI answer."""
    nodes = [_node("result", "add")]
    mock_backward = AsyncMock(return_value=nodes)
    captured: list[list] = []

    async def mock_completion(**kwargs):
        captured.append(kwargs["messages"])
        return _stop_response("Because input was negative")

    with (
        patch("aaiclick.ai.agents.debug_agent.backward_oplog", new=mock_backward),
        patch("aaiclick.ai.agents.debug_agent.acompletion", new=mock_completion),
        patch(
            "aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=MagicMock(model="test/model", _api_key=None)
        ),
    ):
        result = await debug_result("result", "Why is this value negative?")

    assert result == "Because input was negative"
    mock_backward.assert_called_once_with("result")
    all_content = " ".join(str(m.get("content") or "") for m in captured[0])
    assert "Why is this value negative?" in all_content


async def test_debug_result_with_one_tool_call():
    """Tool call loop: model calls sample_table, receives result, then gives final answer."""
    nodes = [_node("result", "filter")]
    tool_resp = _tool_response("sample_table", '{"table": "result"}')
    final_resp = _stop_response("After sampling: 3 rows found")

    with (
        patch("aaiclick.ai.agents.debug_agent.backward_oplog", new=AsyncMock(return_value=nodes)),
        patch("aaiclick.ai.agents.debug_agent.acompletion", new=AsyncMock(side_effect=[tool_resp, final_resp])),
        patch("aaiclick.ai.agents.debug_agent.dispatch_tool", new=AsyncMock(return_value="id | val\n1 | x")),
        patch(
            "aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=MagicMock(model="test/model", _api_key=None)
        ),
    ):
        result = await debug_result("result", "Why are there only 3 rows?")

    assert "3 rows" in result


async def test_debug_result_dispatches_correct_tool():
    """dispatch_tool() is called with the exact tool name and parsed arguments from the model."""
    nodes = [_node("result", "add")]
    tool_resp = _tool_response("get_schema", '{"table": "result"}')
    final_resp = _stop_response("Schema analysis done")

    mock_dispatch = AsyncMock(return_value="id: UInt64\nval: Float64")

    with (
        patch("aaiclick.ai.agents.debug_agent.backward_oplog", new=AsyncMock(return_value=nodes)),
        patch("aaiclick.ai.agents.debug_agent.acompletion", new=AsyncMock(side_effect=[tool_resp, final_resp])),
        patch("aaiclick.ai.agents.debug_agent.dispatch_tool", new=mock_dispatch),
        patch(
            "aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=MagicMock(model="test/model", _api_key=None)
        ),
    ):
        await debug_result("result", "What is the schema?")

    mock_dispatch.assert_called_once_with("get_schema", {"table": "result"})
