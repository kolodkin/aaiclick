"""
Tests for AIProvider — mocks litellm.acompletion throughout.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from aaiclick.ai.provider import AIProvider


def _mock_response(content: str) -> MagicMock:
    resp = MagicMock()
    resp.choices[0].message.content = content
    resp.choices[0].message.tool_calls = None
    resp.choices[0].finish_reason = "stop"
    return resp


def _mock_tool_response(tool_name: str, tool_args: str, tool_id: str = "call_1") -> MagicMock:
    tc = MagicMock()
    tc.id = tool_id
    tc.function.name = tool_name
    tc.function.arguments = tool_args
    resp = MagicMock()
    resp.choices[0].message.content = None
    resp.choices[0].message.tool_calls = [tc]
    resp.choices[0].finish_reason = "tool_calls"
    return resp


async def test_query_returns_content_and_sends_prompt_directly():
    provider = AIProvider(model="test/model")
    captured: list[list] = []

    async def mock_completion(**kwargs):
        captured.append(kwargs["messages"])
        return _mock_response("Answer")

    with patch("aaiclick.ai.provider.acompletion", new=mock_completion):
        result = await provider.query("Direct question?")

    assert result == "Answer"
    assert len(captured[0]) == 1
    assert captured[0][0]["content"] == "Direct question?"


async def test_query_with_context_prepends_context():
    provider = AIProvider(model="test/model")
    captured: list[list] = []

    async def mock_completion(**kwargs):
        captured.append(kwargs["messages"])
        return _mock_response("ok")

    with patch("aaiclick.ai.provider.acompletion", new=mock_completion):
        await provider.query("Question?", context="Some context")

    user_content = captured[0][-1]["content"]
    assert "Some context" in user_content
    assert "Question?" in user_content


async def test_query_with_system_adds_system_message():
    provider = AIProvider(model="test/model")
    captured: list[list] = []

    async def mock_completion(**kwargs):
        captured.append(kwargs["messages"])
        return _mock_response("ok")

    with patch("aaiclick.ai.provider.acompletion", new=mock_completion):
        await provider.query("Q?", system="Be helpful")

    assert captured[0][0]["role"] == "system"
    assert captured[0][0]["content"] == "Be helpful"


async def test_query_with_tools_returns_tool_call_dict():
    provider = AIProvider(model="test/model")
    resp = _mock_tool_response("sample_table", '{"table": "t1"}')
    with patch("aaiclick.ai.provider.acompletion", new=AsyncMock(return_value=resp)):
        result = await provider.query_with_tools("Inspect table", tools=[])

    assert result["finish_reason"] == "tool_calls"
    assert len(result["tool_calls"]) == 1
    assert result["tool_calls"][0]["name"] == "sample_table"
    assert result["tool_calls"][0]["arguments"] == {"table": "t1"}


async def test_query_with_tools_stop_returns_content():
    provider = AIProvider(model="test/model")
    with patch("aaiclick.ai.provider.acompletion", new=AsyncMock(return_value=_mock_response("Final"))):
        result = await provider.query_with_tools("Question", tools=[])

    assert result["content"] == "Final"
    assert result["tool_calls"] == []
    assert result["finish_reason"] == "stop"


async def test_query_passes_api_key():
    provider = AIProvider(model="test/model", api_key="sk-test")
    captured_kwargs: list[dict] = []

    async def mock_completion(**kwargs):
        captured_kwargs.append(kwargs)
        return _mock_response("ok")

    with patch("aaiclick.ai.provider.acompletion", new=mock_completion):
        await provider.query("Q?")

    assert captured_kwargs[0].get("api_key") == "sk-test"
