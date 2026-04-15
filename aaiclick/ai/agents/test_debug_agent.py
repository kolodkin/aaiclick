"""
Tests for debug_result — mocks oplog_subgraph, provider.complete, dispatch_tool,
and the forest helpers (_try_build_forest / _replay_and_build_forest).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from aaiclick.conftest import make_oplog_node
from aaiclick.oplog.lineage import OplogGraph, OplogNode
from aaiclick.ai.agents.debug_agent import _try_build_forest, debug_result


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


def _forest_patch(forest_text: str = ""):
    """Patch _try_build_forest so debug_result doesn't hit ClickHouse."""
    return patch(
        "aaiclick.ai.agents.debug_agent._try_build_forest",
        new=AsyncMock(return_value=forest_text),
    )


def _replay_patch(forest_text: str = ""):
    """Patch _replay_and_build_forest so debug_result doesn't run a replay."""
    return patch(
        "aaiclick.ai.agents.debug_agent._replay_and_build_forest",
        new=AsyncMock(return_value=forest_text),
    )


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
        _forest_patch(),
        _replay_patch(),
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
        _forest_patch(),
        _replay_patch(),
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
        _forest_patch(),
        _replay_patch(),
    ):
        await debug_result("result", "What is the schema?")

    mock_dispatch.assert_called_once_with("get_schema", {"table": "result"})


async def test_debug_result_uses_forest_from_existing_oplog():
    """When _try_build_forest returns text (STRATEGY-mode target), it lands in
    the LLM context, replay is skipped, and the agentic tool loop is
    short-circuited (single completion, no tools parameter)."""
    graph = _mock_graph(make_oplog_node("result", "add"))
    provider = _mock_provider(_stop_response("done"))
    forest_text = "## Row-Level Lineage (strategy-matched)\n\n- Unique routes: 2"
    replay_mock = AsyncMock(return_value="should-not-be-called")

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=provider),
        patch("aaiclick.ai.agents.debug_agent.get_schemas_for_nodes", new=AsyncMock(return_value="")),
        patch("aaiclick.ai.agents.debug_agent.produce_strategy", new=AsyncMock(return_value={"result": "val > 0"})),
        _forest_patch(forest_text),
        patch("aaiclick.ai.agents.debug_agent._replay_and_build_forest", new=replay_mock),
    ):
        result = await debug_result("result", "Why?")

    assert result == "done"
    replay_mock.assert_not_awaited()
    # Single completion call only — no tool loop
    assert provider.complete.await_count == 1
    # The short-circuit uses a positional-args call without tools
    call = provider.complete.call_args
    assert "tools" not in call.kwargs
    user_msg = call[0][0][1]["content"]
    assert "Row-Level Lineage (strategy-matched)" in user_msg
    assert "Unique routes: 2" in user_msg


async def test_debug_result_falls_back_to_replay_when_forest_empty():
    """When _try_build_forest returns empty AND a strategy was produced,
    debug_result falls through to _replay_and_build_forest."""
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
    replay_forest = AsyncMock(return_value="## Row-Level Lineage (strategy-matched)\n- from replay")

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=provider),
        patch("aaiclick.ai.agents.debug_agent.get_schemas_for_nodes", new=AsyncMock(return_value="")),
        patch("aaiclick.ai.agents.debug_agent.produce_strategy", new=AsyncMock(return_value=strategy)),
        _forest_patch(""),
        patch("aaiclick.ai.agents.debug_agent._replay_and_build_forest", new=replay_forest),
    ):
        await debug_result("result", "Why negative?")

    replay_forest.assert_awaited_once_with("result", graph, strategy)
    user_msg = provider.complete.call_args[0][0][1]["content"]
    assert "from replay" in user_msg


async def test_debug_result_skips_replay_when_strategy_is_empty():
    """Empty forest + empty strategy → replay is never called."""
    graph = _mock_graph(make_oplog_node("result", "add"))
    provider = _mock_provider(_stop_response("done"))
    replay_mock = AsyncMock(return_value="should-not-be-called")

    with (
        patch("aaiclick.ai.agents.debug_agent.oplog_subgraph", new=AsyncMock(return_value=graph)),
        patch("aaiclick.ai.agents.debug_agent.get_ai_provider", return_value=provider),
        patch("aaiclick.ai.agents.debug_agent.get_schemas_for_nodes", new=AsyncMock(return_value="")),
        patch("aaiclick.ai.agents.debug_agent.produce_strategy", new=AsyncMock(return_value={})),
        _forest_patch(""),
        patch("aaiclick.ai.agents.debug_agent._replay_and_build_forest", new=replay_mock),
    ):
        await debug_result("result", "Why?")

    replay_mock.assert_not_awaited()
    user_msg = provider.complete.call_args[0][0][1]["content"]
    assert "Row-Level Lineage" not in user_msg


async def test_try_build_forest_short_circuits_on_empty_result_aai_ids():
    """Target with empty result_aai_ids (NONE / FULL mode) short-circuits
    before touching the database."""
    target = OplogNode(
        table="t_result",
        operation="add",
        kwargs={},
        kwargs_aai_ids={},
        result_aai_ids=[],  # ← not a STRATEGY-mode target
        sql_template=None,
        task_id=1,
        job_id=2,
    )
    graph = OplogGraph(nodes=[target], edges=[])

    safe_build = AsyncMock(return_value="should-not-be-called")
    with patch("aaiclick.ai.agents.debug_agent._safe_build", new=safe_build):
        result = await _try_build_forest("t_result", graph)

    assert result == ""
    safe_build.assert_not_awaited()


async def test_try_build_forest_runs_when_result_aai_ids_populated():
    """Target with populated result_aai_ids forwards to _safe_build."""
    target = OplogNode(
        table="t_result",
        operation="add",
        kwargs={},
        kwargs_aai_ids={"left": [10, 20]},
        result_aai_ids=[100, 200],
        sql_template=None,
        task_id=1,
        job_id=2,
    )
    graph = OplogGraph(nodes=[target], edges=[])

    safe_build = AsyncMock(return_value="rendered forest")
    with patch("aaiclick.ai.agents.debug_agent._safe_build", new=safe_build):
        result = await _try_build_forest("t_result", graph)

    assert result == "rendered forest"
    safe_build.assert_awaited_once_with("forest build", "t_result", 2)


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
        _forest_patch(),
        _replay_patch(),
    ):
        await debug_result("result", "Why?")

    messages = provider.complete.call_args[0][0]
    user_msg = messages[1]["content"]
    assert "Table Schemas" in user_msg
    assert "val: Float64" in user_msg
