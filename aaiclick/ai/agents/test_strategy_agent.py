"""
Tests for ``produce_strategy`` — mocks provider, schema lookup, and CH dry-run.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from aaiclick.ai.agents.strategy_agent import format_strategy, produce_strategy
from aaiclick.conftest import make_oplog_node
from aaiclick.oplog.lineage import OplogGraph


def _graph(*nodes) -> OplogGraph:
    return OplogGraph(nodes=list(nodes), edges=[])


def _mock_provider(*responses: str):
    provider = AsyncMock()
    if len(responses) == 1:
        provider.query = AsyncMock(return_value=responses[0])
    else:
        provider.query = AsyncMock(side_effect=list(responses))
    return provider


def _passing_ch_client():
    """CH client whose LIMIT 0 query always succeeds (returns an empty row set)."""
    ch = AsyncMock()
    ch.query = AsyncMock(return_value=AsyncMock(result_rows=[]))
    return ch


async def _run(raw_outputs, graph, question="Why?", ch_client=None):
    provider = _mock_provider(*raw_outputs)
    ch = ch_client or _passing_ch_client()
    with (
        patch("aaiclick.ai.agents.strategy_agent.get_ai_provider", return_value=provider),
        patch(
            "aaiclick.ai.agents.strategy_agent.get_schemas_for_nodes",
            new=AsyncMock(return_value=""),
        ),
        patch("aaiclick.ai.agents.strategy_agent.get_ch_client", return_value=ch),
    ):
        return await produce_strategy(question, graph), provider


async def test_well_formed_json():
    graph = _graph(
        make_oplog_node("t_scores", "filter", {"source": "p_raw"}),
    )
    strategy, _ = await _run(['{"p_raw": "cvss < 0"}'], graph)
    assert strategy == {"p_raw": "cvss < 0"}


async def test_empty_strategy_is_valid():
    graph = _graph(make_oplog_node("result", "add"))
    strategy, _ = await _run(["{}"], graph)
    assert strategy == {}


async def test_strips_markdown_fences():
    graph = _graph(
        make_oplog_node("t_out", "map", {"source": "p_in"}),
    )
    fenced = '```json\n{"p_in": "x = 1"}\n```'
    strategy, _ = await _run([fenced], graph)
    assert strategy == {"p_in": "x = 1"}


async def test_retries_on_malformed_json():
    graph = _graph(make_oplog_node("result", "add", {"source": "p_in"}))
    strategy, provider = await _run(
        ["not-json-at-all", '{"p_in": "id = 42"}'],
        graph,
    )
    assert strategy == {"p_in": "id = 42"}
    assert provider.query.call_count == 2


async def test_rejects_unknown_table():
    graph = _graph(make_oplog_node("result", "add", {"source": "p_in"}))
    with pytest.raises(ValueError, match="not in the lineage graph"):
        await _run(
            ['{"p_other": "x = 1"}', '{"p_other": "x = 1"}'],
            graph,
        )


async def test_rejects_non_object_output():
    graph = _graph(make_oplog_node("result", "add", {"source": "p_in"}))
    with pytest.raises(ValueError, match="JSON object"):
        await _run(
            ['["p_in", "x = 1"]', '["p_in", "x = 1"]'],
            graph,
        )


async def test_rejects_non_string_values():
    graph = _graph(make_oplog_node("result", "add", {"source": "p_in"}))
    with pytest.raises(ValueError, match="string→string"):
        await _run(
            ['{"p_in": 42}', '{"p_in": 42}'],
            graph,
        )


async def test_rejects_empty_clause():
    graph = _graph(make_oplog_node("result", "add", {"source": "p_in"}))
    with pytest.raises(ValueError, match="empty WHERE clause"):
        await _run(
            ['{"p_in": "   "}', '{"p_in": "   "}'],
            graph,
        )


async def test_multi_source_graph_all_keys_allowed():
    graph = _graph(
        make_oplog_node("t_merge", "concat", {"left": "p_a", "right": "p_b"}),
    )
    raw = '{"p_a": "id = 1", "p_b": "id = 2", "t_merge": "id IN (1, 2)"}'
    strategy, _ = await _run([raw], graph)
    assert strategy == {
        "p_a": "id = 1",
        "p_b": "id = 2",
        "t_merge": "id IN (1, 2)",
    }


async def test_dry_run_catches_sql_errors():
    graph = _graph(make_oplog_node("result", "add", {"source": "p_in"}))
    ch_client = AsyncMock()
    ch_client.query = AsyncMock(side_effect=RuntimeError("syntax error near 'x'"))
    with pytest.raises(ValueError, match="failed validation"):
        await _run(
            ['{"p_in": "invalid ==="}', '{"p_in": "invalid ==="}'],
            graph,
            ch_client=ch_client,
        )


async def test_dry_run_accepts_valid_clauses():
    graph = _graph(make_oplog_node("result", "add", {"source": "p_in"}))
    ch_client = _passing_ch_client()
    strategy, _ = await _run(
        ['{"p_in": "id = 1"}'],
        graph,
        ch_client=ch_client,
    )
    assert strategy == {"p_in": "id = 1"}
    assert "LIMIT 0" in ch_client.query.call_args[0][0]


async def test_dry_run_parallelizes_queries():
    graph = _graph(
        make_oplog_node("t_merge", "concat", {"left": "p_a", "right": "p_b"}),
    )
    ch_client = _passing_ch_client()
    await _run(
        ['{"p_a": "id = 1", "p_b": "id = 2"}'],
        graph,
        ch_client=ch_client,
    )
    # Both entries dry-run — gather should have dispatched exactly 2 queries.
    assert ch_client.query.call_count == 2


def test_format_strategy_empty():
    assert format_strategy({}) == ""


def test_format_strategy_populated():
    out = format_strategy({"p_in": "x = 1", "t_out": "y IS NULL"})
    assert "p_in: x = 1" in out
    assert "t_out: y IS NULL" in out
    assert out.startswith("\n\nSampling strategy")
