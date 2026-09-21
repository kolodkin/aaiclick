"""
Tests for the AI-independent lineage internal_api primitives.

The underlying ``oplog_subgraph`` and the SQL-safety / scope helpers have
their own test modules; here we only assert the wrappers correctly delegate,
pass kwargs through, and translate ``ToolError`` results into ``Invalid`` /
``NotFound`` exceptions.

Tests for the AI-backed wrappers (``explain_lineage`` / ``debug_result``)
live in ``aaiclick/ai/agents/test_lineage_internal_api.py`` so they only
run in matrices that install the ``ai`` extra.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from aaiclick.ai.agents.lineage_tools import ColumnSchema, QueryResult, TableSchema
from aaiclick.internal_api import lineage as lineage_api
from aaiclick.internal_api.errors import Invalid, NotFound
from aaiclick.oplog.lineage import OplogGraph
from aaiclick.testing import make_oplog_node


async def test_oplog_subgraph_returns_graph_and_passes_kwargs():
    graph = OplogGraph(nodes=[make_oplog_node("result", "add")], edges=[])
    mock_subgraph = AsyncMock(return_value=graph)

    with patch("aaiclick.internal_api.lineage._oplog_subgraph", new=mock_subgraph):
        result = await lineage_api.oplog_subgraph("result", direction="forward", max_depth=3)

    assert result is graph
    mock_subgraph.assert_awaited_once_with("result", direction="forward", max_depth=3)


def _graph_of(*tables: str) -> OplogGraph:
    return OplogGraph(nodes=[make_oplog_node(t, "add") for t in tables], edges=[])


async def test_query_table_runs_validated_select(orch_ctx):
    qr = QueryResult(columns=["id"], rows=[[1], [2]], truncated=False)
    mock_run = AsyncMock(return_value=qr)
    mock_subgraph = AsyncMock(return_value=_graph_of("p_revenue"))

    with (
        patch("aaiclick.internal_api.lineage._oplog_subgraph", new=mock_subgraph),
        patch("aaiclick.internal_api.lineage.run_select", new=mock_run),
    ):
        result = await lineage_api.query_table(
            "SELECT id FROM p_revenue",
            target_table="p_revenue",
            row_limit=50,
            direction="forward",
            max_depth=3,
        )

    assert result is qr
    mock_subgraph.assert_awaited_once_with("p_revenue", direction="forward", max_depth=3)
    mock_run.assert_awaited_once_with("SELECT id FROM p_revenue", 50, scan="SELECT id FROM p_revenue")


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("DROP TABLE p_revenue", id="ddl"),
        pytest.param("SELECT * FROM p_secret", id="out-of-scope"),
    ],
)
async def test_query_table_raises_invalid(orch_ctx, sql):
    """The scope is the target's lineage, looked up here — a caller cannot
    widen it by naming extra tables."""
    with patch("aaiclick.internal_api.lineage._oplog_subgraph", new=AsyncMock(return_value=_graph_of("p_revenue"))):
        with pytest.raises(Invalid):
            await lineage_api.query_table(sql, target_table="p_revenue")


async def test_query_table_without_lineage_raises_not_found(orch_ctx):
    """A target no operation produced has an empty graph, so nothing is in scope."""
    with patch("aaiclick.internal_api.lineage._oplog_subgraph", new=AsyncMock(return_value=_graph_of())):
        with pytest.raises(NotFound):
            await lineage_api.query_table("SELECT 1", target_table="p_nowhere")


async def test_get_table_schema_returns_describe_result():
    schema = TableSchema(table="p_revenue", columns=[ColumnSchema(name="id", type="UInt64")])
    mock_describe = AsyncMock(return_value=schema)

    with (
        patch("aaiclick.internal_api.lineage._oplog_subgraph", new=AsyncMock(return_value=_graph_of("p_revenue"))),
        patch("aaiclick.internal_api.lineage.describe_table", new=mock_describe),
    ):
        result = await lineage_api.get_table_schema("p_revenue", target_table="p_revenue")

    assert result is schema
    mock_describe.assert_awaited_once_with("p_revenue")


async def test_get_table_schema_raises_invalid_when_out_of_scope(orch_ctx):
    with patch("aaiclick.internal_api.lineage._oplog_subgraph", new=AsyncMock(return_value=_graph_of("p_revenue"))):
        with pytest.raises(Invalid):
            await lineage_api.get_table_schema("p_secret", target_table="p_revenue")


async def test_get_table_schema_raises_not_found_when_describe_fails(orch_ctx):
    mock_describe = AsyncMock(side_effect=RuntimeError("table dropped"))

    with (
        patch("aaiclick.internal_api.lineage._oplog_subgraph", new=AsyncMock(return_value=_graph_of("p_revenue"))),
        patch("aaiclick.internal_api.lineage.describe_table", new=mock_describe),
    ):
        with pytest.raises(NotFound):
            await lineage_api.get_table_schema("p_revenue", target_table="p_revenue")
