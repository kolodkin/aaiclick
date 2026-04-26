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


async def test_query_table_runs_validated_select():
    qr = QueryResult(columns=["id"], rows=[[1], [2]], truncated=False)
    mock_run = AsyncMock(return_value=qr)

    with patch("aaiclick.internal_api.lineage.run_select", new=mock_run):
        result = await lineage_api.query_table(
            "SELECT id FROM p_revenue",
            scope_tables=["p_revenue"],
            row_limit=50,
        )

    assert result is qr
    mock_run.assert_awaited_once_with("SELECT id FROM p_revenue", 50, scan="SELECT id FROM p_revenue")


async def test_query_table_raises_invalid_on_ddl():
    with pytest.raises(Invalid):
        await lineage_api.query_table("DROP TABLE p_revenue", scope_tables=["p_revenue"])


async def test_query_table_raises_invalid_on_out_of_scope():
    with pytest.raises(Invalid):
        await lineage_api.query_table(
            "SELECT * FROM p_secret",
            scope_tables=["p_revenue"],
        )


async def test_get_table_schema_returns_describe_result():
    schema = TableSchema(table="p_revenue", columns=[ColumnSchema(name="id", type="UInt64")])
    mock_describe = AsyncMock(return_value=schema)

    with patch("aaiclick.internal_api.lineage.describe_table", new=mock_describe):
        result = await lineage_api.get_table_schema("p_revenue", scope_tables=["p_revenue"])

    assert result is schema
    mock_describe.assert_awaited_once_with("p_revenue")


async def test_get_table_schema_raises_invalid_when_out_of_scope():
    with pytest.raises(Invalid):
        await lineage_api.get_table_schema("p_secret", scope_tables=["p_revenue"])


async def test_get_table_schema_raises_not_found_when_describe_fails():
    mock_describe = AsyncMock(side_effect=RuntimeError("table dropped"))

    with patch("aaiclick.internal_api.lineage.describe_table", new=mock_describe):
        with pytest.raises(NotFound):
            await lineage_api.get_table_schema("p_revenue", scope_tables=["p_revenue"])
