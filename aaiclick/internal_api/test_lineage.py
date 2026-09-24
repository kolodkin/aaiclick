"""
Tests for the AI-independent lineage internal_api primitives.

The underlying ``oplog_subgraph`` and the SQL-safety / scope helpers have
their own test modules; here we run each wrapper against real tables and
their recorded lineage, and assert it translates ``ToolError`` results into
``Invalid`` / ``NotFound`` exceptions.

Tests for the AI-backed wrappers (``explain_lineage`` / ``debug_result``)
live in ``aaiclick/ai/agents/test_lineage_internal_api.py`` so they only
run in matrices that install the ``ai`` extra.
"""

from __future__ import annotations

import pytest

from aaiclick.data.data_context import create_object_from_value
from aaiclick.internal_api import lineage as lineage_api
from aaiclick.internal_api import objects
from aaiclick.internal_api.errors import Invalid, NotFound
from aaiclick.oplog.lineage import lineage_context
from aaiclick.orchestration.orch_context import task_scope

_REVENUE = "lineage_api_revenue"
REVENUE_TABLE = f"p_{_REVENUE}"


@pytest.fixture
async def revenue_table(orch_ctx):
    """A persistent ``[1, 2]`` object with recorded lineage; returns its table name."""
    async with task_scope(task_id=1, job_id=1, run_id=100):
        await create_object_from_value([1, 2], name=_REVENUE, scope="global")
    return REVENUE_TABLE


async def test_oplog_subgraph_follows_direction_and_depth(orch_ctx):
    async with task_scope(task_id=1, job_id=1, run_id=100):
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([4, 5, 6])
        result = await a.concat(b)

    async with lineage_context():
        graph = await lineage_api.oplog_subgraph(a.table, direction="forward", max_depth=3)

    assert {n.table for n in graph.nodes} == {a.table, result.table}
    assert {(e.source, e.target) for e in graph.edges} == {(a.table, result.table), (b.table, result.table)}


async def test_query_table_runs_validated_select(revenue_table):
    result = await lineage_api.query_table(
        f"SELECT value FROM {revenue_table} ORDER BY value",
        target_table=revenue_table,
        row_limit=50,
        direction="forward",
        max_depth=3,
    )

    assert result.columns == ["value"]
    assert result.rows == [[1], [2]]
    assert result.truncated is False


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param(f"DROP TABLE {REVENUE_TABLE}", id="ddl"),
        pytest.param("SELECT * FROM p_secret", id="out-of-scope"),
    ],
)
async def test_query_table_raises_invalid(revenue_table, sql):
    """The scope is the target's lineage, looked up here — a caller cannot
    widen it by naming extra tables."""
    with pytest.raises(Invalid):
        await lineage_api.query_table(sql, target_table=revenue_table)


async def test_query_table_without_lineage_raises_not_found(orch_ctx):
    """A target no operation produced has an empty graph, so nothing is in scope."""
    with pytest.raises(NotFound):
        await lineage_api.query_table("SELECT 1", target_table="p_nowhere")


async def test_get_table_schema_returns_describe_result(revenue_table):
    schema = await lineage_api.get_table_schema(revenue_table, target_table=revenue_table)

    assert schema.table == revenue_table
    assert "value" in [c.name for c in schema.columns]


async def test_get_table_schema_raises_invalid_when_out_of_scope(revenue_table):
    with pytest.raises(Invalid):
        await lineage_api.get_table_schema("p_secret", target_table=revenue_table)


async def test_get_table_schema_raises_not_found_when_describe_fails(revenue_table):
    """The table was dropped after its lineage was recorded."""
    await objects.delete_object(_REVENUE)

    with pytest.raises(NotFound):
        await lineage_api.get_table_schema(revenue_table, target_table=revenue_table)
