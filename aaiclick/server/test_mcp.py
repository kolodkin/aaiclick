"""Tests for the FastMCP tool surface over ``internal_api``.

Router tests already cover business logic end-to-end; these tests assert
the MCP plumbing only — that every CLI verb is registered as a tool, that
tool results round-trip through the declared view models, and that
``internal_api`` errors surface as tool errors on the client.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, patch

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError

from aaiclick.ai.agents.lineage_tools import ColumnSchema, QueryResult, TableSchema
from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.view_models import ObjectDetail, ObjectView
from aaiclick.oplog.lineage import OplogGraph
from aaiclick.orchestration.execution.worker import register_worker
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import WorkerStatus
from aaiclick.orchestration.view_models import JobDetail, JobView, TaskDetail, WorkerView
from aaiclick.testing import make_oplog_node
from aaiclick.view_models import Page

from .mcp import mcp

EXPECTED_TOOLS = {
    "list_jobs",
    "get_job",
    "job_stats",
    "cancel_job",
    "run_job",
    "list_registered_jobs",
    "register_job",
    "enable_job",
    "disable_job",
    "get_task",
    "list_workers",
    "stop_worker",
    "list_objects",
    "get_object",
    "delete_object",
    "purge_objects",
    "oplog_subgraph",
    "query_table",
    "get_table_schema",
    "setup",
    "migrate",
    "bootstrap_ollama",
}


@pytest.fixture
async def mcp_client() -> AsyncIterator[Client]:
    async with Client(mcp) as client:
        yield client


async def test_registered_tools_match_expected(mcp_client):
    tools = await mcp_client.list_tools()
    names = {t.name for t in tools}
    assert names == EXPECTED_TOOLS


async def test_list_jobs_returns_page_view(orch_ctx, mcp_client):
    await create_job("mcp_list_a", simple_task)

    result = await mcp_client.call_tool("list_jobs", {})

    page = Page[JobView].model_validate(result.structured_content)
    assert page.total is not None and page.total >= 1
    assert any(j.name == "mcp_list_a" for j in page.items)


async def test_get_job_returns_detail(orch_ctx, mcp_client):
    job = await create_job("mcp_get_job", simple_task)

    result = await mcp_client.call_tool("get_job", {"ref": job.id})

    detail = JobDetail.model_validate(result.structured_content)
    assert detail.id == job.id
    assert detail.name == "mcp_get_job"


async def test_get_job_not_found_raises_tool_error(mcp_client, orch_ctx):
    with pytest.raises(ToolError):
        await mcp_client.call_tool("get_job", {"ref": 999_999_999})


async def test_get_task_returns_detail(orch_ctx, mcp_client):
    job = await create_job("mcp_task_job", simple_task)
    task = (await get_tasks_for_job(job.id))[0]

    result = await mcp_client.call_tool("get_task", {"task_id": task.id})

    detail = TaskDetail.model_validate(result.structured_content)
    assert detail.id == task.id


async def test_list_workers_returns_page(orch_ctx, mcp_client):
    await register_worker(hostname="mcp_worker")

    result = await mcp_client.call_tool("list_workers", {})

    page = Page[WorkerView].model_validate(result.structured_content)
    assert any(w.hostname == "mcp_worker" for w in page.items)


async def test_stop_worker_transitions_to_stopping(orch_ctx, mcp_client):
    worker = await register_worker(hostname="mcp_stop")

    result = await mcp_client.call_tool("stop_worker", {"worker_id": worker.id})

    view = WorkerView.model_validate(result.structured_content)
    assert view.status is WorkerStatus.STOPPING


async def test_list_objects_returns_page(orch_ctx, mcp_client):
    await create_object_from_value([1, 2, 3], name="mcp_obj_a", scope="global")

    result = await mcp_client.call_tool("list_objects", {})

    page = Page[ObjectView].model_validate(result.structured_content)
    assert any(o.name == "mcp_obj_a" for o in page.items)


async def test_get_object_returns_detail(orch_ctx, mcp_client):
    await create_object_from_value([4, 5], name="mcp_obj_detail", scope="global")

    result = await mcp_client.call_tool("get_object", {"name": "mcp_obj_detail"})

    detail = ObjectDetail.model_validate(result.structured_content)
    assert detail.name == "mcp_obj_detail"


async def test_oplog_subgraph_returns_graph(orch_ctx, mcp_client):
    graph = OplogGraph(nodes=[make_oplog_node("result_table", "add")], edges=[])
    mock_subgraph = AsyncMock(return_value=graph)

    with patch("aaiclick.internal_api.lineage._oplog_subgraph", new=mock_subgraph):
        result = await mcp_client.call_tool(
            "oplog_subgraph",
            {"target_table": "result_table", "direction": "backward"},
        )

    parsed = OplogGraph.model_validate(result.structured_content)
    assert [n.table for n in parsed.nodes] == ["result_table"]
    mock_subgraph.assert_awaited_once_with("result_table", direction="backward", max_depth=10)


async def test_query_table_returns_query_result(orch_ctx, mcp_client):
    qr = QueryResult(columns=["id", "val"], rows=[[1, 10.0], [2, 20.0]], truncated=False)
    mock_run = AsyncMock(return_value=qr)

    with patch("aaiclick.internal_api.lineage.run_select", new=mock_run):
        result = await mcp_client.call_tool(
            "query_table",
            {"sql": "SELECT id, val FROM p_revenue", "scope_tables": ["p_revenue"]},
        )

    parsed = QueryResult.model_validate(result.structured_content)
    assert parsed.columns == ["id", "val"]
    assert parsed.rows == [[1, 10.0], [2, 20.0]]
    mock_run.assert_awaited_once()


async def test_query_table_rejects_out_of_scope(orch_ctx, mcp_client):
    """Out-of-scope table reference raises Invalid → MCP ToolError."""
    with pytest.raises(ToolError):
        await mcp_client.call_tool(
            "query_table",
            {"sql": "SELECT * FROM p_secret", "scope_tables": ["p_revenue"]},
        )


async def test_query_table_rejects_ddl(orch_ctx, mcp_client):
    with pytest.raises(ToolError):
        await mcp_client.call_tool(
            "query_table",
            {"sql": "DROP TABLE p_revenue", "scope_tables": ["p_revenue"]},
        )


async def test_get_table_schema_returns_columns(orch_ctx, mcp_client):
    schema = TableSchema(
        table="p_revenue",
        columns=[ColumnSchema(name="id", type="UInt64"), ColumnSchema(name="val", type="Float64")],
    )
    mock_describe = AsyncMock(return_value=schema)

    with patch("aaiclick.internal_api.lineage.describe_table", new=mock_describe):
        result = await mcp_client.call_tool(
            "get_table_schema",
            {"table": "p_revenue", "scope_tables": ["p_revenue"]},
        )

    parsed = TableSchema.model_validate(result.structured_content)
    assert parsed.table == "p_revenue"
    assert [c.name for c in parsed.columns] == ["id", "val"]


async def test_get_table_schema_rejects_out_of_scope(orch_ctx, mcp_client):
    with pytest.raises(ToolError):
        await mcp_client.call_tool(
            "get_table_schema",
            {"table": "p_secret", "scope_tables": ["p_revenue"]},
        )
