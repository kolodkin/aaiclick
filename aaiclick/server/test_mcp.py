"""Tests for the FastMCP tool surface over ``internal_api``.

Router tests already cover business logic end-to-end; these tests assert
the MCP plumbing only — that every CLI verb is registered as a tool, that
tool results round-trip through the declared view models, and that
``internal_api`` errors surface as tool errors on the client.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError

from aaiclick.ai.agents.lineage_tools import QueryResult, TableSchema
from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.view_models import ObjectDetail, ObjectView
from aaiclick.oplog.lineage import OplogGraph
from aaiclick.orchestration.execution.execution_worker import register_execution_worker
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import EXECUTION_WORKER_STOPPING
from aaiclick.orchestration.orch_context import task_scope
from aaiclick.orchestration.view_models import ClearTaskView, ExecutionWorkerView, JobDetail, JobView, TaskDetail
from aaiclick.view_models import Page
from aaiclick.viewer.view_models import ObjectQueryResult

from .mcp import mcp

# Exact match, so a new tool fails this test until it is listed here on
# purpose. One name must never appear: a job *waiter*. ``job_stats`` reports
# where a job is and the agent re-triggers on its own scheduled event; a tool
# that blocks for a job's lifetime would hold the agent's turn open instead.
# Blocking on a job is CLI-only by design — see ``aaiclick/cli_wait.py``.
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
    "clear_task",
    "list_execution_workers",
    "start_execution_worker",
    "stop_execution_worker",
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
    "query_object",
    "list_saved_queries",
    "save_query",
    "delete_saved_query",
    "list_dashboards",
    "get_dashboard",
    "save_dashboard",
    "delete_dashboard",
    "run_dashboard",
}


@pytest.fixture
async def revenue_lineage(orch_ctx):
    """A persistent ``p_mcp_revenue`` table with recorded lineage."""
    async with task_scope(task_id=1, job_id=1, run_id=100):
        await create_object_from_value({"id": [1, 2], "val": [10.0, 20.0]}, name="mcp_revenue", scope="global")


@pytest.fixture
async def mcp_client() -> AsyncIterator[Client]:
    async with Client(mcp) as client:
        yield client


async def test_registered_tools_match_expected(mcp_client):
    tools = await mcp_client.list_tools()
    names = {t.name for t in tools}
    assert names == EXPECTED_TOOLS


async def test_no_blocking_job_waiter_is_exposed(mcp_client):
    """Status is a question an agent asks and re-asks on its own schedule,
    never a tool it blocks inside. Catches a waiter added under any name."""
    tools = await mcp_client.list_tools()
    blocking = {t.name for t in tools if any(k in t.name.lower() for k in ("wait", "block", "poll", "watch"))}
    assert blocking == set()


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


async def test_clear_task_returns_view(orch_ctx, mcp_client):
    job = await create_job("mcp_clear_job", simple_task)
    task = (await get_tasks_for_job(job.id))[0]

    result = await mcp_client.call_tool("clear_task", {"task_id": task.id})

    view = ClearTaskView.model_validate(result.structured_content)
    assert view.cleared_task_ids == [task.id]
    assert view.job.id == job.id


async def test_list_workers_returns_page(orch_ctx, mcp_client):
    await register_execution_worker(hostname="mcp_worker")

    result = await mcp_client.call_tool("list_execution_workers", {})

    page = Page[ExecutionWorkerView].model_validate(result.structured_content)
    assert any(w.hostname == "mcp_worker" for w in page.items)


async def test_stop_worker_transitions_to_stopping(orch_ctx, mcp_client):
    worker = await register_execution_worker(hostname="mcp_stop")

    result = await mcp_client.call_tool("stop_execution_worker", {"execution_worker_id": worker.id})

    view = ExecutionWorkerView.model_validate(result.structured_content)
    assert view.status == EXECUTION_WORKER_STOPPING


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
    async with task_scope(task_id=1, job_id=1, run_id=100):
        a = await create_object_from_value([1, 2])
        b = await create_object_from_value([3])
        concat = await a.concat(b)

    result = await mcp_client.call_tool(
        "oplog_subgraph",
        {"target_table": concat.table, "direction": "backward"},
    )

    parsed = OplogGraph.model_validate(result.structured_content)
    assert {n.table for n in parsed.nodes} == {a.table, b.table, concat.table}


async def test_query_table_returns_query_result(orch_ctx, mcp_client, revenue_lineage):
    result = await mcp_client.call_tool(
        "query_table",
        {"sql": "SELECT id, val FROM p_mcp_revenue ORDER BY id", "target_table": "p_mcp_revenue"},
    )

    parsed = QueryResult.model_validate(result.structured_content)
    assert parsed.columns == ["id", "val"]
    assert parsed.rows == [[1, 10.0], [2, 20.0]]


async def test_query_table_rejects_out_of_scope(orch_ctx, mcp_client, revenue_lineage):
    """Invalid from internal_api surfaces as an MCP ToolError.

    Validation itself (scope / DDL rules) is covered in
    aaiclick/internal_api/test_lineage.py — this asserts only the
    error mapping through the MCP layer.
    """
    with pytest.raises(ToolError):
        await mcp_client.call_tool(
            "query_table",
            {"sql": "SELECT * FROM p_secret", "target_table": "p_mcp_revenue"},
        )


async def test_get_table_schema_returns_columns(orch_ctx, mcp_client, revenue_lineage):
    result = await mcp_client.call_tool(
        "get_table_schema",
        {"table": "p_mcp_revenue", "target_table": "p_mcp_revenue"},
    )

    parsed = TableSchema.model_validate(result.structured_content)
    assert parsed.table == "p_mcp_revenue"
    assert {"id", "val"} <= {c.name for c in parsed.columns}


async def test_query_object_tool_round_trips(orch_ctx, mcp_client):
    await create_object_from_value({"id": [1, 2]}, name="mcp_orders", scope="global")
    result = await mcp_client.call_tool(
        "query_object", {"request": {"object": "mcp_orders", "order_by": [["id", "DESC"]]}}
    )
    parsed = ObjectQueryResult.model_validate(result.structured_content)
    assert parsed.data == [["2"], ["1"]]


async def test_query_object_tool_maps_not_found(orch_ctx, mcp_client):
    with pytest.raises(ToolError):
        await mcp_client.call_tool("query_object", {"request": {"object": "missing"}})
