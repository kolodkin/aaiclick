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

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.view_models import ObjectDetail, ObjectView
from aaiclick.oplog.lineage import OplogGraph
from aaiclick.oplog.view_models import LineageAnswer
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
    "explain_lineage",
    "debug_result",
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


async def test_explain_lineage_returns_answer(orch_ctx, mcp_client):
    mock_explain = AsyncMock(return_value="Pipeline does X then Y.")

    with patch("aaiclick.internal_api.lineage._explain_lineage", new=mock_explain):
        result = await mcp_client.call_tool(
            "explain_lineage",
            {"target_table": "result_table", "question": "How?"},
        )

    answer = LineageAnswer.model_validate(result.structured_content)
    assert answer.text == "Pipeline does X then Y."
    mock_explain.assert_awaited_once_with("result_table", question="How?")


async def test_debug_result_returns_answer(orch_ctx, mcp_client):
    mock_debug = AsyncMock(return_value="Row 3 has the highest value.")

    with patch("aaiclick.internal_api.lineage._debug_result", new=mock_debug):
        result = await mcp_client.call_tool(
            "debug_result",
            {"target_table": "result_table", "question": "Which row?", "max_iterations": 5},
        )

    answer = LineageAnswer.model_validate(result.structured_content)
    assert answer.text == "Row 3 has the highest value."
    mock_debug.assert_awaited_once_with("result_table", question="Which row?", max_iterations=5)
