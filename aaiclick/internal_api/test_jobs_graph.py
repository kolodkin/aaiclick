"""Tests for ``aaiclick.internal_api.jobs.get_job_graph``."""

from __future__ import annotations

import pytest

from aaiclick.orchestration.factories import create_job, create_task
from aaiclick.orchestration.orch_context import commit_tasks
from aaiclick.orchestration.view_models import GRAPH_NODE_TASK, JobGraphView

from . import errors, jobs

_SAMPLE_TASK = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"


async def test_get_job_graph_returns_nodes_and_edges(orch_ctx):
    job = await create_job("graph_pipeline", _SAMPLE_TASK)
    raw = create_task(_SAMPLE_TASK)
    transform = create_task(_SAMPLE_TASK)
    report = create_task(_SAMPLE_TASK)
    raw >> transform >> report
    await commit_tasks(report, job_id=job.id)

    graph = await jobs.get_job_graph(job.id)

    assert isinstance(graph, JobGraphView)
    assert graph.job_id == job.id
    node_ids = {n.id for n in graph.nodes}
    assert {raw.id, transform.id, report.id} <= node_ids
    edges = {(e.source_id, e.target_id) for e in graph.edges}
    assert (raw.id, transform.id) in edges
    assert (transform.id, report.id) in edges
    assert all(n.kind == GRAPH_NODE_TASK for n in graph.nodes)


async def test_get_job_graph_resolves_by_name(orch_ctx):
    job = await create_job("graph_by_name", _SAMPLE_TASK)
    only = create_task(_SAMPLE_TASK)
    await commit_tasks(only, job_id=job.id)

    graph = await jobs.get_job_graph("graph_by_name")

    assert graph.job_id == job.id


async def test_get_job_graph_missing_job_raises_not_found(orch_ctx):
    with pytest.raises(errors.NotFound):
        await jobs.get_job_graph("no_such_job_at_all")
