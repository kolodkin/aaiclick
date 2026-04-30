"""Tests for ``aaiclick.internal_api.jobs``."""

from __future__ import annotations

import pytest
from sqlmodel import select

from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.models import JOB_CANCELLED, JOB_COMPLETED, JOB_PENDING, Task
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.registered_jobs import register_job
from aaiclick.orchestration.view_models import JobDetail, JobStatsView, JobView
from aaiclick.view_models import JobListFilter, Page, RunJobRequest

from . import errors, jobs

_SAMPLE_TASK = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"


async def test_list_jobs_returns_page_with_total(orch_ctx):
    await create_job("list_a", _SAMPLE_TASK)
    await create_job("list_b", _SAMPLE_TASK)

    page = await jobs.list_jobs()

    assert isinstance(page, Page)
    assert page.total is not None and page.total >= 2
    assert all(isinstance(j, JobView) for j in page.items)
    names = [j.name for j in page.items]
    assert "list_a" in names and "list_b" in names


async def test_list_jobs_filter_by_status(orch_ctx):
    await create_job("only_pending", _SAMPLE_TASK)

    pending = await jobs.list_jobs(JobListFilter(status=JOB_PENDING))
    completed = await jobs.list_jobs(JobListFilter(status=JOB_COMPLETED))

    assert "only_pending" in [j.name for j in pending.items]
    assert "only_pending" not in [j.name for j in completed.items]


async def test_list_jobs_name_like_and_pagination(orch_ctx):
    for i in range(5):
        await create_job(f"page_{i}", _SAMPLE_TASK)

    first = await jobs.list_jobs(JobListFilter(name="page_%", limit=2, offset=0))
    second = await jobs.list_jobs(JobListFilter(name="page_%", limit=2, offset=2))

    assert first.total == 5
    assert len(first.items) == 2 and len(second.items) == 2
    assert {j.id for j in first.items}.isdisjoint({j.id for j in second.items})


async def test_get_job_by_int_id(orch_ctx):
    created = await create_job("by_int", _SAMPLE_TASK)

    detail = await jobs.get_job(created.id)

    assert isinstance(detail, JobDetail)
    assert detail.id == created.id
    assert detail.name == "by_int"
    assert len(detail.tasks) == 1


async def test_get_job_by_numeric_string(orch_ctx):
    created = await create_job("by_str_id", _SAMPLE_TASK)

    detail = await jobs.get_job(str(created.id))

    assert detail.id == created.id


async def test_get_job_by_name_returns_latest(orch_ctx):
    older = await create_job("twice", _SAMPLE_TASK)
    newer = await create_job("twice", _SAMPLE_TASK)

    detail = await jobs.get_job("twice")

    assert detail.id == newer.id
    assert detail.id != older.id


async def test_get_job_not_found_raises(orch_ctx):
    with pytest.raises(errors.NotFound):
        await jobs.get_job(999_999_999)


async def test_job_stats_structure(orch_ctx):
    created = await create_job("stats_job", _SAMPLE_TASK)

    stats = await jobs.job_stats(created.id)

    assert isinstance(stats, JobStatsView)
    assert stats.job_id == created.id
    assert stats.total_tasks == 1
    assert "PENDING" in stats.status_counts


async def test_job_stats_not_found_raises(orch_ctx):
    with pytest.raises(errors.NotFound):
        await jobs.job_stats(0)


async def test_cancel_job_transitions_to_cancelled(orch_ctx):
    created = await create_job("to_cancel", _SAMPLE_TASK)

    view = await jobs.cancel_job(created.id)

    assert view.status == JOB_CANCELLED
    assert view.completed_at is not None


async def test_cancel_job_terminal_raises_conflict(orch_ctx):
    created = await create_job("double_cancel", _SAMPLE_TASK)

    await jobs.cancel_job(created.id)

    with pytest.raises(errors.Conflict):
        await jobs.cancel_job(created.id)


async def test_cancel_job_not_found_raises(orch_ctx):
    with pytest.raises(errors.NotFound):
        await jobs.cancel_job(0)


async def test_run_job_creates_job_with_kwargs(orch_ctx):
    request = RunJobRequest(name="run_simple", kwargs={"x": 1})

    view = await jobs.run_job(request)

    assert isinstance(view, JobView)
    assert view.name == "run_simple"

    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == view.id))
        task = result.scalar_one()

    assert task.kwargs == {"x": 1}


async def test_run_job_dotted_name_splits_entrypoint(orch_ctx):
    request = RunJobRequest(name="myapp.pipelines.daily_etl")

    view = await jobs.run_job(request)
    detail = await jobs.get_job(view.id)

    assert view.name == "daily_etl"
    assert detail.tasks[0].entrypoint == "myapp.pipelines.daily_etl"


async def test_run_job_bare_name_resolves_registered_entrypoint(orch_ctx):
    """A bare name must look up the registered job's entrypoint, not reuse the name."""
    await register_job(name="my_pipeline", entrypoint=_SAMPLE_TASK)

    view = await jobs.run_job(RunJobRequest(name="my_pipeline"))
    detail = await jobs.get_job(view.id)

    assert detail.tasks[0].entrypoint == _SAMPLE_TASK
