# `a >> b` wires a dependency as a bare statement; pyright flags every such binary operation.
# pyright: reportUnusedExpression=false
"""Tests for ``aaiclick.internal_api.jobs.get_job_timings``."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from sqlmodel import update

from aaiclick.datetime_utils import utc_now
from aaiclick.orchestration.factories import create_job, create_task
from aaiclick.orchestration.models import TASK_COMPLETED, TASK_FAILED, Group, Task
from aaiclick.orchestration.orch_context import commit_tasks, get_sql_session
from aaiclick.snowflake import get_snowflake_id
from aaiclick.testing import set_task_runs

from . import errors, jobs

_SAMPLE_TASK = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"


async def _finish(task: Task, at: datetime) -> None:
    async with get_sql_session() as session:
        await session.execute(update(Task).where(Task.id == task.id).values(completed_at=at))
        await session.commit()


def _by_id(view) -> dict:
    return {t.id: t for t in view.tasks}


async def test_timings_lists_attempts_in_order(orch_ctx):
    job = await create_job("timings_attempts", _SAMPLE_TASK)
    task = create_task(_SAMPLE_TASK)
    await commit_tasks(task, job_id=job.id)
    await set_task_runs(task.id, [get_snowflake_id(), get_snowflake_id()], [TASK_FAILED, TASK_COMPLETED])

    timing = _by_id(await jobs.get_job_timings(job.id))[task.id]

    assert [a.attempt for a in timing.attempts] == [1, 2]
    assert [a.status for a in timing.attempts] == [TASK_FAILED, TASK_COMPLETED]


async def test_timings_never_run_task(orch_ctx):
    job = await create_job("timings_never_run", _SAMPLE_TASK)
    task = create_task(_SAMPLE_TASK)
    await commit_tasks(task, job_id=job.id)

    assert _by_id(await jobs.get_job_timings(job.id))[task.id].attempts == []


async def test_timings_ready_at(orch_ctx):
    job = await create_job("timings_ready", _SAMPLE_TASK)
    raw = create_task(_SAMPLE_TASK)
    report = create_task(_SAMPLE_TASK)
    raw >> report
    await commit_tasks(report, job_id=job.id)
    done = utc_now() + timedelta(seconds=5)
    await _finish(raw, done)

    timings = _by_id(await jobs.get_job_timings(job.id))

    assert timings[raw.id].ready_at == timings[raw.id].created_at
    assert timings[report.id].ready_at == done


async def test_timings_ready_at_none_while_upstream_running(orch_ctx):
    job = await create_job("timings_blocked", _SAMPLE_TASK)
    raw = create_task(_SAMPLE_TASK)
    report = create_task(_SAMPLE_TASK)
    raw >> report
    await commit_tasks(report, job_id=job.id)

    assert _by_id(await jobs.get_job_timings(job.id))[report.id].ready_at is None


async def test_timings_ready_at_through_group(orch_ctx):
    job = await create_job("timings_group", _SAMPLE_TASK)
    stage = Group(id=get_snowflake_id(), name="stage")
    first = create_task(_SAMPLE_TASK)
    second = create_task(_SAMPLE_TASK)
    first.group_id = stage.id
    second.group_id = stage.id
    after = create_task(_SAMPLE_TASK)
    stage >> after
    await commit_tasks([first, second, stage, after], job_id=job.id)
    early = utc_now() + timedelta(seconds=1)
    late = utc_now() + timedelta(seconds=9)
    await _finish(first, early)

    assert _by_id(await jobs.get_job_timings(job.id))[after.id].ready_at is None

    await _finish(second, late)

    assert _by_id(await jobs.get_job_timings(job.id))[after.id].ready_at == late


async def test_timings_unknown_ref(orch_ctx):
    with pytest.raises(errors.NotFound):
        await jobs.get_job_timings(get_snowflake_id())
