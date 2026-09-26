"""Tests for complete_task_and_roll_up() — a task's completion and its job rollup in one commit."""

import asyncio

import pytest

from aaiclick.backend import is_postgres

from ..factories import create_job, create_task
from ..jobs import get_job, get_task, get_tasks_for_job
from ..models import JOB_COMPLETED, TASK_COMPLETED, TASK_PENDING, TASK_RUNNING, Task
from ..orch_context import commit_tasks
from .claiming import cancel_job, clear_task, complete_task_and_roll_up, update_task_status

EP = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"


async def _running_job(name: str, n_tasks: int) -> tuple[int, list[int]]:
    """A job whose ``n_tasks`` tasks are all RUNNING at ``run_epoch`` 0."""
    job = await create_job(name, EP)
    await commit_tasks([create_task(EP) for _ in range(n_tasks - 1)], job_id=job.id)
    task_ids = [t.id for t in await get_tasks_for_job(job.id)]
    for task_id in task_ids:
        await update_task_status(task_id, TASK_RUNNING)
    return job.id, task_ids


async def _task(task_id: int) -> Task:
    task = await get_task(task_id)
    assert task is not None
    return task


async def _job_status(job_id: int) -> str:
    job = await get_job(job_id)
    assert job is not None
    return job.status


async def test_last_task_completes_the_job(orch_ctx):
    """Only the last task's completion completes the job."""
    job_id, (first, last) = await _running_job("complete_last", 2)

    assert await complete_task_and_roll_up(first, job_id, {"ref": 1}, expected_epoch=0) is True
    assert await _job_status(job_id) != JOB_COMPLETED

    assert await complete_task_and_roll_up(last, job_id, None, expected_epoch=0) is True
    assert await _job_status(job_id) == JOB_COMPLETED
    done = await _task(first)
    assert done.status == TASK_COMPLETED
    assert done.result == {"ref": 1}
    assert done.completed_at is not None


async def test_fenced_completion_does_not_roll_up(orch_ctx):
    """A stale-epoch completion (the task was cleared) writes nothing, so the job is not completed."""
    job_id, (task_id,) = await _running_job("complete_fenced", 1)
    await clear_task(task_id)  # run_epoch 0 -> 1

    assert await complete_task_and_roll_up(task_id, job_id, None, expected_epoch=0) is False
    assert (await _task(task_id)).status == TASK_PENDING
    assert await _job_status(job_id) != JOB_COMPLETED


async def test_cancelled_completion_is_refused(orch_ctx):
    """A late completion of a cancelled task leaves the cancellation in place."""
    job_id, (task_id,) = await _running_job("complete_cancelled", 1)
    cancelled = await cancel_job(job_id)

    assert await complete_task_and_roll_up(task_id, job_id, None, expected_epoch=0) is False
    assert (await _task(task_id)).status != TASK_COMPLETED
    assert await _job_status(job_id) == cancelled.status


async def test_missing_task_is_refused(orch_ctx):
    assert await complete_task_and_roll_up(999999999, 999999999, None, expected_epoch=0) is False


@pytest.mark.skipif(not is_postgres(), reason="Postgres backend only: SQLite serializes writers")
async def test_concurrent_sibling_completions_complete_the_job(orch_ctx):
    """Race window: siblings finishing together must not each read the other as RUNNING.

    Without the job-row lock, every transaction's rollup sees its sibling
    uncommitted and nobody completes the job (write skew). Several jobs raise
    the odds that an unlocked implementation hits the window.
    """
    jobs = [await _running_job(f"complete_race_{i}", 2) for i in range(10)]

    await asyncio.gather(
        *(
            complete_task_and_roll_up(task_id, job_id, None, expected_epoch=0)
            for job_id, task_ids in jobs
            for task_id in task_ids
        )
    )

    assert [await _job_status(job_id) for job_id, _ in jobs] == [JOB_COMPLETED] * len(jobs)
