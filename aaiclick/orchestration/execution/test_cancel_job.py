"""Tests for cancel_job() API."""

import pytest
from sqlmodel import select

from ..factories import create_job, create_task
from ..jobs import get_task
from ..models import (
    JOB_CANCELLED,
    JOB_COMPLETED,
    JOB_FAILED,
    JOB_PENDING,
    TASK_CANCELLED,
    TASK_COMPLETED,
    TASK_PENDING_CANCELLED_CLEANUP,
    TASK_PENDING_FAILURE_CLEANUP,
    TASK_RUNNING,
    Job,
    Task,
)
from ..orch_context import commit_tasks, get_sql_session
from .claiming import (
    JobAlreadyTerminal,
    JobNotFound,
    cancel_job,
    check_task_cancelled,
    claim_next_task,
    release_cancelled_run,
    update_job_status,
    update_task_status,
)
from .execution_worker import _set_pending_failure_cleanup, register_execution_worker
from .runner import register_run

SIMPLE_TASK = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"


async def _job_tasks(job_id: int) -> list[Task]:
    async with get_sql_session() as session:
        return list((await session.execute(select(Task).where(Task.job_id == job_id).order_by(Task.id))).scalars())


async def _claim_fresh(worker_id: int, job_name: str) -> tuple[int, int]:
    """Drain leftover tasks, create a one-task job, claim it. Returns (job_id, task_id)."""
    while await claim_next_task(worker_id) is not None:
        pass
    job = await create_job(job_name, SIMPLE_TASK)
    claimed = await claim_next_task(worker_id)
    assert claimed is not None
    return job.id, claimed.id


async def _start_run(task_id: int) -> None:
    """Mark the claimed task RUNNING with one registered run, as a worker would."""
    await update_task_status(task_id, TASK_RUNNING)
    await register_run(task_id)


async def test_cancel_pending_job(orch_ctx):
    """Cancelling a PENDING job closes the job at once; its unclaimed tasks go
    to PENDING_CANCELLED_CLEANUP, not yet terminal."""
    job = await create_job("cancel_pending", SIMPLE_TASK)
    assert job.status == JOB_PENDING

    cancelled = await cancel_job(job.id)
    assert cancelled.status == JOB_CANCELLED

    async with get_sql_session() as session:
        db_job = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        assert db_job.status == JOB_CANCELLED
        assert db_job.completed_at is not None

    for t in await _job_tasks(job.id):
        assert t.status == TASK_PENDING_CANCELLED_CLEANUP
        assert t.execution_worker_id is None
        assert t.completed_at is None


async def test_cancel_running_job_keeps_worker_ownership(orch_ctx):
    """A claimed task is cancelled but stays owned by its worker until the
    worker reports the killed run back."""
    worker = await register_execution_worker()
    job_id, task_id = await _claim_fresh(worker.id, "cancel_running")

    cancelled = await cancel_job(job_id)
    assert cancelled.status == JOB_CANCELLED

    (task,) = await _job_tasks(job_id)
    assert task.id == task_id
    assert task.status == TASK_PENDING_CANCELLED_CLEANUP
    assert task.execution_worker_id == worker.id


async def test_cancel_releases_failure_cleanup_task(orch_ctx):
    """A task already reported back as failed has no worker to wait for, so
    cancelling it releases the ownership at once."""
    worker = await register_execution_worker()
    job_id, task_id = await _claim_fresh(worker.id, "cancel_failure_cleanup")
    await _set_pending_failure_cleanup(task_id, "boom")
    (task,) = await _job_tasks(job_id)
    assert task.status == TASK_PENDING_FAILURE_CLEANUP
    assert task.execution_worker_id == worker.id

    await cancel_job(job_id)

    (task,) = await _job_tasks(job_id)
    assert task.status == TASK_PENDING_CANCELLED_CLEANUP
    assert task.execution_worker_id is None


@pytest.mark.parametrize(
    "name, status, error",
    [
        pytest.param("cancel_completed", JOB_COMPLETED, None, id="completed"),
        pytest.param("cancel_failed", JOB_FAILED, "some error", id="failed"),
    ],
)
async def test_cancel_terminal_job_raises_already_terminal(orch_ctx, name, status, error):
    """A job in a terminal state cannot be cancelled, and keeps that state."""
    job = await create_job(name, SIMPLE_TASK)
    await update_job_status(job.id, status, error=error)

    with pytest.raises(JobAlreadyTerminal, match=status):
        await cancel_job(job.id)

    async with get_sql_session() as session:
        db_job = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        assert db_job.status == status


async def test_cancel_already_cancelled_raises_already_terminal(orch_ctx):
    """Test that cancelling an already-cancelled job raises."""
    job = await create_job("cancel_twice", SIMPLE_TASK)

    first = await cancel_job(job.id)
    assert first.status == JOB_CANCELLED

    with pytest.raises(JobAlreadyTerminal, match="CANCELLED"):
        await cancel_job(job.id)


async def test_cancel_nonexistent_job_raises_not_found(orch_ctx):
    """Test that cancelling a non-existent job raises JobNotFound."""
    with pytest.raises(JobNotFound, match="999999999"):
        await cancel_job(999999999)


async def test_claim_skips_cancelled_job_tasks(orch_ctx):
    """Test that claim_next_task skips tasks from cancelled jobs."""
    worker = await register_execution_worker()

    # Clear pending tasks from other tests
    while await claim_next_task(worker.id) is not None:
        pass

    job = await create_job("claim_cancelled", SIMPLE_TASK)

    await cancel_job(job.id)

    # No tasks should be claimable from a cancelled job
    task = await claim_next_task(worker.id)
    assert task is None


async def test_cancel_preserves_completed_tasks(orch_ctx):
    """Test that completed tasks are preserved when a job is cancelled."""
    worker = await register_execution_worker()

    # Clear pending tasks from other tests
    while await claim_next_task(worker.id) is not None:
        pass

    job = await create_job("cancel_preserves", SIMPLE_TASK)

    # Claim and complete the task
    claimed = await claim_next_task(worker.id)
    assert claimed is not None
    await update_task_status(claimed.id, TASK_COMPLETED)

    # Add another pending task
    extra = create_task(SIMPLE_TASK)
    await commit_tasks(extra, job_id=job.id)

    # Cancel the job
    await cancel_job(job.id)

    statuses = [t.status for t in await _job_tasks(job.id)]
    assert statuses == [TASK_COMPLETED, TASK_PENDING_CANCELLED_CLEANUP]


async def test_check_task_cancelled(orch_ctx):
    """Test check_task_cancelled returns correct values."""
    job = await create_job("check_cancelled", SIMPLE_TASK)

    async with get_sql_session() as session:
        task = (await session.execute(select(Task).where(Task.job_id == job.id))).scalar_one()
        task_id = task.id

    # Task is PENDING, not cancelled
    assert await check_task_cancelled(task_id) is False

    # Cancel the job (marks tasks PENDING_CANCELLED_CLEANUP) — the run must abort now,
    # before the background worker settles the task to CANCELLED.
    await cancel_job(job.id)
    assert await check_task_cancelled(task_id) is True

    # Non-existent task returns False
    assert await check_task_cancelled(999999999) is False


async def test_update_task_status_refuses_overwrite_cancelled(orch_ctx):
    """A cancelled task's status is owned by the cleanup pass; a late worker
    write (COMPLETED) is refused."""
    job = await create_job("overwrite_cancelled", SIMPLE_TASK)
    (task,) = await _job_tasks(job.id)

    await cancel_job(job.id)

    assert await update_task_status(task.id, TASK_COMPLETED) is False
    refreshed = await get_task(task.id)
    assert refreshed is not None
    assert refreshed.status == TASK_PENDING_CANCELLED_CLEANUP


async def test_set_pending_failure_cleanup_refuses_cancelled(orch_ctx):
    """A killed run reports back as a failure; that must not resurrect the
    cancelled task as a retry."""
    worker = await register_execution_worker()
    job_id, task_id = await _claim_fresh(worker.id, "failure_after_cancel")
    await cancel_job(job_id)

    assert await _set_pending_failure_cleanup(task_id, "killed", expected_epoch=0) is False

    (task,) = await _job_tasks(job_id)
    assert task.status == TASK_PENDING_CANCELLED_CLEANUP
    assert task.error is None


async def test_release_cancelled_run(orch_ctx):
    """The worker's report-back on a cancelled task stamps the run CANCELLED
    and releases the ownership the cleanup pass waits for."""
    worker = await register_execution_worker()
    job_id, task_id = await _claim_fresh(worker.id, "release_run")
    await _start_run(task_id)
    await cancel_job(job_id)

    assert await release_cancelled_run(task_id, expected_epoch=0) is True

    (task,) = await _job_tasks(job_id)
    assert task.status == TASK_PENDING_CANCELLED_CLEANUP
    assert task.execution_worker_id is None
    assert task.run_statuses == [TASK_CANCELLED]


async def test_release_cancelled_run_is_noop_unless_cancelled(orch_ctx):
    """A running task, or a stale-epoch report, is left owned by its worker."""
    worker = await register_execution_worker()
    job_id, task_id = await _claim_fresh(worker.id, "release_noop")
    await _start_run(task_id)

    assert await release_cancelled_run(task_id) is False
    (task,) = await _job_tasks(job_id)
    assert task.execution_worker_id == worker.id
    assert task.run_statuses == [TASK_RUNNING]

    await cancel_job(job_id)
    assert await release_cancelled_run(task_id, expected_epoch=1) is False
    (task,) = await _job_tasks(job_id)
    assert task.execution_worker_id == worker.id
    assert task.run_statuses == [TASK_RUNNING]
