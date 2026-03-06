"""Tests for cancel_job() API."""

from sqlmodel import select

from .claiming import cancel_job, check_task_cancelled, claim_next_task, update_job_status, update_task_status
from .context import commit_tasks, get_orch_session
from .factories import create_job, create_task
from .models import Job, JobStatus, Task, TaskStatus
from .worker import register_worker


async def test_cancel_pending_job(orch_ctx):
    """Test cancelling a PENDING job sets job and tasks to CANCELLED."""
    job = await create_job("cancel_pending", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    assert job.status == JobStatus.PENDING

    result = await cancel_job(job.id)
    assert result is True

    async with get_orch_session() as session:
        db_job = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        assert db_job.status == JobStatus.CANCELLED
        assert db_job.completed_at is not None

        tasks = (await session.execute(select(Task).where(Task.job_id == job.id))).scalars().all()
        for t in tasks:
            assert t.status == TaskStatus.CANCELLED
            assert t.completed_at is not None


async def test_cancel_running_job(orch_ctx):
    """Test cancelling a RUNNING job cancels all non-terminal tasks."""
    worker = await register_worker()

    # Clear pending tasks from other tests
    while await claim_next_task(worker.id) is not None:
        pass

    job = await create_job("cancel_running", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

    # Claim a task to transition job to RUNNING
    claimed = await claim_next_task(worker.id)
    assert claimed is not None

    result = await cancel_job(job.id)
    assert result is True

    async with get_orch_session() as session:
        db_job = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        assert db_job.status == JobStatus.CANCELLED

        tasks = (await session.execute(select(Task).where(Task.job_id == job.id))).scalars().all()
        for t in tasks:
            assert t.status == TaskStatus.CANCELLED


async def test_cancel_completed_job_returns_false(orch_ctx):
    """Test that a COMPLETED job cannot be cancelled."""
    job = await create_job("cancel_completed", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    await update_job_status(job.id, JobStatus.COMPLETED)

    result = await cancel_job(job.id)
    assert result is False

    async with get_orch_session() as session:
        db_job = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        assert db_job.status == JobStatus.COMPLETED


async def test_cancel_failed_job_returns_false(orch_ctx):
    """Test that a FAILED job cannot be cancelled."""
    job = await create_job("cancel_failed", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    await update_job_status(job.id, JobStatus.FAILED, error="some error")

    result = await cancel_job(job.id)
    assert result is False

    async with get_orch_session() as session:
        db_job = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        assert db_job.status == JobStatus.FAILED


async def test_cancel_already_cancelled_returns_false(orch_ctx):
    """Test that cancelling an already-cancelled job returns False."""
    job = await create_job("cancel_twice", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

    result1 = await cancel_job(job.id)
    assert result1 is True

    result2 = await cancel_job(job.id)
    assert result2 is False


async def test_cancel_nonexistent_job_returns_false(orch_ctx):
    """Test that cancelling a non-existent job returns False."""
    result = await cancel_job(999999999)
    assert result is False


async def test_claim_skips_cancelled_job_tasks(orch_ctx):
    """Test that claim_next_task skips tasks from cancelled jobs."""
    worker = await register_worker()

    # Clear pending tasks from other tests
    while await claim_next_task(worker.id) is not None:
        pass

    job = await create_job("claim_cancelled", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

    await cancel_job(job.id)

    # No tasks should be claimable from a cancelled job
    task = await claim_next_task(worker.id)
    assert task is None


async def test_cancel_preserves_completed_tasks(orch_ctx):
    """Test that completed tasks are preserved when a job is cancelled."""
    worker = await register_worker()

    # Clear pending tasks from other tests
    while await claim_next_task(worker.id) is not None:
        pass

    job = await create_job("cancel_preserves", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

    # Claim and complete the task
    claimed = await claim_next_task(worker.id)
    assert claimed is not None
    await update_task_status(claimed.id, TaskStatus.COMPLETED)

    # Add another pending task
    extra = create_task("aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    await commit_tasks(extra, job_id=job.id)

    # Cancel the job
    await cancel_job(job.id)

    async with get_orch_session() as session:
        tasks = (await session.execute(
            select(Task).where(Task.job_id == job.id).order_by(Task.id)
        )).scalars().all()

        statuses = [t.status for t in tasks]
        assert TaskStatus.COMPLETED in statuses
        assert TaskStatus.CANCELLED in statuses


async def test_check_task_cancelled(orch_ctx):
    """Test check_task_cancelled returns correct values."""
    job = await create_job("check_cancelled", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

    async with get_orch_session() as session:
        task = (await session.execute(select(Task).where(Task.job_id == job.id))).scalar_one()
        task_id = task.id

    # Task is PENDING, not cancelled
    assert await check_task_cancelled(task_id) is False

    # Cancel the job (marks tasks as CANCELLED)
    await cancel_job(job.id)

    # Task should now be cancelled
    assert await check_task_cancelled(task_id) is True

    # Non-existent task returns False
    assert await check_task_cancelled(999999999) is False


async def test_update_task_status_refuses_overwrite_cancelled(orch_ctx):
    """Test that update_task_status won't overwrite CANCELLED status."""
    job = await create_job("overwrite_cancelled", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

    async with get_orch_session() as session:
        task = (await session.execute(select(Task).where(Task.job_id == job.id))).scalar_one()
        task_id = task.id

    # Cancel the job
    await cancel_job(job.id)

    # Attempt to overwrite with COMPLETED should fail
    result = await update_task_status(task_id, TaskStatus.COMPLETED)
    assert result is False

    # Verify status is still CANCELLED
    async with get_orch_session() as session:
        task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()
        assert task.status == TaskStatus.CANCELLED
