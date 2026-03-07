"""Tests for task retry logic."""

from datetime import datetime, timedelta

from sqlmodel import select

from .claiming import claim_next_task, update_task_status
from .context import get_orch_session
from .decorators import task
from .factories import create_job, create_task
from .models import Job, JobStatus, Task, TaskStatus
from .worker import (
    _schedule_retry,
    _try_complete_job,
    deregister_worker,
    register_worker,
    worker_main_loop,
)


async def test_task_default_no_retries(orch_ctx):
    """Task defaults: max_retries=0, attempt=0, retry_after=None."""
    t = create_task("aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    assert t.max_retries == 0
    assert t.attempt == 0
    assert t.retry_after is None


async def test_create_task_with_max_retries(orch_ctx):
    """create_task() accepts and sets max_retries."""
    t = create_task(
        "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
        max_retries=3,
    )
    assert t.max_retries == 3
    assert t.attempt == 0


async def test_task_decorator_with_max_retries(orch_ctx):
    """@task(max_retries=2) creates tasks with max_retries=2."""

    @task(max_retries=2)
    async def my_retryable_task():
        pass

    t = my_retryable_task()
    assert t.max_retries == 2
    assert t.attempt == 0


async def test_task_decorator_bare(orch_ctx):
    """@task without arguments still works."""

    @task
    async def my_task():
        pass

    t = my_task()
    assert t.max_retries == 0


async def test_schedule_retry(orch_ctx):
    """_schedule_retry resets task to PENDING with incremented attempt."""
    job = await create_job(
        "test_retry",
        create_task(
            "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
            max_retries=3,
        ),
    )

    # Get the task and simulate it being claimed/running
    async with get_orch_session() as session:
        result = await session.execute(
            select(Task).where(Task.job_id == job.id)
        )
        t = result.scalar_one()
        task_id = t.id

    # Mark as running first
    await update_task_status(task_id, TaskStatus.RUNNING)

    # Schedule retry
    before = datetime.utcnow()
    await _schedule_retry(task_id, 0, "test error")

    # Verify state
    async with get_orch_session() as session:
        result = await session.execute(
            select(Task).where(Task.id == task_id)
        )
        t = result.scalar_one()
        assert t.status == TaskStatus.PENDING
        assert t.attempt == 1
        assert t.error == "test error"
        assert t.worker_id is None
        assert t.claimed_at is None
        assert t.started_at is None
        assert t.completed_at is None
        assert t.retry_after is not None
        # Backoff: 1s * 2^0 = 1s
        assert t.retry_after >= before + timedelta(seconds=0.9)
        assert t.retry_after <= before + timedelta(seconds=2)


async def test_retry_backoff_timing(orch_ctx):
    """Backoff doubles each attempt: 1s, 2s, 4s."""
    job = await create_job(
        "test_backoff",
        create_task(
            "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
            max_retries=3,
        ),
    )

    async with get_orch_session() as session:
        result = await session.execute(
            select(Task).where(Task.job_id == job.id)
        )
        task_id = result.scalar_one().id

    # Attempt 0 -> retry_after ~1s
    before = datetime.utcnow()
    await _schedule_retry(task_id, 0, "err")
    async with get_orch_session() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        t = result.scalar_one()
        delay_0 = (t.retry_after - before).total_seconds()
        assert 0.9 <= delay_0 <= 2.0

    # Attempt 1 -> retry_after ~2s
    before = datetime.utcnow()
    await _schedule_retry(task_id, 1, "err")
    async with get_orch_session() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        t = result.scalar_one()
        delay_1 = (t.retry_after - before).total_seconds()
        assert 1.9 <= delay_1 <= 3.0

    # Attempt 2 -> retry_after ~4s
    before = datetime.utcnow()
    await _schedule_retry(task_id, 2, "err")
    async with get_orch_session() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        t = result.scalar_one()
        delay_2 = (t.retry_after - before).total_seconds()
        assert 3.9 <= delay_2 <= 5.0


async def test_claim_respects_retry_after(orch_ctx):
    """Tasks with future retry_after are not claimed."""
    # Clear pending tasks
    cleanup_worker = await register_worker()
    while await claim_next_task(cleanup_worker.id) is not None:
        pass
    await deregister_worker(cleanup_worker.id)

    job = await create_job(
        "test_claim_retry",
        create_task(
            "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
            max_retries=1,
        ),
    )

    # Set retry_after to future
    async with get_orch_session() as session:
        result = await session.execute(
            select(Task).where(Task.job_id == job.id).with_for_update()
        )
        t = result.scalar_one()
        task_id = t.id
        t.retry_after = datetime.utcnow() + timedelta(hours=1)
        session.add(t)
        await session.commit()

    worker = await register_worker()

    # Should not be claimed (retry_after in the future)
    claimed = await claim_next_task(worker.id)
    assert claimed is None

    # Set retry_after to past
    async with get_orch_session() as session:
        result = await session.execute(
            select(Task).where(Task.id == task_id).with_for_update()
        )
        t = result.scalar_one()
        t.retry_after = datetime.utcnow() - timedelta(seconds=1)
        session.add(t)
        await session.commit()

    # Now it should be claimable
    claimed = await claim_next_task(worker.id)
    assert claimed is not None
    assert claimed.id == task_id

    await deregister_worker(worker.id)


async def test_failed_task_retries_via_worker(orch_ctx, monkeypatch, tmpdir):
    """Failing task with max_retries=2 is retried, not immediately FAILED."""
    monkeypatch.setenv("AAICLICK_LOG_DIR", str(tmpdir))

    # Clear pending tasks
    cleanup_worker = await register_worker()
    while await claim_next_task(cleanup_worker.id) is not None:
        pass
    await deregister_worker(cleanup_worker.id)

    job = await create_job(
        "test_retry_worker",
        create_task(
            "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
            max_retries=2,
        ),
    )

    # Run worker for 1 task attempt
    await worker_main_loop(
        max_tasks=1,
        install_signal_handlers=False,
        max_empty_polls=3,
    )

    # Task should be PENDING (retrying), not FAILED
    async with get_orch_session() as session:
        result = await session.execute(
            select(Task).where(Task.job_id == job.id)
        )
        t = result.scalar_one()
        assert t.status == TaskStatus.PENDING
        assert t.attempt == 1
        assert t.retry_after is not None
        assert t.error is not None

    # Job should still be RUNNING (not FAILED)
    async with get_orch_session() as session:
        result = await session.execute(
            select(Job).where(Job.id == job.id)
        )
        j = result.scalar_one()
        assert j.status == JobStatus.RUNNING


async def test_failed_task_exhausts_retries(orch_ctx, monkeypatch, tmpdir):
    """After all retries exhausted, task and job are FAILED."""
    monkeypatch.setenv("AAICLICK_LOG_DIR", str(tmpdir))

    # Clear pending tasks
    cleanup_worker = await register_worker()
    while await claim_next_task(cleanup_worker.id) is not None:
        pass
    await deregister_worker(cleanup_worker.id)

    job = await create_job(
        "test_exhaust_retries",
        create_task(
            "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
            max_retries=1,
        ),
    )

    # First attempt: should retry
    await worker_main_loop(
        max_tasks=1,
        install_signal_handlers=False,
        max_empty_polls=3,
    )

    async with get_orch_session() as session:
        result = await session.execute(
            select(Task).where(Task.job_id == job.id)
        )
        t = result.scalar_one()
        assert t.status == TaskStatus.PENDING
        assert t.attempt == 1
        task_id = t.id

    # Set retry_after to past so worker can pick it up
    async with get_orch_session() as session:
        result = await session.execute(
            select(Task).where(Task.id == task_id).with_for_update()
        )
        t = result.scalar_one()
        t.retry_after = datetime.utcnow() - timedelta(seconds=1)
        session.add(t)
        await session.commit()

    # Second attempt: should exhaust retries
    await worker_main_loop(
        max_tasks=1,
        install_signal_handlers=False,
        max_empty_polls=3,
    )

    # Task should be FAILED now
    async with get_orch_session() as session:
        result = await session.execute(
            select(Task).where(Task.id == task_id)
        )
        t = result.scalar_one()
        assert t.status == TaskStatus.FAILED
        assert t.attempt == 1  # attempt stays at 1 (last retry attempt)
        assert t.error is not None

    # Job should be FAILED
    async with get_orch_session() as session:
        result = await session.execute(
            select(Job).where(Job.id == job.id)
        )
        j = result.scalar_one()
        assert j.status == JobStatus.FAILED
