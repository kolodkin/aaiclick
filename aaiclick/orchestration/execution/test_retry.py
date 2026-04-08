"""Tests for task retry logic."""

from datetime import datetime, timedelta

from sqlalchemy import text
from sqlmodel import select

from .claiming import claim_next_task, update_task_status
from ..orch_context import get_sql_session
from ..decorators import task
from ..factories import create_job, create_task
from ..models import Job, JobStatus, Task, TaskStatus
from .mp_worker import mp_worker_main_loop
from .worker import (
    _schedule_retry,
    _try_complete_job,
    deregister_worker,
    register_worker,
)


async def _cancel_all_pending_tasks():
    """Cancel all pending/running tasks to prevent interference between tests."""
    async with get_sql_session() as session:
        await session.execute(
            text(
                "UPDATE tasks SET status = 'CANCELLED', completed_at = :now "
                "WHERE status IN ('PENDING', 'CLAIMED', 'RUNNING')"
            ),
            {"now": datetime.utcnow()},
        )
        await session.commit()


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
    async with get_sql_session() as session:
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
    async with get_sql_session() as session:
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

    async with get_sql_session() as session:
        result = await session.execute(
            select(Task).where(Task.job_id == job.id)
        )
        task_id = result.scalar_one().id

    # Attempt 0 -> retry_after ~1s
    before = datetime.utcnow()
    await _schedule_retry(task_id, 0, "err")
    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        t = result.scalar_one()
        delay_0 = (t.retry_after - before).total_seconds()
        assert 0.9 <= delay_0 <= 2.0

    # Attempt 1 -> retry_after ~2s
    before = datetime.utcnow()
    await _schedule_retry(task_id, 1, "err")
    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        t = result.scalar_one()
        delay_1 = (t.retry_after - before).total_seconds()
        assert 1.9 <= delay_1 <= 3.0

    # Attempt 2 -> retry_after ~4s
    before = datetime.utcnow()
    await _schedule_retry(task_id, 2, "err")
    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        t = result.scalar_one()
        delay_2 = (t.retry_after - before).total_seconds()
        assert 3.9 <= delay_2 <= 5.0


async def test_claim_respects_retry_after(orch_ctx):
    """Tasks with future retry_after are not claimed."""
    # Cancel all pending/running tasks from previous tests
    await _cancel_all_pending_tasks()

    job = await create_job(
        "test_claim_retry",
        create_task(
            "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
            max_retries=1,
        ),
    )

    # Set retry_after to future
    async with get_sql_session() as session:
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
    async with get_sql_session() as session:
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


async def test_worker_retries_and_exhausts(orch_ctx_no_ch, fast_poll):
    """Worker retries a failing task until max_retries exhausted, then marks FAILED."""
    await _cancel_all_pending_tasks()

    job = await create_job(
        "test_retry_worker",
        create_task(
            "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
            max_retries=2,
        ),
    )

    await mp_worker_main_loop(
        max_tasks=1,
        install_signal_handlers=False,
        max_empty_polls=2,
    )

    # After exhausting all retries: task FAILED, attempt=2
    async with get_sql_session() as session:
        result = await session.execute(
            select(Task).where(Task.job_id == job.id)
        )
        t = result.scalar_one()
        assert t.status == TaskStatus.FAILED
        assert t.attempt == 2  # 0 + 2 retries
        assert t.max_retries == 2
        assert t.error is not None

    # Job should be FAILED
    async with get_sql_session() as session:
        result = await session.execute(
            select(Job).where(Job.id == job.id)
        )
        j = result.scalar_one()
        assert j.status == JobStatus.FAILED


async def test_worker_no_retries_immediate_fail(orch_ctx_no_ch, fast_poll):
    """Task with max_retries=0 fails immediately (no retry)."""
    await _cancel_all_pending_tasks()

    job = await create_job(
        "test_no_retry",
        create_task(
            "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
            max_retries=0,
        ),
    )

    await mp_worker_main_loop(
        max_tasks=1,
        install_signal_handlers=False,
        max_empty_polls=1,
    )

    # Task should be FAILED immediately (no retries)
    async with get_sql_session() as session:
        result = await session.execute(
            select(Task).where(Task.job_id == job.id)
        )
        t = result.scalar_one()
        assert t.status == TaskStatus.FAILED
        assert t.attempt == 0  # Never retried
        assert t.error is not None

    # Job should be FAILED
    async with get_sql_session() as session:
        result = await session.execute(
            select(Job).where(Job.id == job.id)
        )
        j = result.scalar_one()
        assert j.status == JobStatus.FAILED


async def test_worker_retry_succeeds_on_third_attempt(orch_ctx_no_ch, tmp_path, fast_poll):
    """Flaky task fails twice, succeeds on the third attempt via retry."""
    await _cancel_all_pending_tasks()

    counter_file = str(tmp_path / "counter.txt")

    t = create_task(
        "aaiclick.orchestration.fixtures.sample_tasks.flaky_task",
        {"counter_file": counter_file},
        max_retries=2,
    )
    job = await create_job("test_retry_succeeds", t)

    tasks_executed = await mp_worker_main_loop(
        max_tasks=1,
        install_signal_handlers=False,
        max_empty_polls=2,
    )

    assert tasks_executed == 1  # Task eventually succeeded

    # Task should be COMPLETED after 3 attempts
    async with get_sql_session() as session:
        result = await session.execute(
            select(Task).where(Task.job_id == job.id)
        )
        t = result.scalar_one()
        assert t.status == TaskStatus.COMPLETED
        assert t.attempt == 2  # Succeeded on attempt 2 (third try)

    # Job should be COMPLETED
    async with get_sql_session() as session:
        result = await session.execute(
            select(Job).where(Job.id == job.id)
        )
        j = result.scalar_one()
        assert j.status == JobStatus.COMPLETED
