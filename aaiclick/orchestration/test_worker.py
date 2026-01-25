"""Tests for worker management and task claiming."""

import asyncio

import pytest

from aaiclick.orchestration import (
    Job,
    JobStatus,
    OrchContext,
    Task,
    TaskStatus,
    WorkerStatus,
    claim_next_task,
    create_job,
    deregister_worker,
    get_worker,
    list_workers,
    register_worker,
    worker_heartbeat,
    worker_main_loop,
)
from aaiclick.orchestration.context import get_orch_context_session


async def test_register_worker():
    """Test worker registration."""
    async with OrchContext():
        worker = await register_worker()

        assert worker.id is not None
        assert worker.status == WorkerStatus.ACTIVE
        assert worker.hostname is not None
        assert worker.pid is not None
        assert worker.tasks_completed == 0
        assert worker.tasks_failed == 0

        # Verify in database
        db_worker = await get_worker(worker.id)
        assert db_worker is not None
        assert db_worker.status == WorkerStatus.ACTIVE


async def test_register_worker_custom_values():
    """Test worker registration with custom hostname and pid."""
    async with OrchContext():
        worker = await register_worker(hostname="test-host", pid=12345)

        assert worker.hostname == "test-host"
        assert worker.pid == 12345


async def test_worker_heartbeat():
    """Test worker heartbeat updates."""
    async with OrchContext():
        worker = await register_worker()
        original_heartbeat = worker.last_heartbeat

        # Wait a bit and send heartbeat
        await asyncio.sleep(0.1)
        result = await worker_heartbeat(worker.id)

        assert result is True

        # Verify heartbeat was updated
        db_worker = await get_worker(worker.id)
        assert db_worker.last_heartbeat > original_heartbeat


async def test_worker_heartbeat_nonexistent():
    """Test heartbeat for non-existent worker returns False."""
    async with OrchContext():
        result = await worker_heartbeat(999999999)
        assert result is False


async def test_deregister_worker():
    """Test worker deregistration."""
    async with OrchContext():
        worker = await register_worker()

        result = await deregister_worker(worker.id)
        assert result is True

        # Verify status changed
        db_worker = await get_worker(worker.id)
        assert db_worker.status == WorkerStatus.STOPPED


async def test_deregister_worker_nonexistent():
    """Test deregistering non-existent worker returns False."""
    async with OrchContext():
        result = await deregister_worker(999999999)
        assert result is False


async def test_list_workers():
    """Test listing workers."""
    async with OrchContext():
        # Register multiple workers
        worker1 = await register_worker(hostname="host1", pid=1001)
        worker2 = await register_worker(hostname="host2", pid=1002)

        # Deregister one
        await deregister_worker(worker1.id)

        # List all workers
        all_workers = await list_workers()
        assert len(all_workers) >= 2

        # List only active workers
        active_workers = await list_workers(status=WorkerStatus.ACTIVE)
        active_ids = [w.id for w in active_workers]
        assert worker2.id in active_ids

        # List only stopped workers
        stopped_workers = await list_workers(status=WorkerStatus.STOPPED)
        stopped_ids = [w.id for w in stopped_workers]
        assert worker1.id in stopped_ids


async def test_claim_next_task_no_tasks():
    """Test claiming when no tasks are available."""
    async with OrchContext():
        worker = await register_worker()

        task = await claim_next_task(worker.id)
        assert task is None


async def test_claim_next_task_basic():
    """Test basic task claiming."""
    async with OrchContext():
        # Create a job with a task
        job = await create_job(
            "test_claim_job",
            "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
        )

        # Register worker and claim task
        worker = await register_worker()
        task = await claim_next_task(worker.id)

        assert task is not None
        assert task.job_id == job.id
        assert task.status == TaskStatus.RUNNING
        assert task.worker_id == worker.id
        assert task.claimed_at is not None

        # Verify job status changed to RUNNING
        async with get_orch_context_session() as session:
            from sqlmodel import select

            result = await session.execute(select(Job).where(Job.id == job.id))
            db_job = result.scalar_one()
            assert db_job.status == JobStatus.RUNNING
            assert db_job.started_at is not None


async def test_claim_next_task_skip_locked():
    """Test that concurrent workers don't claim the same task."""
    async with OrchContext():
        # Create multiple jobs with tasks
        job1 = await create_job(
            "test_claim_job1",
            "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
        )
        job2 = await create_job(
            "test_claim_job2",
            "aaiclick.orchestration.fixtures.sample_tasks.async_task",
        )

        # Register two workers
        worker1 = await register_worker(hostname="worker1", pid=1001)
        worker2 = await register_worker(hostname="worker2", pid=1002)

        # Both workers claim tasks concurrently
        task1, task2 = await asyncio.gather(
            claim_next_task(worker1.id),
            claim_next_task(worker2.id),
        )

        # Each worker should get a different task
        assert task1 is not None
        assert task2 is not None
        assert task1.id != task2.id
        assert task1.worker_id == worker1.id
        assert task2.worker_id == worker2.id


async def test_claim_next_task_prioritizes_oldest_job(monkeypatch, tmpdir):
    """Test that older running jobs are prioritized."""
    monkeypatch.setenv("AAICLICK_LOG_DIR", str(tmpdir))

    async with OrchContext():
        # Create first job and start it
        job1 = await create_job(
            "test_job_old",
            "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
        )

        # Create worker and claim the first task to start job1
        worker = await register_worker()
        task1 = await claim_next_task(worker.id)
        assert task1 is not None

        # Execute and complete the task
        from aaiclick.orchestration import execute_task

        await execute_task(task1)

        # Mark task as completed
        from aaiclick.orchestration.claiming import update_task_status

        await update_task_status(task1.id, TaskStatus.COMPLETED)

        # Now create a second job (newer)
        job2 = await create_job(
            "test_job_new",
            "aaiclick.orchestration.fixtures.sample_tasks.async_task",
        )

        # Add another task to job1 (which is already running)
        from aaiclick.orchestration.factories import create_task as factory_create_task
        from aaiclick.orchestration.context import get_orch_context

        ctx = get_orch_context()
        extra_task = factory_create_task(
            "aaiclick.orchestration.fixtures.sample_tasks.simple_task"
        )
        await ctx.apply(extra_task, job_id=job1.id)

        # Claim next task - should prioritize job1 (older running job)
        next_task = await claim_next_task(worker.id)
        assert next_task is not None
        assert next_task.job_id == job1.id


async def test_worker_main_loop_executes_tasks(monkeypatch, tmpdir):
    """Test that worker main loop executes tasks."""
    monkeypatch.setenv("AAICLICK_LOG_DIR", str(tmpdir))

    async with OrchContext():
        # Create a job
        job = await create_job(
            "test_main_loop_job",
            "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
        )

        # Run worker with max_tasks=1
        tasks_executed = await worker_main_loop(max_tasks=1)

        assert tasks_executed == 1

        # Verify task was completed
        async with get_orch_context_session() as session:
            from sqlmodel import select

            result = await session.execute(
                select(Task).where(Task.job_id == job.id)
            )
            task = result.scalar_one()
            assert task.status == TaskStatus.COMPLETED


async def test_worker_main_loop_handles_failures(monkeypatch, tmpdir):
    """Test that worker main loop handles task failures."""
    monkeypatch.setenv("AAICLICK_LOG_DIR", str(tmpdir))

    async with OrchContext():
        # Create a job with a failing task
        job = await create_job(
            "test_failing_job",
            "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
        )

        # Run worker with max_tasks=1
        tasks_executed = await worker_main_loop(max_tasks=1)

        # Task was attempted
        assert tasks_executed == 0  # Failed tasks don't count as executed

        # Verify task was marked as failed
        async with get_orch_context_session() as session:
            from sqlmodel import select

            result = await session.execute(
                select(Task).where(Task.job_id == job.id)
            )
            task = result.scalar_one()
            assert task.status == TaskStatus.FAILED
            assert task.error is not None
