"""Tests for worker management."""

import asyncio

from sqlmodel import select

from aaiclick.orchestration import (
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


async def test_worker_main_loop_executes_tasks(monkeypatch, tmpdir):
    """Test that worker main loop executes tasks."""
    monkeypatch.setenv("AAICLICK_LOG_DIR", str(tmpdir))

    async with OrchContext():
        # Clear any pending tasks from previous tests first
        cleanup_worker = await register_worker()
        while True:
            old_task = await claim_next_task(cleanup_worker.id)
            if old_task is None:
                break
        await deregister_worker(cleanup_worker.id)

        # Create a job
        job = await create_job(
            "test_main_loop_job",
            "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
        )

        # Run worker with max_tasks=1 (disable signal handlers, limit empty polls for test)
        tasks_executed = await worker_main_loop(
            max_tasks=1,
            install_signal_handlers=False,
            max_empty_polls=5,
        )

        assert tasks_executed == 1

        # Verify task was completed
        async with get_orch_context_session() as session:
            result = await session.execute(
                select(Task).where(Task.job_id == job.id)
            )
            task = result.scalar_one()
            assert task.status == TaskStatus.COMPLETED


async def test_worker_main_loop_handles_failures(monkeypatch, tmpdir):
    """Test that worker main loop handles task failures."""
    monkeypatch.setenv("AAICLICK_LOG_DIR", str(tmpdir))

    async with OrchContext():
        # Clear any pending tasks from previous tests first
        cleanup_worker = await register_worker()
        while True:
            old_task = await claim_next_task(cleanup_worker.id)
            if old_task is None:
                break
        await deregister_worker(cleanup_worker.id)

        # Create a job with a failing task
        job = await create_job(
            "test_failing_job",
            "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
        )

        # Run worker with max_tasks=1 (disable signal handlers, limit empty polls for test)
        tasks_executed = await worker_main_loop(
            max_tasks=1,
            install_signal_handlers=False,
            max_empty_polls=5,
        )

        # Task was attempted but failed
        assert tasks_executed == 0  # Failed tasks don't count as executed

        # Verify task was marked as failed
        async with get_orch_context_session() as session:
            result = await session.execute(
                select(Task).where(Task.job_id == job.id)
            )
            task = result.scalar_one()
            assert task.status == TaskStatus.FAILED
            assert task.error is not None
