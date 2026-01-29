"""Tests for task claiming functionality."""

import asyncio

from sqlalchemy import text
from sqlmodel import select

from aaiclick.orchestration import (
    DEPENDENCY_GROUP,
    Group,
    Job,
    JobStatus,
    OrchContext,
    Task,
    TaskStatus,
    claim_next_task,
    create_job,
    deregister_worker,
    execute_task,
    register_worker,
)
from aaiclick.orchestration.claiming import update_task_status
from aaiclick.orchestration.context import get_orch_context, get_orch_context_session
from aaiclick.orchestration.factories import create_task
from aaiclick.snowflake_id import get_snowflake_id


async def test_claim_next_task_no_tasks():
    """Test claiming when no tasks are available for the worker."""
    async with OrchContext():
        worker = await register_worker()

        # Claim all available tasks first to get to empty state
        while True:
            task = await claim_next_task(worker.id)
            if task is None:
                break

        # Now verify no more tasks are available
        task = await claim_next_task(worker.id)
        assert task is None


async def test_claim_next_task_basic():
    """Test basic task claiming."""
    async with OrchContext():
        # Register worker
        worker = await register_worker()

        # Clear any pending tasks from previous tests
        while True:
            old_task = await claim_next_task(worker.id)
            if old_task is None:
                break

        # Create a job with a task
        job = await create_job(
            "test_claim_job",
            "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
        )

        # Claim the task we just created
        task = await claim_next_task(worker.id)

        assert task is not None
        assert task.job_id == job.id
        assert task.status == TaskStatus.RUNNING
        assert task.worker_id == worker.id
        assert task.claimed_at is not None

        # Verify job status changed to RUNNING
        async with get_orch_context_session() as session:
            result = await session.execute(select(Job).where(Job.id == job.id))
            db_job = result.scalar_one()
            assert db_job.status == JobStatus.RUNNING
            assert db_job.started_at is not None


async def test_claim_next_task_skip_locked():
    """Test that concurrent workers don't claim the same task."""
    async with OrchContext():
        # Register workers first
        worker1 = await register_worker(hostname="worker1", pid=1001)
        worker2 = await register_worker(hostname="worker2", pid=1002)

        # Clear any pending tasks from previous tests
        while True:
            old_task = await claim_next_task(worker1.id)
            if old_task is None:
                break

        # Create multiple jobs with tasks
        job1 = await create_job(
            "test_claim_job1",
            "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
        )
        job2 = await create_job(
            "test_claim_job2",
            "aaiclick.orchestration.fixtures.sample_tasks.async_task",
        )

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
        # Create worker first
        worker = await register_worker()

        # Clear any pending tasks from previous tests
        while True:
            old_task = await claim_next_task(worker.id)
            if old_task is None:
                break

        # Create first job and start it
        job1 = await create_job(
            "test_job_old",
            "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
        )

        # Claim the first task to start job1
        task1 = await claim_next_task(worker.id)
        assert task1 is not None
        assert task1.job_id == job1.id

        # Execute and complete the task
        await execute_task(task1)

        # Mark task as completed
        await update_task_status(task1.id, TaskStatus.COMPLETED)

        # Now create a second job (newer)
        job2 = await create_job(
            "test_job_new",
            "aaiclick.orchestration.fixtures.sample_tasks.async_task",
        )

        # Add another task to job1 (which is already running)
        ctx = get_orch_context()
        extra_task = create_task(
            "aaiclick.orchestration.fixtures.sample_tasks.simple_task"
        )
        await ctx.apply(extra_task, job_id=job1.id)

        # Claim next task - should prioritize job1 (older running job)
        next_task = await claim_next_task(worker.id)
        assert next_task is not None
        assert next_task.job_id == job1.id


async def test_claim_respects_task_dependency():
    """Test that claim_next_task respects task -> task dependencies."""
    async with OrchContext():
        # Register worker
        worker = await register_worker()

        # Clear any pending tasks from previous tests
        while True:
            old_task = await claim_next_task(worker.id)
            if old_task is None:
                break

        # Create job with two dependent tasks
        job = await create_job(
            "test_deps_claim_job",
            "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
        )

        # Get the initial task created by create_job
        async with get_orch_context_session() as session:
            result = await session.execute(
                select(Task).where(Task.job_id == job.id)
            )
            initial_task = result.scalar_one()

        # Create a second task that depends on the first
        ctx = OrchContext()
        await ctx.__aenter__()
        try:
            task2 = create_task("aaiclick.orchestration.fixtures.sample_tasks.async_task")
            initial_task >> task2  # task2 depends on initial_task
            await ctx.apply(task2, job_id=job.id)
        finally:
            await ctx.__aexit__(None, None, None)

        # First claim should get initial_task (no dependencies)
        claimed1 = await claim_next_task(worker.id)
        assert claimed1 is not None
        assert claimed1.id == initial_task.id

        # Second claim should return None (task2 depends on uncompleted initial_task)
        claimed2 = await claim_next_task(worker.id)
        assert claimed2 is None

        # Mark initial_task as completed
        await update_task_status(initial_task.id, TaskStatus.COMPLETED)

        # Now task2 should be claimable
        claimed3 = await claim_next_task(worker.id)
        assert claimed3 is not None
        assert claimed3.id == task2.id


async def test_claim_respects_group_dependency(monkeypatch, tmpdir):
    """Test that claim_next_task respects group -> task dependencies."""
    monkeypatch.setenv("AAICLICK_LOG_DIR", str(tmpdir))

    async with OrchContext():
        # Register worker
        worker = await register_worker()

        # Clear any pending tasks from previous tests
        while True:
            old_task = await claim_next_task(worker.id)
            if old_task is None:
                break

        # Create job
        job = await create_job(
            "test_group_deps_job",
            "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
        )

        # Get the initial task and put it in a group
        ctx = OrchContext()
        await ctx.__aenter__()
        try:
            # Get initial task
            async with get_orch_context_session() as session:
                result = await session.execute(
                    select(Task).where(Task.job_id == job.id)
                )
                initial_task = result.scalar_one()

            # Create a group and add initial_task to it
            group1 = Group(id=get_snowflake_id(), name="group1")
            await ctx.apply(group1, job_id=job.id)

            # Update initial_task to be in group1
            async with get_orch_context_session() as session:
                await session.execute(
                    text("UPDATE tasks SET group_id = :group_id WHERE id = :task_id"),
                    {"group_id": group1.id, "task_id": initial_task.id},
                )
                await session.commit()

            # Create a task that depends on group1
            task2 = create_task("aaiclick.orchestration.fixtures.sample_tasks.async_task")
            group1 >> task2
            await ctx.apply(task2, job_id=job.id)

        finally:
            await ctx.__aexit__(None, None, None)

        # Claim initial_task (in group1)
        claimed1 = await claim_next_task(worker.id)
        assert claimed1 is not None
        assert claimed1.id == initial_task.id

        # task2 should not be claimable (depends on group1 which has uncompleted task)
        claimed2 = await claim_next_task(worker.id)
        assert claimed2 is None

        # Complete initial_task
        await update_task_status(initial_task.id, TaskStatus.COMPLETED)

        # Now task2 should be claimable (group1 is complete)
        claimed3 = await claim_next_task(worker.id)
        assert claimed3 is not None
        assert claimed3.id == task2.id
