"""Tests for dependency operators and dependency-aware task claiming."""

from aaiclick.orchestration import (
    DEPENDENCY_GROUP,
    DEPENDENCY_TASK,
    Dependency,
    Group,
    OrchContext,
    Task,
    TaskStatus,
    claim_next_task,
    create_job,
    register_worker,
)
from aaiclick.orchestration.claiming import update_task_status
from aaiclick.orchestration.context import get_orch_context_session
from aaiclick.orchestration.factories import create_task
from aaiclick.snowflake_id import get_snowflake_id


async def test_task_rshift_creates_dependency():
    """Test that >> operator creates dependency (A >> B means B depends on A)."""
    task1 = create_task("module.func1")
    task2 = create_task("module.func2")

    result = task1 >> task2

    # Result should be the right operand
    assert result is task2

    # task2 should have a pending dependency on task1
    deps = task2.pending_dependencies
    assert len(deps) == 1
    dep = deps[0]
    assert dep.previous_id == task1.id
    assert dep.previous_type == DEPENDENCY_TASK
    assert dep.next_id == task2.id
    assert dep.next_type == DEPENDENCY_TASK


async def test_task_lshift_creates_dependency():
    """Test that << operator creates dependency (A << B means A depends on B)."""
    task1 = create_task("module.func1")
    task2 = create_task("module.func2")

    result = task1 << task2

    # Result should be the left operand
    assert result is task1

    # task1 should have a pending dependency on task2
    deps = task1.pending_dependencies
    assert len(deps) == 1
    dep = deps[0]
    assert dep.previous_id == task2.id
    assert dep.previous_type == DEPENDENCY_TASK
    assert dep.next_id == task1.id
    assert dep.next_type == DEPENDENCY_TASK


async def test_task_chained_rshift():
    """Test chained >> operators (A >> B >> C)."""
    task1 = create_task("module.func1")
    task2 = create_task("module.func2")
    task3 = create_task("module.func3")

    task1 >> task2 >> task3

    # task2 depends on task1
    deps2 = task2.pending_dependencies
    assert len(deps2) == 1
    assert deps2[0].previous_id == task1.id

    # task3 depends on task2
    deps3 = task3.pending_dependencies
    assert len(deps3) == 1
    assert deps3[0].previous_id == task2.id


async def test_task_fanout():
    """Test fan-out: A >> [B, C, D] means B, C, D all depend on A."""
    task1 = create_task("module.func1")
    task2 = create_task("module.func2")
    task3 = create_task("module.func3")
    task4 = create_task("module.func4")

    task1 >> [task2, task3, task4]

    # All tasks should depend on task1
    for task in [task2, task3, task4]:
        deps = task.pending_dependencies
        assert len(deps) == 1
        assert deps[0].previous_id == task1.id


async def test_task_fanin():
    """Test fan-in: [A, B, C] >> D means D depends on A, B, and C."""
    task1 = create_task("module.func1")
    task2 = create_task("module.func2")
    task3 = create_task("module.func3")
    task4 = create_task("module.func4")

    [task1, task2, task3] >> task4

    # task4 should depend on all three
    deps = task4.pending_dependencies
    assert len(deps) == 3
    dep_ids = {dep.previous_id for dep in deps}
    assert dep_ids == {task1.id, task2.id, task3.id}


async def test_group_rshift_creates_dependency():
    """Test that >> operator works with groups."""
    group1 = Group(id=get_snowflake_id(), name="group1")
    task1 = create_task("module.func1")

    group1 >> task1

    # task1 depends on group1
    deps = task1.pending_dependencies
    assert len(deps) == 1
    dep = deps[0]
    assert dep.previous_id == group1.id
    assert dep.previous_type == DEPENDENCY_GROUP
    assert dep.next_id == task1.id
    assert dep.next_type == DEPENDENCY_TASK


async def test_task_rshift_to_group():
    """Test task >> group creates dependency."""
    task1 = create_task("module.func1")
    group1 = Group(id=get_snowflake_id(), name="group1")

    task1 >> group1

    # group1 depends on task1
    deps = group1.pending_dependencies
    assert len(deps) == 1
    dep = deps[0]
    assert dep.previous_id == task1.id
    assert dep.previous_type == DEPENDENCY_TASK
    assert dep.next_id == group1.id
    assert dep.next_type == DEPENDENCY_GROUP


async def test_group_to_group_dependency():
    """Test group >> group creates dependency."""
    group1 = Group(id=get_snowflake_id(), name="group1")
    group2 = Group(id=get_snowflake_id(), name="group2")

    group1 >> group2

    # group2 depends on group1
    deps = group2.pending_dependencies
    assert len(deps) == 1
    dep = deps[0]
    assert dep.previous_id == group1.id
    assert dep.previous_type == DEPENDENCY_GROUP
    assert dep.next_id == group2.id
    assert dep.next_type == DEPENDENCY_GROUP


async def test_apply_saves_dependencies():
    """Test that apply() saves dependencies to database."""
    async with OrchContext() as ctx:
        # Create job
        job = await create_job(
            "test_deps_job",
            "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
        )

        # Create tasks with dependency
        task1 = create_task("aaiclick.orchestration.fixtures.sample_tasks.simple_task")
        task2 = create_task("aaiclick.orchestration.fixtures.sample_tasks.async_task")
        task1 >> task2  # task2 depends on task1

        # Apply tasks to job
        await ctx.apply([task1, task2], job_id=job.id)

        # Verify dependency was saved
        async with get_orch_context_session() as session:
            from sqlmodel import select

            result = await session.execute(
                select(Dependency).where(
                    Dependency.previous_id == task1.id,
                    Dependency.next_id == task2.id,
                )
            )
            dep = result.scalar_one_or_none()
            assert dep is not None
            assert dep.previous_type == DEPENDENCY_TASK
            assert dep.next_type == DEPENDENCY_TASK


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
            from sqlmodel import select

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
                from sqlmodel import select

                result = await session.execute(
                    select(Task).where(Task.job_id == job.id)
                )
                initial_task = result.scalar_one()

            # Create a group and add initial_task to it
            group1 = Group(id=get_snowflake_id(), name="group1")
            await ctx.apply(group1, job_id=job.id)

            # Update initial_task to be in group1
            async with get_orch_context_session() as session:
                from sqlalchemy import text

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

