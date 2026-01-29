"""Tests for dependency operators."""

from sqlmodel import select

from aaiclick.orchestration import (
    DEPENDENCY_GROUP,
    DEPENDENCY_TASK,
    Dependency,
    Group,
    OrchContext,
    create_job,
)
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
    deps = task2.previous_dependencies
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
    deps = task1.previous_dependencies
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
    deps2 = task2.previous_dependencies
    assert len(deps2) == 1
    assert deps2[0].previous_id == task1.id

    # task3 depends on task2
    deps3 = task3.previous_dependencies
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
        deps = task.previous_dependencies
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
    deps = task4.previous_dependencies
    assert len(deps) == 3
    dep_ids = {dep.previous_id for dep in deps}
    assert dep_ids == {task1.id, task2.id, task3.id}


async def test_group_rshift_creates_dependency():
    """Test that >> operator works with groups."""
    group1 = Group(id=get_snowflake_id(), name="group1")
    task1 = create_task("module.func1")

    group1 >> task1

    # task1 depends on group1
    deps = task1.previous_dependencies
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
    deps = group1.previous_dependencies
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
    deps = group2.previous_dependencies
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
