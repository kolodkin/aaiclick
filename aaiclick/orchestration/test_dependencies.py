"""Tests for dependency operators."""

from sqlmodel import select

from ..data.data_context import data_context
from ..snowflake import get_snowflake_id
from . import get_job_result, task_result
from .decorators import job, task
from .execution.debug import ajob_test
from .factories import create_job, create_task
from .jobs import get_task
from .models import DEPENDENCY_GROUP, DEPENDENCY_TASK, JOB_COMPLETED, Dependency, Group
from .orch_context import commit_tasks, get_sql_session


@task
def produce(value: int) -> int:
    return value


@task
def add(left: int, right: list[int]) -> int:
    return left + right[0]


@job("test_same_upstream_twice")
def same_upstream_twice_pipeline(value: int):
    upstream = produce(value=value)
    consumer = add(left=upstream, right=[upstream])
    upstream >> consumer
    return task_result(data=consumer, tasks=[upstream, consumer])


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


async def test_repeated_edge_is_recorded_once():
    """``a >> b`` twice, or ``a >> [b, b]``, yields one Dependency (composite PK)."""
    task1 = create_task("module.func1")
    task2 = create_task("module.func2")

    task1 >> task2
    task1 >> [task2, task2]

    assert [d.previous_id for d in task2.previous_dependencies] == [task1.id]


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
    for downstream in [task2, task3, task4]:
        deps = downstream.previous_dependencies
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


async def test_commit_tasks_persists_upstream_graph(orch_ctx):
    """Passing only the terminal task to commit_tasks() should persist all upstream tasks.

    This is a regression test for the bug where intermediate tasks were never
    saved to the DB when the developer only returned the terminal task from a
    @job function, causing workers to fail with 'Upstream task not found'.
    """
    job = await create_job(
        "test_graph_persist_job",
        "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
    )

    # Build a three-task pipeline: raw >> transform >> report
    raw = create_task("aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    transform = create_task("aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    report = create_task("aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    raw >> transform >> report

    # Only pass the terminal task — framework must auto-collect the full graph
    await commit_tasks(report, job_id=job.id)

    # All three tasks must exist in the DB
    for node in (raw, transform, report):
        assert await get_task(node.id) is not None, f"Task {node.id} ({node.entrypoint}) was not persisted"

    # Dependencies must also be saved
    async with get_sql_session() as session:
        dep1 = await session.execute(
            select(Dependency).where(Dependency.previous_id == raw.id, Dependency.next_id == transform.id)
        )
        assert dep1.scalar_one_or_none() is not None

        dep2 = await session.execute(
            select(Dependency).where(Dependency.previous_id == transform.id, Dependency.next_id == report.id)
        )
        assert dep2.scalar_one_or_none() is not None


async def test_apply_saves_dependencies(orch_ctx):
    """Test that commit_tasks() saves dependencies to database."""
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
    await commit_tasks([task1, task2], job_id=job.id)

    # Verify dependency was saved
    async with get_sql_session() as session:
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


async def test_same_upstream_in_two_kwargs_runs(orch_ctx):
    """A job passing one upstream to two kwargs (and wiring it with ``>>``) runs.

    Each would add a ``Dependency`` on the same edge; a duplicate row collides
    on the composite primary key and fails the commit with ``IntegrityError``.
    """
    j = await ajob_test(same_upstream_twice_pipeline, value=21)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        assert await get_job_result(j) == 42


async def test_commit_tasks_persists_group_members(orch_ctx):
    """A group reached through ``group >> consumer`` is committed with its members.

    ``add_task`` is the only membership call; the member rows must still carry
    the group id so the scheduler and pin fan-out see them.
    """
    job = await create_job("test_group_members_job", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    group = Group(id=get_snowflake_id(), name="producers")
    members = [create_task("aaiclick.orchestration.fixtures.sample_tasks.simple_task") for _ in range(2)]
    for member in members:
        group.add_task(member)
    consumer = create_task("aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    group >> consumer

    await commit_tasks(consumer, job_id=job.id)

    for member in members:
        row = await get_task(member.id)
        assert row is not None, f"Member {member.id} was not persisted"
        assert row.group_id == group.id
