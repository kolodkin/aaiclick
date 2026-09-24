"""Tests for dependency operators, asserted on the ``Dependency`` rows ``commit_tasks`` persists."""

import pytest
from sqlmodel import select

from ..data.data_context import data_context
from ..snowflake import get_snowflake_id
from . import get_job_result, task_result
from .decorators import job, task
from .execution.debug import ajob_test
from .factories import create_job, create_task
from .fixtures.sample_tasks import simple_task
from .jobs import get_task
from .models import DEPENDENCY_GROUP, DEPENDENCY_TASK, JOB_COMPLETED, Dependency, Group, Task
from .orch_context import commit_tasks, get_sql_session

Edge = tuple[int, str, int, str]


@task
def produce(value: int) -> int:
    return value


@task
def add(left: int, right: list[int], extra: dict[str, int]) -> int:
    return left + right[0] + extra["nested"]


@job("test_same_upstream_twice")
def same_upstream_twice_pipeline(value: int):
    upstream = produce(value=value)
    consumer = add(left=upstream, right=[upstream], extra={"nested": upstream})
    upstream >> consumer
    return task_result(data=consumer, tasks=[upstream, consumer])


def _new_task() -> Task:
    return create_task(simple_task)


def _new_group() -> Group:
    return Group(id=get_snowflake_id(), name="group")


async def _persisted_edges() -> set[Edge]:
    """Every persisted dependency as ``(previous_id, previous_type, next_id, next_type)``."""
    async with get_sql_session() as session:
        rows = (await session.execute(select(Dependency))).scalars().all()
    return {(d.previous_id, d.previous_type, d.next_id, d.next_type) for d in rows}


# Each node maker with the ``Dependency`` type its rows carry; stacked below
# into all four previous/next combinations (ids read ``<previous>-<next>``).
_NODE_KINDS = [
    pytest.param(_new_task, DEPENDENCY_TASK, id="task"),
    pytest.param(_new_group, DEPENDENCY_GROUP, id="group"),
]


@pytest.mark.parametrize("make_next, next_type", _NODE_KINDS)
@pytest.mark.parametrize("make_previous, previous_type", _NODE_KINDS)
async def test_rshift_persists_dependency(orch_ctx, make_previous, make_next, previous_type, next_type):
    """``A >> B`` means B depends on A and returns the right operand."""
    job = await create_job("test_rshift_job", simple_task)
    previous, next_ = make_previous(), make_next()

    assert (previous >> next_) is next_

    await commit_tasks([previous, next_], job_id=job.id)
    assert await _persisted_edges() == {(previous.id, previous_type, next_.id, next_type)}


async def test_lshift_persists_dependency(orch_ctx):
    """``A << B`` means A depends on B and returns the left operand."""
    job = await create_job("test_lshift_job", simple_task)
    task1, task2 = _new_task(), _new_task()

    assert (task1 << task2) is task1

    await commit_tasks([task1, task2], job_id=job.id)
    assert await _persisted_edges() == {(task2.id, DEPENDENCY_TASK, task1.id, DEPENDENCY_TASK)}


async def test_fanout_persists_dependency_per_target(orch_ctx):
    """``A >> [B, C, D]`` means B, C and D all depend on A."""
    job = await create_job("test_fanout_job", simple_task)
    source = _new_task()
    targets: list[Task | Group] = [_new_task(), _new_task(), _new_task()]

    source >> targets

    await commit_tasks(targets, job_id=job.id)
    assert await _persisted_edges() == {(source.id, DEPENDENCY_TASK, t.id, DEPENDENCY_TASK) for t in targets}


async def test_fanin_persists_dependency_per_source(orch_ctx):
    """``[A, B, C] >> D`` means D depends on A, B and C."""
    job = await create_job("test_fanin_job", simple_task)
    sources = [_new_task(), _new_task(), _new_task()]
    sink = _new_task()

    sources >> sink

    await commit_tasks(sink, job_id=job.id)
    assert await _persisted_edges() == {(s.id, DEPENDENCY_TASK, sink.id, DEPENDENCY_TASK) for s in sources}


async def test_commit_tasks_persists_upstream_graph(orch_ctx):
    """commit_tasks() on only the terminal task of a ``>>`` chain persists every upstream task and edge.

    This is a regression test for the bug where intermediate tasks were never
    saved to the DB when the developer only returned the terminal task from a
    @job function, causing workers to fail with 'Upstream task not found'.
    """
    job = await create_job("test_graph_persist_job", simple_task)

    raw, transform, report = _new_task(), _new_task(), _new_task()
    raw >> transform >> report

    # Only pass the terminal task — framework must auto-collect the full graph
    await commit_tasks(report, job_id=job.id)

    for node in (raw, transform, report):
        assert await get_task(node.id) is not None, f"Task {node.id} ({node.entrypoint}) was not persisted"

    assert await _persisted_edges() == {
        (raw.id, DEPENDENCY_TASK, transform.id, DEPENDENCY_TASK),
        (transform.id, DEPENDENCY_TASK, report.id, DEPENDENCY_TASK),
    }


async def test_same_upstream_in_several_kwargs_runs(orch_ctx):
    """One upstream passed to three kwargs (bare, in a list, in a dict) and wired with ``>>`` runs.

    Each would add a ``Dependency`` on the same edge; a duplicate row collides
    on the composite primary key and fails the commit with ``IntegrityError``.
    """
    j = await ajob_test(same_upstream_twice_pipeline, value=21)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        assert await get_job_result(j) == 63


async def test_commit_tasks_persists_group_members(orch_ctx):
    """A group reached through ``group >> consumer`` is committed with its members.

    ``add_task`` is the only membership call; the member rows must still carry
    the group id so the scheduler and pin fan-out see them.
    """
    job = await create_job("test_group_members_job", simple_task)
    group = Group(id=get_snowflake_id(), name="producers")
    members = [_new_task() for _ in range(2)]
    for member in members:
        group.add_task(member)
    consumer = _new_task()
    group >> consumer

    await commit_tasks(consumer, job_id=job.id)

    for member in members:
        row = await get_task(member.id)
        assert row is not None, f"Member {member.id} was not persisted"
        assert row.group_id == group.id
