"""Tests for clear_task() — reset a task and its downstream to PENDING."""

import pytest
from sqlmodel import select

from ...datetime_utils import utc_now
from ...snowflake import get_snowflake_id
from ..factories import create_job, create_task
from ..models import (
    DEPENDENCY_GROUP,
    DEPENDENCY_TASK,
    JOB_FAILED,
    JOB_RUNNING,
    TASK_CANCELLED,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_PENDING,
    TASK_PENDING_CLEANUP,
    TASK_RUNNING,
    Dependency,
    Group,
    Job,
    Task,
    TaskStatus,
)
from ..orch_context import get_sql_session
from .claiming import (
    TaskNotFound,
    check_run_aborted,
    clear_task,
    update_task_status,
)
from .execution_worker import _set_pending_cleanup, register_execution_worker

EP = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"


def _task(job_id: int, *, status: TaskStatus = TASK_COMPLETED, group_id: int | None = None) -> Task:
    """Build (uncommitted) a Task with a given status and optional group."""
    t = create_task(EP)
    t.job_id = job_id
    t.status = status
    t.group_id = group_id
    return t


async def _insert(*objs) -> None:
    async with get_sql_session() as session:
        session.add_all(objs)
        await session.commit()


async def _status(task_id: int) -> str:
    async with get_sql_session() as session:
        return (await session.execute(select(Task.status).where(Task.id == task_id))).scalar_one()


async def _get(task_id: int) -> Task:
    async with get_sql_session() as session:
        return (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()


def _dep(prev_id: int, prev_type: str, next_id: int, next_type: str) -> Dependency:
    return Dependency(previous_id=prev_id, previous_type=prev_type, next_id=next_id, next_type=next_type)


async def test_clear_resets_target_only(orch_ctx):
    """A leaf task with no downstream is reset to PENDING with epoch bumped."""
    job = await create_job("clear_leaf", EP)
    t = _task(job.id, status=TASK_COMPLETED)
    await _insert(t)

    cleared, _ = await clear_task(t.id)

    assert cleared == [t.id]
    reset = await _get(t.id)
    assert reset.status == TASK_PENDING
    assert reset.run_epoch == 1


async def test_clear_resets_transitive_chain(orch_ctx):
    """Clearing the head of A→B→C resets all three; an unrelated task is untouched."""
    job = await create_job("clear_chain", EP)
    a, b, c = (_task(job.id, status=TASK_COMPLETED) for _ in range(3))
    unrelated = _task(job.id, status=TASK_COMPLETED)
    await _insert(
        a,
        b,
        c,
        unrelated,
        _dep(a.id, DEPENDENCY_TASK, b.id, DEPENDENCY_TASK),
        _dep(b.id, DEPENDENCY_TASK, c.id, DEPENDENCY_TASK),
    )

    cleared, _ = await clear_task(a.id)

    assert set(cleared) == {a.id, b.id, c.id}
    for tid in (a.id, b.id, c.id):
        assert await _status(tid) == TASK_PENDING
    assert await _status(unrelated.id) == TASK_COMPLETED


async def test_clear_leaves_upstream_untouched(orch_ctx):
    """Clearing the middle of A→B→C resets B and C but leaves A completed."""
    job = await create_job("clear_middle", EP)
    a, b, c = (_task(job.id, status=TASK_COMPLETED) for _ in range(3))
    await _insert(
        a,
        b,
        c,
        _dep(a.id, DEPENDENCY_TASK, b.id, DEPENDENCY_TASK),
        _dep(b.id, DEPENDENCY_TASK, c.id, DEPENDENCY_TASK),
    )

    cleared, _ = await clear_task(b.id)

    assert set(cleared) == {b.id, c.id}
    assert await _status(a.id) == TASK_COMPLETED
    assert await _status(b.id) == TASK_PENDING
    assert await _status(c.id) == TASK_PENDING


async def test_clear_grouped_task_resets_group_consumers(orch_ctx):
    """Clearing one task in a group resets the group's consumers, not siblings."""
    job = await create_job("clear_group_consumer", EP)
    g_id = get_snowflake_id()
    x = _task(job.id, status=TASK_COMPLETED, group_id=g_id)
    sibling = _task(job.id, status=TASK_COMPLETED, group_id=g_id)
    consumer = _task(job.id, status=TASK_COMPLETED)
    await _insert(
        Group(id=g_id, job_id=job.id, name="g"),
        x,
        sibling,
        consumer,
        _dep(g_id, DEPENDENCY_GROUP, consumer.id, DEPENDENCY_TASK),
    )

    cleared, _ = await clear_task(x.id)

    assert set(cleared) == {x.id, consumer.id}
    assert await _status(x.id) == TASK_PENDING
    assert await _status(consumer.id) == TASK_PENDING
    assert await _status(sibling.id) == TASK_COMPLETED


async def test_clear_task_to_group_resets_group_members(orch_ctx):
    """Clearing a task whose downstream is a group resets the group's members."""
    job = await create_job("clear_task_to_group", EP)
    g_id = get_snowflake_id()
    x = _task(job.id, status=TASK_COMPLETED)
    m1 = _task(job.id, status=TASK_COMPLETED, group_id=g_id)
    m2 = _task(job.id, status=TASK_COMPLETED, group_id=g_id)
    await _insert(
        Group(id=g_id, job_id=job.id, name="g"),
        x,
        m1,
        m2,
        _dep(x.id, DEPENDENCY_TASK, g_id, DEPENDENCY_GROUP),
    )

    cleared, _ = await clear_task(x.id)

    assert set(cleared) == {x.id, m1.id, m2.id}
    for tid in (x.id, m1.id, m2.id):
        assert await _status(tid) == TASK_PENDING


async def test_clear_revives_terminal_job(orch_ctx):
    """Clearing a task in a FAILED job reactivates the job to RUNNING."""
    job = await create_job("clear_revive", EP)
    async with get_sql_session() as session:
        j = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        j.status = JOB_FAILED
        j.completed_at = utc_now()
        j.error = "boom"
        session.add(j)
        await session.commit()

    t = _task(job.id, status=TASK_FAILED)
    await _insert(t)

    _, revived = await clear_task(t.id)

    assert revived.status == JOB_RUNNING
    assert revived.completed_at is None
    assert revived.error is None


async def test_clear_clears_task_fields(orch_ctx):
    """A cleared task has its run-state fields nulled."""
    job = await create_job("clear_fields", EP)
    worker = await register_execution_worker()
    t = _task(job.id, status=TASK_COMPLETED)
    t.execution_worker_id = worker.id
    t.claimed_at = utc_now()
    t.started_at = utc_now()
    t.completed_at = utc_now()
    t.error = "old error"
    t.result = {"table": "t_old"}
    await _insert(t)

    await clear_task(t.id)

    reset = await _get(t.id)
    assert reset.status == TASK_PENDING
    assert reset.execution_worker_id is None
    assert reset.claimed_at is None
    assert reset.started_at is None
    assert reset.completed_at is None
    assert reset.error is None
    assert reset.result is None


async def test_clear_nonexistent_task_raises(orch_ctx):
    """Clearing a missing task raises TaskNotFound."""
    with pytest.raises(TaskNotFound, match="999999999"):
        await clear_task(999999999)


async def test_clear_fences_stale_update_task_status(orch_ctx):
    """After a clear bumps the epoch, a stale-epoch status write is rejected."""
    job = await create_job("fence_update", EP)
    t = _task(job.id, status=TASK_RUNNING)
    await _insert(t)

    await clear_task(t.id)  # run_epoch 0 -> 1

    assert await update_task_status(t.id, TASK_COMPLETED, expected_epoch=0) is False
    assert await _status(t.id) == TASK_PENDING

    assert await update_task_status(t.id, TASK_COMPLETED, expected_epoch=1) is True
    assert await _status(t.id) == TASK_COMPLETED


async def test_clear_fences_stale_pending_cleanup(orch_ctx):
    """A stale-epoch _set_pending_cleanup is a no-op; the matching epoch applies."""
    job = await create_job("fence_cleanup", EP)
    t = _task(job.id, status=TASK_RUNNING)
    t.run_statuses = [TASK_RUNNING]
    await _insert(t)

    await clear_task(t.id)  # run_epoch 0 -> 1

    await _set_pending_cleanup(t.id, "boom", expected_epoch=0)
    assert await _status(t.id) == TASK_PENDING

    await _set_pending_cleanup(t.id, "boom", expected_epoch=1)
    assert await _status(t.id) == TASK_PENDING_CLEANUP


async def test_check_run_aborted(orch_ctx):
    """check_run_aborted fires on epoch mismatch or CANCELLED, else False."""
    job = await create_job("aborted", EP)
    t = _task(job.id, status=TASK_RUNNING)
    await _insert(t)

    assert await check_run_aborted(t.id, 0) is False

    await clear_task(t.id)  # epoch -> 1
    assert await check_run_aborted(t.id, 0) is True
    assert await check_run_aborted(t.id, 1) is False

    await update_task_status(t.id, TASK_CANCELLED, expected_epoch=1)
    assert await check_run_aborted(t.id, 1) is True

    assert await check_run_aborted(999999999, 0) is False
