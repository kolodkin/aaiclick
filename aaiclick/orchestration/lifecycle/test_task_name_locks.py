"""Tests for task_name_locks acquire / release / sweep operations.

These coordinate non-preserved named tables between concurrent tasks in
the same job: only one live task can hold a given (job_id, name) at a time.
"""

import pytest
from sqlalchemy import select

from aaiclick.orchestration.lifecycle.db_lifecycle import (
    TableNameCollision,
    TaskNameLock,
    acquire_task_name_lock,
    release_task_name_locks_for_dead_tasks,
    release_task_name_locks_for_task,
)
from aaiclick.orchestration.models import Job, JobStatus, RunType, Task, TaskStatus
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.snowflake import get_snowflake_id


async def _all_locks(session):
    return list((await session.execute(select(TaskNameLock))).scalars().all())


async def _make_job(session) -> Job:
    job = Job(id=get_snowflake_id(), name="j", run_type=RunType.MANUAL, status=JobStatus.RUNNING)
    session.add(job)
    await session.flush()
    return job


async def _make_task(session, *, status: TaskStatus, job_id: int) -> Task:
    task = Task(
        id=get_snowflake_id(),
        job_id=job_id,
        entrypoint="m.f",
        name="t",
        status=status,
    )
    session.add(task)
    await session.flush()
    return task


async def test_acquire_succeeds_when_free(orch_ctx):
    async with get_sql_session() as session:
        await acquire_task_name_lock(session, job_id=1, name="foo", task_id=100)
        rows = await _all_locks(session)
    assert len(rows) == 1
    assert rows[0].name == "foo"
    assert rows[0].task_id == 100


async def test_acquire_idempotent_for_same_task(orch_ctx):
    async with get_sql_session() as session:
        await acquire_task_name_lock(session, job_id=1, name="foo", task_id=100)
        await acquire_task_name_lock(session, job_id=1, name="foo", task_id=100)
        rows = await _all_locks(session)
    assert len(rows) == 1


async def test_acquire_collision_raises(orch_ctx):
    async with get_sql_session() as session:
        await acquire_task_name_lock(session, job_id=1, name="foo", task_id=100)
        with pytest.raises(TableNameCollision) as exc_info:
            await acquire_task_name_lock(session, job_id=1, name="foo", task_id=200)
    assert exc_info.value.held_by_task_id == 100
    assert exc_info.value.name == "foo"


async def test_acquire_different_names_isolated(orch_ctx):
    async with get_sql_session() as session:
        await acquire_task_name_lock(session, job_id=1, name="foo", task_id=100)
        await acquire_task_name_lock(session, job_id=1, name="bar", task_id=200)
        rows = await _all_locks(session)
    assert len(rows) == 2


async def test_acquire_different_jobs_isolated(orch_ctx):
    async with get_sql_session() as session:
        await acquire_task_name_lock(session, job_id=1, name="foo", task_id=100)
        await acquire_task_name_lock(session, job_id=2, name="foo", task_id=100)
        rows = await _all_locks(session)
    assert len(rows) == 2


async def test_release_for_task_clears_only_that_task(orch_ctx):
    async with get_sql_session() as session:
        await acquire_task_name_lock(session, job_id=1, name="foo", task_id=100)
        await acquire_task_name_lock(session, job_id=1, name="bar", task_id=100)
        await acquire_task_name_lock(session, job_id=1, name="baz", task_id=200)
        await release_task_name_locks_for_task(session, task_id=100)
        rows = await _all_locks(session)
    assert len(rows) == 1
    assert rows[0].name == "baz"


async def test_release_after_release_allows_acquire(orch_ctx):
    async with get_sql_session() as session:
        await acquire_task_name_lock(session, job_id=1, name="foo", task_id=100)
        await release_task_name_locks_for_task(session, task_id=100)
        await acquire_task_name_lock(session, job_id=1, name="foo", task_id=200)
        rows = await _all_locks(session)
    assert len(rows) == 1
    assert rows[0].task_id == 200


async def test_dead_task_sweep_releases_locks(orch_ctx):
    async with get_sql_session() as session:
        job = await _make_job(session)
        alive_task = await _make_task(session, status=TaskStatus.RUNNING, job_id=job.id)
        dead_task = await _make_task(session, status=TaskStatus.FAILED, job_id=job.id)
        await acquire_task_name_lock(session, job_id=job.id, name="alive", task_id=alive_task.id)
        await acquire_task_name_lock(session, job_id=job.id, name="dead", task_id=dead_task.id)
        await release_task_name_locks_for_dead_tasks(session)
        rows = await _all_locks(session)
    assert len(rows) == 1
    assert rows[0].name == "alive"
