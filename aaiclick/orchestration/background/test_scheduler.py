"""Tests for scheduled job creation in BackgroundWorker."""

import json
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.registered_jobs import register_job


async def _get_engine(orch_ctx):
    """Create a standalone async engine matching the test DB."""
    import os

    url = os.environ["AAICLICK_SQL_URL"]
    return create_async_engine(url, echo=False)


async def test_check_schedules_creates_job(orch_ctx):
    """A due registered job should produce a Job + Task."""
    # Register a job with next_run_at in the past
    reg = await register_job(
        name="sched_test",
        entrypoint="myapp.sched_task",
        schedule="* * * * *",
        default_kwargs={"key": "val"},
    )

    # Force next_run_at to the past
    async with get_sql_session() as session:
        result = await session.execute(
            text("UPDATE registered_jobs SET next_run_at = :past WHERE id = :id"),
            {"past": datetime.utcnow() - timedelta(minutes=5), "id": reg.id},
        )
        await session.commit()

    # Create a BackgroundWorker with our test engine
    worker = BackgroundWorker()
    worker._engine = await _get_engine(orch_ctx)
    worker._ch_client = None

    await worker._check_schedules()

    # Verify Job was created
    async with get_sql_session() as session:
        result = await session.execute(
            text(
                "SELECT id, name, run_type, registered_job_id FROM jobs "
                "WHERE registered_job_id = :reg_id"
            ),
            {"reg_id": reg.id},
        )
        jobs = result.fetchall()

    assert len(jobs) == 1
    job_id, name, run_type, registered_job_id = jobs[0]
    assert name == "sched_test"
    assert run_type == "SCHEDULED"
    assert registered_job_id == reg.id

    # Verify entry Task was created
    async with get_sql_session() as session:
        result = await session.execute(
            text("SELECT entrypoint, name, kwargs FROM tasks WHERE job_id = :job_id"),
            {"job_id": job_id},
        )
        tasks = result.fetchall()

    assert len(tasks) == 1
    assert tasks[0][0] == "myapp.sched_task"
    assert tasks[0][1] == "sched_test"
    task_kwargs = tasks[0][2]
    if isinstance(task_kwargs, str):
        task_kwargs = json.loads(task_kwargs)
    assert task_kwargs == {"key": "val"}

    # Verify next_run_at was updated to the future
    async with get_sql_session() as session:
        result = await session.execute(
            text("SELECT next_run_at FROM registered_jobs WHERE id = :id"),
            {"id": reg.id},
        )
        raw = result.scalar_one()

    new_next_run = datetime.fromisoformat(raw) if isinstance(raw, str) else raw
    assert new_next_run > datetime.utcnow() - timedelta(seconds=5)

    await worker._engine.dispose()


async def test_check_schedules_optimistic_lock_prevents_duplicates(orch_ctx):
    """Running _check_schedules twice should not create duplicate jobs."""
    reg = await register_job(
        name="dedup_test",
        entrypoint="myapp.dedup_task",
        schedule="* * * * *",
    )

    async with get_sql_session() as session:
        await session.execute(
            text("UPDATE registered_jobs SET next_run_at = :past WHERE id = :id"),
            {"past": datetime.utcnow() - timedelta(minutes=5), "id": reg.id},
        )
        await session.commit()

    worker = BackgroundWorker()
    worker._engine = await _get_engine(orch_ctx)
    worker._ch_client = None

    # Run twice
    await worker._check_schedules()
    await worker._check_schedules()

    # Should have exactly one job
    async with get_sql_session() as session:
        result = await session.execute(
            text("SELECT COUNT(*) FROM jobs WHERE registered_job_id = :reg_id"),
            {"reg_id": reg.id},
        )
        count = result.scalar_one()

    assert count == 1

    await worker._engine.dispose()


async def test_check_schedules_skips_disabled_jobs(orch_ctx):
    """Disabled registered jobs should not create runs."""
    reg = await register_job(
        name="disabled_sched",
        entrypoint="myapp.disabled",
        schedule="* * * * *",
        enabled=False,
    )

    worker = BackgroundWorker()
    worker._engine = await _get_engine(orch_ctx)
    worker._ch_client = None

    await worker._check_schedules()

    async with get_sql_session() as session:
        result = await session.execute(
            text("SELECT COUNT(*) FROM jobs WHERE registered_job_id = :reg_id"),
            {"reg_id": reg.id},
        )
        count = result.scalar_one()

    assert count == 0

    await worker._engine.dispose()


async def test_check_schedules_skips_no_schedule_jobs(orch_ctx):
    """Registered jobs without a schedule should not create runs."""
    reg = await register_job(
        name="no_sched",
        entrypoint="myapp.no_sched",
    )

    worker = BackgroundWorker()
    worker._engine = await _get_engine(orch_ctx)
    worker._ch_client = None

    await worker._check_schedules()

    async with get_sql_session() as session:
        result = await session.execute(
            text("SELECT COUNT(*) FROM jobs WHERE registered_job_id = :reg_id"),
            {"reg_id": reg.id},
        )
        count = result.scalar_one()

    assert count == 0

    await worker._engine.dispose()
