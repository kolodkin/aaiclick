"""Tests for local mode worker (in-process async execution with chdb)."""

from sqlmodel import select

from aaiclick.backend import is_sqlite

from ..factories import create_job
from ..models import Task, TaskStatus
from ..orch_context import get_sql_session
from .worker import worker_main_loop


async def test_local_worker_executes_task(orch_ctx):
    """In local mode the worker executes tasks in-process sharing the chdb session."""
    job = await create_job(
        "test_local_job",
        "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
    )

    tasks_executed = await worker_main_loop(
        max_tasks=1,
        install_signal_handlers=False,
        max_empty_polls=5,
    )

    assert tasks_executed == 1

    async with get_sql_session() as session:
        result = await session.execute(
            select(Task).where(Task.job_id == job.id)
        )
        task = result.scalar_one()
        assert task.status == TaskStatus.COMPLETED


async def test_local_worker_handles_failure(orch_ctx):
    """In local mode task failures are handled without child process crashes."""
    job = await create_job(
        "test_local_failing_job",
        "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
    )

    tasks_executed = await worker_main_loop(
        max_tasks=1,
        install_signal_handlers=False,
        max_empty_polls=5,
    )

    assert tasks_executed == 0

    async with get_sql_session() as session:
        result = await session.execute(
            select(Task).where(Task.job_id == job.id)
        )
        task = result.scalar_one()
        assert task.status == TaskStatus.FAILED
        assert task.error is not None


async def test_local_worker_no_tasks(orch_ctx):
    """In local mode the worker exits after max_empty_polls with no tasks."""
    tasks_executed = await worker_main_loop(
        install_signal_handlers=False,
        max_empty_polls=3,
    )

    assert tasks_executed == 0
