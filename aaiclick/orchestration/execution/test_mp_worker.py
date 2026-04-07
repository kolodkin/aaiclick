"""Tests for multiprocessing worker."""

from sqlmodel import select

from ..factories import create_job
from ..models import Task, TaskStatus
from ..orch_context import get_sql_session
from .mp_worker import mp_worker_main_loop


async def test_mp_worker_executes_task(orch_ctx_no_ch):
    """Test that mp worker executes a task in a child process."""
    job = await create_job(
        "test_mp_job",
        "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
    )

    tasks_executed = await mp_worker_main_loop(
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


async def test_mp_worker_handles_failure(orch_ctx_no_ch):
    """Test that mp worker handles task failures from child process."""
    job = await create_job(
        "test_mp_failing_job",
        "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
    )

    tasks_executed = await mp_worker_main_loop(
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


async def test_mp_worker_no_tasks(orch_ctx_no_ch):
    """Test that mp worker exits after max_empty_polls with no tasks."""
    tasks_executed = await mp_worker_main_loop(
        install_signal_handlers=False,
        max_empty_polls=3,
    )

    assert tasks_executed == 0
