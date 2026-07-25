"""Tests for local mode worker (in-process async execution with chdb)."""

import pytest
from sqlmodel import select

from ..factories import create_job, create_task
from ..models import TASK_COMPLETED, TASK_PENDING_CLEANUP, Task
from ..orch_context import get_sql_session
from .execution_worker import execution_worker_main_loop

pytestmark = pytest.mark.usefixtures("fast_poll")


async def test_local_worker_executes_task(orch_ctx):
    """In local mode the worker executes tasks in-process sharing the chdb session."""
    job = await create_job(
        "test_local_job",
        "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
    )

    tasks_executed = await execution_worker_main_loop(
        max_tasks=1,
        install_signal_handlers=False,
        max_empty_polls=1,
    )

    assert tasks_executed == 1

    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        task = result.scalar_one()
        assert task.status == TASK_COMPLETED


async def test_local_worker_handles_failure(orch_ctx):
    """In local mode task failures are handled without child process crashes."""
    job = await create_job(
        "test_local_failing_job",
        "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
    )

    tasks_executed = await execution_worker_main_loop(
        max_tasks=1,
        install_signal_handlers=False,
        max_empty_polls=1,
    )

    assert tasks_executed == 0

    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        task = result.scalar_one()
        assert task.status == TASK_PENDING_CLEANUP
        assert task.error is not None


async def test_local_worker_executes_shell_task(orch_ctx):
    """A shell task runs on the in-process worker (log content is pinned in
    test_execution.py — here only the dispatch through the worker loop)."""
    entry = create_task(None, entry_type="shell", command=["sh", "-c", "echo from shell"])
    job = await create_job("test_local_shell_job", entry)

    tasks_executed = await execution_worker_main_loop(
        max_tasks=1,
        install_signal_handlers=False,
        max_empty_polls=1,
    )
    assert tasks_executed == 1

    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        task = result.scalar_one()
        assert task.status == TASK_COMPLETED


async def test_local_worker_shell_task_nonzero_exit(orch_ctx):
    """A failing shell task lands in PENDING_CLEANUP with its exit code."""
    entry = create_task(None, entry_type="shell", command=["sh", "-c", "exit 7"])
    job = await create_job("test_local_shell_fail", entry)

    tasks_executed = await execution_worker_main_loop(
        max_tasks=1,
        install_signal_handlers=False,
        max_empty_polls=1,
    )
    assert tasks_executed == 0

    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        task = result.scalar_one()
        assert task.status == TASK_PENDING_CLEANUP
        assert "exit 7" in (task.error or "")


async def test_local_worker_no_tasks(orch_ctx):
    """In local mode the worker exits after max_empty_polls with no tasks."""
    tasks_executed = await execution_worker_main_loop(
        install_signal_handlers=False,
        max_empty_polls=1,
    )

    assert tasks_executed == 0
