"""Tests for local mode worker (in-process async execution with chdb)."""

import asyncio

import pytest
from sqlmodel import select

from ..background.test_cancelled_cleanup import run_cancelled_cleanup
from ..decorators import job, task
from ..factories import create_job, create_task
from ..models import (
    TASK_CANCELLED,
    TASK_COMPLETED,
    TASK_PENDING_CANCELLED_CLEANUP,
    TASK_PENDING_FAILURE_CLEANUP,
    TASK_RUNNING,
    Task,
)
from ..orch_context import get_sql_session
from .claiming import cancel_job
from .execution_worker import execution_worker_main_loop

pytestmark = pytest.mark.usefixtures("fast_poll")


@task
async def _child_step(label: str) -> str:
    """Child of a dynamic registration test — returns its label."""
    return label


@job("test_local_dynamic_list")
def dynamic_list_pipeline():
    """Entry task returning a plain list of Tasks (the documented @job shape)."""
    return [_child_step(label="a"), _child_step(label="b")]


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
        assert task.status == TASK_PENDING_FAILURE_CLEANUP
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
    """A failing shell task lands in PENDING_FAILURE_CLEANUP with its exit code."""
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
        assert task.status == TASK_PENDING_FAILURE_CLEANUP
        assert "exit 7" in (task.error or "")


async def test_local_worker_registers_returned_task_list(orch_ctx):
    """An entry task returning a plain list of Tasks registers them as children."""
    j = await dynamic_list_pipeline()

    tasks_executed = await execution_worker_main_loop(
        max_tasks=3,
        install_signal_handlers=False,
        max_empty_polls=1,
    )
    assert tasks_executed == 3

    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == j.id))
        tasks = result.scalars().all()

    assert len(tasks) == 3
    assert all(t.status == TASK_COMPLETED for t in tasks)


async def test_local_worker_no_tasks(orch_ctx):
    """In local mode the worker exits after max_empty_polls with no tasks."""
    tasks_executed = await execution_worker_main_loop(
        install_signal_handlers=False,
        max_empty_polls=1,
    )

    assert tasks_executed == 0


async def _task_row(job_id: int) -> Task:
    async with get_sql_session() as session:
        return (await session.execute(select(Task).where(Task.job_id == job_id))).scalar_one()


async def test_local_worker_cancelled_mid_run(orch_ctx):
    """Cancelling a running task kills the run; the worker reports it back by
    releasing its ownership and stamping the run, and only then does the
    cancelled-cleanup pass settle the task to CANCELLED."""
    job = await create_job(
        "test_local_cancel",
        create_task("aaiclick.orchestration.fixtures.sample_tasks.slow_task", {"seconds": 30.0, "steps": 300}),
    )
    loop = asyncio.create_task(
        execution_worker_main_loop(max_tasks=1, install_signal_handlers=False, max_empty_polls=1)
    )
    while (await _task_row(job.id)).run_statuses != [TASK_RUNNING]:
        await asyncio.sleep(0.05)

    await cancel_job(job.id)
    assert await loop == 0

    task = await _task_row(job.id)
    assert task.status == TASK_PENDING_CANCELLED_CLEANUP
    assert task.execution_worker_id is None
    assert task.run_statuses == [TASK_CANCELLED]

    await run_cancelled_cleanup()
    assert (await _task_row(job.id)).status == TASK_CANCELLED
