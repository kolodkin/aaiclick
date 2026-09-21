"""Tests for local mode worker (in-process async execution with chdb)."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from sqlmodel import select

from ..decorators import job, task
from ..factories import create_job, create_task
from ..jobs import get_task
from ..models import EXECUTION_WORKER_STOPPED, TASK_COMPLETED, TASK_PENDING_CLEANUP, TASK_RUNNING, Task
from ..orch_context import get_sql_session
from . import execution_worker as ew
from .execution_worker import (
    _execution_worker_loop,
    execution_worker_heartbeat,
    execution_worker_main_loop,
    get_execution_worker,
    register_execution_worker,
)

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


async def _wait_for_status(task_id: int, status: str) -> None:
    """Poll until the task reaches ``status``."""

    async def poll():
        while True:
            task = await get_task(task_id)
            if task is not None and task.status == status:
                return
            await asyncio.sleep(0.05)

    await asyncio.wait_for(poll(), timeout=5)


async def test_local_worker_dispatch_exception_fails_task(orch_ctx):
    """An exception escaping the runner is the task's failure: the task lands in
    PENDING_CLEANUP with the error and the loop keeps running rather than
    stranding the task RUNNING under a STOPPED worker."""
    job = await create_job("test_dispatch_raises", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

    async def boom(task: Task, execution_worker_id: int):
        raise RuntimeError("no image tag")

    tasks_executed = await _execution_worker_loop(
        execute_fn=boom,
        max_tasks=1,
        install_signal_handlers=False,
        max_empty_polls=1,
    )

    assert tasks_executed == 0
    async with get_sql_session() as session:
        task = (await session.execute(select(Task).where(Task.job_id == job.id))).scalar_one()
    assert task.status == TASK_PENDING_CLEANUP
    assert task.error == "RuntimeError: no image tag"


async def test_local_worker_heartbeats_during_task(orch_ctx, monkeypatch):
    """The in-process runner heartbeats while a task runs, not only between
    claims — otherwise any task longer than the dead-worker timeout is
    declared dead and run twice."""
    monkeypatch.setattr(ew, "HEARTBEAT_INTERVAL", 0.05)
    heartbeat = AsyncMock(wraps=execution_worker_heartbeat)
    monkeypatch.setattr(ew, "execution_worker_heartbeat", heartbeat)
    await create_job(
        "test_heartbeat_during_task",
        create_task("aaiclick.orchestration.fixtures.sample_tasks.slow_task", {"seconds": 0.5, "steps": 5}),
    )

    tasks_executed = await execution_worker_main_loop(max_tasks=1, install_signal_handlers=False, max_empty_polls=1)

    assert tasks_executed == 1
    assert heartbeat.await_count >= 3


async def test_local_worker_cancel_propagates_to_loop(orch_ctx):
    """Cancelling the worker (``local start`` shutdown) while a task runs
    cancels the task and exits the loop instead of swallowing the
    cancellation and polling forever."""
    worker = await register_execution_worker()
    job = await create_job(
        "test_worker_cancel",
        create_task("aaiclick.orchestration.fixtures.sample_tasks.slow_task", {"seconds": 30, "steps": 30}),
    )
    async with get_sql_session() as session:
        task = (await session.execute(select(Task).where(Task.job_id == job.id))).scalar_one()

    loop_task = asyncio.create_task(
        execution_worker_main_loop(execution_worker_id=worker.id, install_signal_handlers=False)
    )
    await _wait_for_status(task.id, TASK_RUNNING)

    loop_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(loop_task, timeout=5)

    stopped = await get_execution_worker(worker.id)
    assert stopped is not None
    assert stopped.status == EXECUTION_WORKER_STOPPED
