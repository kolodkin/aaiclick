"""Tests for local mode worker (in-process async execution with chdb)."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from sqlmodel import select

from aaiclick.testing import get_task_runs, until

from ..background.test_cancelled_cleanup import run_cancelled_cleanup
from ..factories import create_job, create_task
from ..jobs import get_task, get_tasks_for_job
from ..models import (
    EXECUTION_WORKER_STOPPED,
    TASK_CANCELLED,
    TASK_COMPLETED,
    TASK_PENDING_CANCELLED_CLEANUP,
    TASK_PENDING_FAILURE_CLEANUP,
    TASK_RUNNING,
    Task,
)
from ..orch_context import get_sql_session
from . import execution_worker as ew
from .claiming import cancel_job
from .execution_worker import (
    _execution_worker_loop,
    deregister_execution_worker,
    execution_worker_heartbeat,
    execution_worker_main_loop,
    get_execution_worker,
    register_execution_worker,
)

pytestmark = pytest.mark.usefixtures("fast_poll")


async def _until_heartbeats(n: int) -> None:
    """Runs in-process, so it sees the test's patched heartbeat."""
    target = ew.execution_worker_heartbeat.await_count + n
    await until(lambda: ew.execution_worker_heartbeat.await_count >= target, what="in-task heartbeats")


async def test_local_worker_no_tasks(orch_ctx):
    """In local mode the worker exits after max_empty_polls with no tasks."""
    tasks_executed = await execution_worker_main_loop(
        install_signal_handlers=False,
        max_empty_polls=1,
    )

    assert tasks_executed == 0


async def _wait_for_status(task_id: int, status: str) -> None:
    async def reached() -> bool:
        return (task := await get_task(task_id)) is not None and task.status == status

    await until(reached, what=status)


async def test_local_worker_dispatch_exception_fails_task(orch_ctx):
    """An exception escaping the runner is the task's failure: the task lands in
    PENDING_FAILURE_CLEANUP with the error and the loop keeps running rather than
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
    assert task.status == TASK_PENDING_FAILURE_CLEANUP
    assert task.error == "RuntimeError: no image tag"


async def test_local_worker_heartbeats_during_task(orch_ctx, monkeypatch):
    """The in-process runner heartbeats while a task runs, not only between
    claims — otherwise any task longer than the dead-worker timeout is
    declared dead and run twice."""
    monkeypatch.setattr(ew, "HEARTBEAT_INTERVAL", 0.05)
    monkeypatch.setattr(ew, "execution_worker_heartbeat", AsyncMock(wraps=execution_worker_heartbeat))
    job = await create_job("test_heartbeat_during_task", create_task(_until_heartbeats, {"n": 3}))

    tasks_executed = await execution_worker_main_loop(max_tasks=1, install_signal_handlers=False, max_empty_polls=1)

    assert tasks_executed == 1
    (task,) = await get_tasks_for_job(job.id)
    assert (task.status, task.error) == (TASK_COMPLETED, None)


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


async def test_local_worker_self_stops_when_marked_stopped(orch_ctx, monkeypatch):
    """A worker whose heartbeat finds it STOPPED aborts its run without
    reporting it and exits the loop; the task keeps its claim and epoch for
    the sweep to reclaim."""
    monkeypatch.setattr(ew, "HEARTBEAT_INTERVAL", 0.05)
    worker = await register_execution_worker()
    job = await create_job(
        "test_worker_self_stop",
        create_task("aaiclick.orchestration.fixtures.sample_tasks.slow_task", {"seconds": 30, "steps": 30}),
    )
    task = await _task_row(job.id)

    loop_task = asyncio.create_task(
        execution_worker_main_loop(execution_worker_id=worker.id, install_signal_handlers=False)
    )
    await _wait_for_status(task.id, TASK_RUNNING)
    await deregister_execution_worker(worker.id)

    assert await asyncio.wait_for(loop_task, timeout=5) == 0
    abandoned = await get_task(task.id)
    assert abandoned is not None
    assert (abandoned.status, abandoned.execution_worker_id, abandoned.run_epoch) == (TASK_RUNNING, worker.id, 0)


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

    async def running() -> bool:
        return [r.status for r in await get_task_runs((await _task_row(job.id)).id)] == [TASK_RUNNING]

    await until(running)
    await cancel_job(job.id)
    assert await loop == 0

    task = await _task_row(job.id)
    assert task.status == TASK_PENDING_CANCELLED_CLEANUP
    assert task.execution_worker_id is None
    assert [r.status for r in await get_task_runs(task.id)] == [TASK_CANCELLED]

    await run_cancelled_cleanup()
    assert (await _task_row(job.id)).status == TASK_CANCELLED
