"""Multiprocessing worker — runs each task in a dedicated child process.

Architecture:
- Main process: claims tasks from SQLite, manages status, waits for child
- Child process: sets up its own orch_context (chdb + SQLite), executes task
- Only one child process runs at a time (chdb constraint)

SQLite is accessed from both processes (concurrent access safe with WAL mode).
chdb runs exclusively in the child process.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import queue
from typing import Any, NamedTuple

from sqlmodel import select

from ..models import Task
from ..orch_context import get_sql_session
from .runner import execute_task, register_returned_tasks, serialize_task_result
from .worker import POLL_INTERVAL, RunnerResult, _worker_loop, drive_vehicle, worker_heartbeat

# How often the parent checks whether the child process has finished.
# Smaller than POLL_INTERVAL because this polls a local queue, not a database.
CHILD_POLL_INTERVAL = 0.5


class _ProcessResult(NamedTuple):
    """Result passed from child process back to main via queue."""

    success: bool
    result_ref: dict | None
    log_path: str | None
    error: str | None


# "spawn" starts a fresh interpreter — no inherited chdb C++ singleton.
_mp_ctx = multiprocessing.get_context("spawn")


# ---------------------------------------------------------------------------
# Child process (runs in spawned process)
# ---------------------------------------------------------------------------


def _child_process_target(
    task_id: int,
    job_id: int,
    result_queue: multiprocessing.Queue,
) -> None:
    """Sync entry point for the child process — bridges to async."""
    try:
        asyncio.run(_child_run_task(task_id, job_id, result_queue))
    except BaseException as e:
        result_queue.put(
            _ProcessResult(
                success=False,
                result_ref=None,
                log_path=None,
                error=str(e),
            )
        )


async def _child_run_task(
    task_id: int,
    job_id: int,
    result_queue: multiprocessing.Queue,
) -> None:
    """Set up orch_context, fetch task from DB, execute, send result back."""
    from ..orch_context import orch_context

    async with orch_context():
        async with get_sql_session() as session:
            db_result = await session.execute(select(Task).where(Task.id == task_id))
            task = db_result.scalar_one()

        data_result, log_path = await execute_task(task)
        data_result = await register_returned_tasks(data_result, task.id, task.job_id)
        result_ref = serialize_task_result(data_result, job_id)

        result_queue.put(
            _ProcessResult(
                success=True,
                result_ref=result_ref,
                log_path=log_path,
                error=None,
            )
        )


# ---------------------------------------------------------------------------
# Parent process
# ---------------------------------------------------------------------------


class _ChildHandle:
    """Mutable handle for the spawned child + its result queue.

    ``wait`` stashes the ``_ProcessResult`` here so ``collect`` can return
    it — the multiprocessing queue carries the full result, unlike Docker
    where ``collect`` re-reads a side-channel file."""

    def __init__(self, proc: Any, result_queue: multiprocessing.Queue) -> None:
        self.proc = proc
        self.result_queue = result_queue
        self.result: _ProcessResult | None = None


class _MpVehicle:
    """``TaskVehicle`` for the multiprocessing runner.

    ``poll_cancelled`` returns False today — the subprocess runner has
    never had in-flight cancellation; timeout (inside ``wait``) is its only
    kill path. Pointing this at ``check_run_aborted`` is the free win the
    driver unlocks."""

    async def launch(self, task: Task, worker_id: int) -> _ChildHandle:
        result_queue = _mp_ctx.Queue()
        proc = _mp_ctx.Process(
            target=_child_process_target,
            args=(task.id, task.job_id, result_queue),
            daemon=True,
        )
        proc.start()
        return _ChildHandle(proc, result_queue)

    async def wait(self, handle: _ChildHandle, timeout: float | None) -> tuple[int, str | None]:
        result = await _poll_child(handle.proc, handle.result_queue, timeout)
        handle.result = result
        return (0, None) if result.success else (-1, result.error)

    async def poll_cancelled(self, task: Task) -> bool:
        return False

    async def terminate(self, handle: _ChildHandle) -> None:
        handle.proc.kill()

    def collect(self, handle: _ChildHandle, exit_code: int, error: str | None, was_cancelled: bool) -> RunnerResult:
        result = handle.result
        assert result is not None, "wait() must run before collect()"
        return RunnerResult(result.success, result.result_ref, result.log_path, result.error)

    async def cleanup(self, handle: _ChildHandle) -> None:
        pass


async def _run_task_in_child(
    task: Task,
    worker_id: int,
) -> tuple[bool, dict | None, str | None, str | None]:
    """ExecuteFn for the multiprocessing worker.

    Hands an ``_MpVehicle`` to the shared ``drive_vehicle`` driver, which
    spawns the child, heartbeats while it runs, and enforces
    AAICLICK_TASK_TIMEOUT (inside the vehicle's ``wait``).
    """
    raw_timeout = os.environ.get("AAICLICK_TASK_TIMEOUT")
    timeout = float(raw_timeout) if raw_timeout is not None else None

    result = await drive_vehicle(
        task,
        worker_id,
        _MpVehicle(),
        timeout=timeout,
        poll_interval=POLL_INTERVAL,
        heartbeat_fn=worker_heartbeat,
    )
    return result.success, result.result_ref, result.log_path, result.error


async def _poll_child(
    proc: Any,
    result_queue: multiprocessing.Queue,
    timeout: float | None,
) -> _ProcessResult:
    """Poll queue for child result, enforce timeout, detect crashes."""
    poll_interval = CHILD_POLL_INTERVAL
    elapsed = 0.0

    while True:
        try:
            result = await asyncio.to_thread(
                result_queue.get,
                timeout=poll_interval,
            )
            await asyncio.to_thread(proc.join)
            return result
        except queue.Empty:
            pass

        elapsed += poll_interval

        if timeout is not None and elapsed >= timeout:
            proc.kill()
            await asyncio.to_thread(proc.join, timeout=5)
            return _ProcessResult(
                success=False,
                result_ref=None,
                log_path=None,
                error=f"Task timed out after {timeout}s",
            )

        if not proc.is_alive():
            return _ProcessResult(
                success=False,
                result_ref=None,
                log_path=None,
                error=f"Child process exited with code {proc.exitcode}",
            )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def mp_worker_main_loop(
    worker_id: int | None = None,
    max_tasks: int | None = None,
    install_signal_handlers: bool = True,
    max_empty_polls: int | None = None,
) -> int:
    """Main worker loop that spawns a multiprocessing.Process per task.

    Must be called inside an active orch_context(with_ch=False) — the main
    process only needs SQLite for claiming and status updates.  chdb is
    initialized inside each child process. Heartbeats continue while the
    child is running.

    Task timeout is read from AAICLICK_TASK_TIMEOUT env var (seconds).
    When a task exceeds the timeout the child process is killed and the
    task is marked as failed.

    Per-task runner dispatch: tasks belonging to a docker-mode job route
    through the Docker runner; subprocess-mode tasks (and the auto-
    injected build task on every docker job) route through the
    multiprocessing child runner. See ``docker_worker.dispatch_execute``.

    Args:
        worker_id: Worker ID (registers new worker if None).
        max_tasks: Maximum tasks to execute (None for unlimited).
        install_signal_handlers: Install SIGTERM/SIGINT handlers.
        max_empty_polls: Exit after N consecutive empty polls (test helper).

    Returns:
        Number of tasks successfully executed.
    """
    from .docker_worker import dispatch_execute

    return await _worker_loop(
        execute_fn=dispatch_execute,
        worker_id=worker_id,
        max_tasks=max_tasks,
        install_signal_handlers=install_signal_handlers,
        max_empty_polls=max_empty_polls,
        mode_label="mp",
    )
