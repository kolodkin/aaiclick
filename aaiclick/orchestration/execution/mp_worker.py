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
from pathlib import Path
from typing import Any, NamedTuple

from sqlmodel import select

from ..logging import get_logs_dir
from ..models import Task
from ..orch_context import get_sql_session
from .claiming import check_task_cancelled
from .execution_worker import (
    POLL_INTERVAL,
    JobDispatch,
    RunnerResult,
    TaskVehicle,
    _execution_worker_loop,
    drive_vehicle,
    execution_worker_heartbeat,
    parse_task_timeout,
)
from .runner import execute_task, register_returned_tasks, serialize_task_result

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


class _ChildHandle(NamedTuple):
    """The spawned child + its result queue."""

    proc: Any
    result_queue: multiprocessing.Queue


class _MpVehicle(TaskVehicle["_ChildHandle", "_ProcessResult"]):
    """``TaskVehicle`` for the multiprocessing runner.

    ``poll_cancelled`` returns False today — the subprocess runner has
    never had in-flight cancellation; timeout (inside ``wait``) is its only
    kill path. Pointing this at ``check_run_aborted`` is the free win the
    driver unlocks."""

    async def launch(self, task: Task, execution_worker_id: int) -> _ChildHandle:
        result_queue = _mp_ctx.Queue()
        proc = _mp_ctx.Process(
            target=_child_process_target,
            args=(task.id, task.job_id, result_queue),
            daemon=True,
        )
        proc.start()
        return _ChildHandle(proc, result_queue)

    async def wait(self, handle: _ChildHandle, timeout: float | None) -> tuple[int, str | None, _ProcessResult]:
        result = await _poll_child(handle.proc, handle.result_queue, timeout)
        return (0 if result.success else -1), (None if result.success else result.error), result

    async def poll_cancelled(self, task: Task) -> bool:
        return False

    async def terminate(self, handle: _ChildHandle) -> None:
        handle.proc.kill()

    def collect(
        self, handle: _ChildHandle, exit_code: int, error: str | None, was_cancelled: bool, payload: _ProcessResult
    ) -> RunnerResult:
        return RunnerResult(payload.success, payload.result_ref, payload.log_path, payload.error)

    async def cleanup(self, handle: _ChildHandle) -> None:
        pass


async def _run_task_in_child(
    task: Task,
    execution_worker_id: int,
) -> tuple[bool, dict | None, str | None, str | None]:
    """ExecuteFn for the multiprocessing worker.

    Hands an ``_MpVehicle`` to the shared ``drive_vehicle`` driver, which
    spawns the child, heartbeats while it runs, and enforces
    AAICLICK_TASK_TIMEOUT (inside the vehicle's ``wait``).
    """
    timeout = parse_task_timeout()

    result = await drive_vehicle(
        task,
        execution_worker_id,
        _MpVehicle(),
        timeout=timeout,
        poll_interval=POLL_INTERVAL,
        heartbeat_fn=execution_worker_heartbeat,
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
# Host shell runner (shell tasks on the subprocess runner)
# ---------------------------------------------------------------------------


class _HostShellHandle(NamedTuple):
    proc: Any
    log_path: str
    log_file: Any


class _HostShellVehicle(TaskVehicle["_HostShellHandle", None]):
    """Runs a shell task's argv as a child of the worker process. Success is the
    process exit code; env is the worker env with command_env overlaid."""

    def __init__(self, command: list[str], command_env: dict[str, str] | None, log_base: str) -> None:
        self._command = command
        self._command_env = command_env or {}
        self._log_base = log_base

    async def launch(self, task: Task, execution_worker_id: int) -> _HostShellHandle:
        log_path = os.path.join(self._log_base, str(task.job_id), str(task.id), f"{task.run_epoch}.log")
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        log_file = open(log_path, "wb")
        env = {**os.environ, **self._command_env}
        proc = await asyncio.create_subprocess_exec(
            *self._command,
            stdout=log_file,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
        )
        return _HostShellHandle(proc, log_path, log_file)

    async def wait(self, handle: _HostShellHandle, timeout: float | None) -> tuple[int, str | None, None]:
        try:
            await asyncio.wait_for(handle.proc.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            handle.proc.kill()
            await handle.proc.wait()
            return -1, f"Task timed out after {timeout}s", None
        return handle.proc.returncode, None, None

    async def poll_cancelled(self, task: Task) -> bool:
        return await check_task_cancelled(task.id)

    async def terminate(self, handle: _HostShellHandle) -> None:
        handle.proc.kill()

    def collect(
        self, handle: _HostShellHandle, exit_code: int, error: str | None, was_cancelled: bool, payload: None
    ) -> RunnerResult:
        if was_cancelled:
            return RunnerResult(False, None, handle.log_path, "cancelled")
        if error is not None:
            return RunnerResult(False, None, handle.log_path, error)
        return RunnerResult(exit_code == 0, None, handle.log_path, None if exit_code == 0 else f"exit {exit_code}")

    async def cleanup(self, handle: _HostShellHandle) -> None:
        handle.log_file.close()


async def _run_shell_on_host(
    task: Task, execution_worker_id: int, dispatch: JobDispatch
) -> tuple[bool, dict | None, str | None, str | None]:
    """ExecuteFn for shell tasks on the subprocess runner."""
    vehicle = _HostShellVehicle(dispatch.command or [], dispatch.command_env, get_logs_dir())
    result = await drive_vehicle(
        task,
        execution_worker_id,
        vehicle,
        timeout=parse_task_timeout(),
        poll_interval=POLL_INTERVAL,
        heartbeat_fn=execution_worker_heartbeat,
    )
    return result.success, result.result_ref, result.log_path, result.error


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def mp_worker_main_loop(
    execution_worker_id: int | None = None,
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

    Per-task runner dispatch: tasks belonging to a docker- or kubernetes-mode
    job route through that runner; subprocess-mode tasks (and the auto-injected
    build task on every docker/kubernetes job) route through the multiprocessing
    child runner. See ``dispatch.dispatch_execute``.

    Args:
        execution_worker_id: ExecutionWorker ID (registers new worker if None).
        max_tasks: Maximum tasks to execute (None for unlimited).
        install_signal_handlers: Install SIGTERM/SIGINT handlers.
        max_empty_polls: Exit after N consecutive empty polls (test helper).

    Returns:
        Number of tasks successfully executed.
    """
    # Delayed import: dispatch imports this module at top level.
    from .dispatch import dispatch_execute

    return await _execution_worker_loop(
        execute_fn=dispatch_execute,
        execution_worker_id=execution_worker_id,
        max_tasks=max_tasks,
        install_signal_handlers=install_signal_handlers,
        max_empty_polls=max_empty_polls,
        mode_label="mp",
    )
