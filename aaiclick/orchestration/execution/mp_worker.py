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
from .log_flush import flush_shell_logs
from .runner import execute_task, register_run, serialize_task_result

# How often the parent checks whether the child process has finished.
# Smaller than POLL_INTERVAL because this polls a local queue, not a database.
CHILD_POLL_INTERVAL = 0.5


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
            RunnerResult(
                success=False,
                result_ref=None,
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

        data_result = await execute_task(task)
        result_ref = serialize_task_result(data_result, job_id)

        result_queue.put(
            RunnerResult(
                success=True,
                result_ref=result_ref,
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


class _MpVehicle(TaskVehicle["_ChildHandle", "RunnerResult"]):
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

    async def wait(self, handle: _ChildHandle, timeout: float | None) -> tuple[int, str | None, RunnerResult]:
        result = await _poll_child(handle.proc, handle.result_queue, timeout)
        return (0 if result.success else -1), (None if result.success else result.error), result

    async def poll_cancelled(self, task: Task) -> bool:
        return False

    async def terminate(self, handle: _ChildHandle) -> None:
        handle.proc.kill()

    def collect(
        self, handle: _ChildHandle, exit_code: int, error: str | None, was_cancelled: bool, payload: RunnerResult
    ) -> RunnerResult:
        return payload

    async def cleanup(self, handle: _ChildHandle) -> None:
        pass


async def _run_task_in_child(
    task: Task,
    execution_worker_id: int,
) -> tuple[bool, dict | None, str | None]:
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
    return result.success, result.result_ref, result.error


async def _poll_child(
    proc: Any,
    result_queue: multiprocessing.Queue,
    timeout: float | None,
) -> RunnerResult:
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
            return RunnerResult(
                success=False,
                result_ref=None,
                error=f"Task timed out after {timeout}s",
            )

        if not proc.is_alive():
            return RunnerResult(
                success=False,
                result_ref=None,
                error=f"Child process exited with code {proc.exitcode}",
            )


# ---------------------------------------------------------------------------
# Host shell runner (shell tasks on the subprocess runner)
# ---------------------------------------------------------------------------


class _HostShellHandle(NamedTuple):
    proc: Any
    task_id: int
    job_id: int
    run_id: int


class _HostShellVehicle(TaskVehicle["_HostShellHandle", None]):
    """Runs a shell task's argv as a child of the worker process. Success is the
    process exit code; env is the worker env with command_env overlaid. Output
    is captured in memory and flushed to CH ``task_logs`` once the process
    exits (kill included — the pipe drains to EOF either way)."""

    def __init__(self, command: list[str], command_env: dict[str, str] | None) -> None:
        self._command = command
        self._command_env = command_env or {}

    async def launch(self, task: Task, execution_worker_id: int) -> _HostShellHandle:
        run_id = await register_run(task.id)
        env = {**os.environ, **self._command_env}
        proc = await asyncio.create_subprocess_exec(
            *self._command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
        )
        return _HostShellHandle(proc, task.id, task.job_id, run_id)

    async def wait(self, handle: _HostShellHandle, timeout: float | None) -> tuple[int, str | None, None]:
        # Drain stdout concurrently so a full pipe buffer can't deadlock wait().
        reader = asyncio.create_task(handle.proc.stdout.read())
        error: str | None = None
        try:
            await asyncio.wait_for(handle.proc.wait(), timeout=timeout)
            exit_code = handle.proc.returncode
        except asyncio.TimeoutError:
            handle.proc.kill()
            await handle.proc.wait()
            error, exit_code = f"Task timed out after {timeout}s", -1
        output = await reader
        await flush_shell_logs(handle.task_id, handle.job_id, handle.run_id, output.decode(errors="replace"))
        return exit_code, error, None

    async def poll_cancelled(self, task: Task) -> bool:
        return await check_task_cancelled(task.id)

    async def terminate(self, handle: _HostShellHandle) -> None:
        handle.proc.kill()

    def collect(
        self, handle: _HostShellHandle, exit_code: int, error: str | None, was_cancelled: bool, payload: None
    ) -> RunnerResult:
        if was_cancelled:
            return RunnerResult(False, None, "cancelled")
        if error is not None:
            return RunnerResult(False, None, error)
        return RunnerResult(exit_code == 0, None, None if exit_code == 0 else f"exit {exit_code}")

    async def cleanup(self, handle: _HostShellHandle) -> None:
        pass


async def _run_shell_on_host(
    task: Task, execution_worker_id: int, dispatch: JobDispatch
) -> tuple[bool, dict | None, str | None]:
    """ExecuteFn for shell tasks on the subprocess runner."""
    vehicle = _HostShellVehicle(dispatch.command or [], dispatch.command_env)
    result = await drive_vehicle(
        task,
        execution_worker_id,
        vehicle,
        timeout=parse_task_timeout(),
        poll_interval=POLL_INTERVAL,
        heartbeat_fn=execution_worker_heartbeat,
    )
    return result.success, result.result_ref, result.error


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
