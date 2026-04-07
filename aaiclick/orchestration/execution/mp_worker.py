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
from typing import NamedTuple, Optional

from sqlmodel import select

from ..models import Task, TaskStatus
from ..orch_context import get_sql_session
from .runner import execute_task, register_returned_tasks, serialize_task_result
from .worker import HEARTBEAT_INTERVAL, _handle_task_result, _worker_loop, worker_heartbeat


class _ProcessResult(NamedTuple):
    """Result passed from child process back to main via queue."""

    success: bool
    result_ref: Optional[dict]
    log_path: Optional[str]
    error: Optional[str]


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
        result_queue.put(_ProcessResult(
            success=False, result_ref=None, log_path=None, error=str(e),
        ))


async def _child_run_task(
    task_id: int,
    job_id: int,
    result_queue: multiprocessing.Queue,
) -> None:
    """Set up orch_context, fetch task from DB, execute, send result back."""
    from ..orch_context import orch_context

    async with orch_context():
        async with get_sql_session() as session:
            db_result = await session.execute(
                select(Task).where(Task.id == task_id)
            )
            task = db_result.scalar_one()

        data_result, log_path = await execute_task(task)
        data_result = await register_returned_tasks(data_result, task.id, task.job_id)
        result_ref = serialize_task_result(data_result, job_id)

        result_queue.put(_ProcessResult(
            success=True, result_ref=result_ref, log_path=log_path, error=None,
        ))


# ---------------------------------------------------------------------------
# Parent process
# ---------------------------------------------------------------------------

async def _run_task_in_child(
    task: Task,
    worker_id: str,
) -> tuple[bool, Optional[dict], Optional[str], Optional[str]]:
    """ExecuteFn for the multiprocessing worker.

    Spawns a child process, sends heartbeats from the parent while
    waiting, and enforces AAICLICK_TASK_TIMEOUT if set.
    """
    raw_timeout = os.environ.get("AAICLICK_TASK_TIMEOUT")
    timeout = float(raw_timeout) if raw_timeout is not None else None

    result_queue = _mp_ctx.Queue()
    proc = _mp_ctx.Process(
        target=_child_process_target,
        args=(task.id, task.job_id, result_queue),
        daemon=True,
    )
    proc.start()

    done = asyncio.Event()
    heartbeat = asyncio.create_task(_heartbeat_while_waiting(worker_id, done))

    try:
        result = await _poll_child(proc, result_queue, timeout)
        return result.success, result.result_ref, result.log_path, result.error
    finally:
        done.set()
        await heartbeat


async def _heartbeat_while_waiting(worker_id: str, done: asyncio.Event) -> None:
    """Send heartbeats in the parent while the child process is running."""
    while not done.is_set():
        try:
            await asyncio.wait_for(done.wait(), timeout=HEARTBEAT_INTERVAL)
            return
        except asyncio.TimeoutError:
            await worker_heartbeat(worker_id)


async def _poll_child(
    proc: multiprocessing.Process,
    result_queue: multiprocessing.Queue,
    timeout: Optional[float],
) -> _ProcessResult:
    """Poll queue for child result, enforce timeout, detect crashes."""
    poll_interval = 0.5
    elapsed = 0.0

    while True:
        try:
            result = await asyncio.to_thread(
                result_queue.get, timeout=poll_interval,
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
                success=False, result_ref=None, log_path=None,
                error=f"Task timed out after {timeout}s",
            )

        if not proc.is_alive():
            return _ProcessResult(
                success=False, result_ref=None, log_path=None,
                error=f"Child process exited with code {proc.exitcode}",
            )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def mp_worker_main_loop(
    worker_id: Optional[str] = None,
    max_tasks: Optional[int] = None,
    install_signal_handlers: bool = True,
    max_empty_polls: Optional[int] = None,
) -> int:
    """Main worker loop that spawns a multiprocessing.Process per task.

    Must be called inside an active orch_context(with_ch=False) — the main
    process only needs SQLite for claiming and status updates.  chdb is
    initialized inside each child process. Heartbeats continue while the
    child is running.

    Task timeout is read from AAICLICK_TASK_TIMEOUT env var (seconds).
    When a task exceeds the timeout the child process is killed and the
    task is marked as failed.

    Args:
        worker_id: Worker ID (registers new worker if None).
        max_tasks: Maximum tasks to execute (None for unlimited).
        install_signal_handlers: Install SIGTERM/SIGINT handlers.
        max_empty_polls: Exit after N consecutive empty polls (test helper).

    Returns:
        Number of tasks successfully executed.
    """
    return await _worker_loop(
        execute_fn=_run_task_in_child,
        worker_id=worker_id,
        max_tasks=max_tasks,
        install_signal_handlers=install_signal_handlers,
        max_empty_polls=max_empty_polls,
        mode_label="mp",
    )
