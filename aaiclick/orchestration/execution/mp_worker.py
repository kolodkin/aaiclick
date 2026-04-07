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
import queue
from typing import NamedTuple, Optional

from sqlmodel import select

from ..models import Task, TaskStatus
from ..orch_context import get_sql_session
from .runner import execute_task, register_returned_tasks, serialize_task_result
from .worker import _handle_task_result, _worker_loop


class _ProcessResult(NamedTuple):
    """Result passed from child process back to main via queue."""

    success: bool
    result_ref: Optional[dict]
    log_path: Optional[str]
    error: Optional[str]


# Use "spawn" context: starts a fresh Python interpreter so the child
# has no inherited chdb embedded-server state from the parent process.
# "fork" would inherit the C++ singleton and deadlock or conflict.
_mp_ctx = multiprocessing.get_context("spawn")


def _child_process_target(
    task_id: int,
    job_id: int,
    result_queue: multiprocessing.Queue,
) -> None:
    """Entry point for the child process. Runs asyncio event loop."""
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
    """Execute a task inside its own orch_context in the child process."""
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


async def _execute_in_child(task: Task) -> tuple[bool, Optional[dict], Optional[str], Optional[str]]:
    """Run a task in a child process, return (success, result_ref, log_path, error)."""
    proc_result = await _run_in_child(task.id, task.job_id, timeout=None)
    return proc_result.success, proc_result.result_ref, proc_result.log_path, proc_result.error


async def mp_worker_main_loop(
    worker_id: Optional[int] = None,
    max_tasks: Optional[int] = None,
    install_signal_handlers: bool = True,
    max_empty_polls: Optional[int] = None,
) -> int:
    """Main worker loop that spawns a multiprocessing.Process per task.

    Must be called inside an active orch_context(with_ch=False) — the main
    process only needs SQLite for claiming and status updates.  chdb is
    initialized inside each child process.

    Args:
        worker_id: Worker ID (registers new worker if None).
        max_tasks: Maximum tasks to execute (None for unlimited).
        install_signal_handlers: Install SIGTERM/SIGINT handlers.
        max_empty_polls: Exit after N consecutive empty polls (test helper).

    Returns:
        Number of tasks successfully executed.
    """
    return await _worker_loop(
        execute_fn=_execute_in_child,
        worker_id=worker_id,
        max_tasks=max_tasks,
        install_signal_handlers=install_signal_handlers,
        max_empty_polls=max_empty_polls,
        mode_label="mp",
    )


async def _run_in_child(
    task_id: int,
    job_id: int,
    timeout: Optional[float],
) -> _ProcessResult:
    """Spawn a child process, wait for its result, return it."""
    result_queue = _mp_ctx.Queue()
    proc = _mp_ctx.Process(
        target=_child_process_target,
        args=(task_id, job_id, result_queue),
        daemon=True,
    )
    proc.start()

    poll_interval = 0.5
    elapsed = 0.0

    while True:
        try:
            result = await asyncio.to_thread(
                result_queue.get, timeout=poll_interval,
            )
            proc.join(timeout=5)
            return result
        except queue.Empty:
            pass

        elapsed += poll_interval

        if timeout is not None and elapsed >= timeout:
            proc.kill()
            proc.join(timeout=5)
            return _ProcessResult(
                success=False, result_ref=None, log_path=None,
                error=f"Task timed out after {timeout}s",
            )

        if not proc.is_alive():
            exit_code = proc.exitcode
            return _ProcessResult(
                success=False, result_ref=None, log_path=None,
                error=f"Child process exited with code {exit_code}",
            )
