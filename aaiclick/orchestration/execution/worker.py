"""Worker management for orchestration backend."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
from collections.abc import Awaitable, Callable

from sqlalchemy import update
from sqlmodel import col, select

from aaiclick.snowflake import get_snowflake_id

from ...datetime_utils import utc_now
from ..background.handler import try_complete_job
from ..models import (
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_PENDING_CLEANUP,
    TASK_RUNNING,
    WORKER_ACTIVE,
    WORKER_STOPPED,
    WORKER_STOPPING,
    Task,
    Worker,
    WorkerStatus,
)
from ..orch_context import get_sql_session
from .claiming import check_run_aborted, claim_next_task, update_task_status
from .runner import execute_task, register_returned_tasks, serialize_task_result

logger = logging.getLogger(__name__)

# Task execution strategy used by _worker_loop.
# Args: (task, worker_id). Returns: (success, result_ref, log_path, error).
ExecuteFn = Callable[[Task, int], Awaitable[tuple[bool, dict | None, str | None, str | None]]]

# Heartbeat interval in seconds
HEARTBEAT_INTERVAL = 30

# Poll interval when no tasks available
POLL_INTERVAL = 1


async def _set_pending_cleanup(task_id: int, error: str, expected_epoch: int | None = None) -> None:
    """Transition a failed task to PENDING_CLEANUP for background ref cleanup.

    When ``expected_epoch`` is given and no longer matches the task's
    ``run_epoch``, the write is skipped — the run was cleared out from under
    this worker and its failure must not clobber the reset state. The epoch
    guard lives in the UPDATE's WHERE clause so it is enforced atomically with
    the write on every backend, not only where ``FOR UPDATE`` holds a row lock.
    """
    async with get_sql_session() as session:
        task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
        if task is None:
            return
        if expected_epoch is not None and task.run_epoch != expected_epoch:
            return
        values: dict[str, str | list[str]] = {"status": TASK_PENDING_CLEANUP, "error": error}
        if task.run_statuses:
            values["run_statuses"] = [*task.run_statuses[:-1], TASK_FAILED]
        stmt = update(Task).where(col(Task.id) == task_id)
        if expected_epoch is not None:
            stmt = stmt.where(col(Task.run_epoch) == expected_epoch)
        await session.execute(stmt.values(**values))
        await session.commit()


async def register_worker(
    hostname: str | None = None,
    pid: int | None = None,
) -> Worker:
    """
    Register a new worker process.

    Creates a Worker record in the database with ACTIVE status.
    Uses system hostname and PID if not provided.

    Args:
        hostname: Worker hostname (default: system hostname)
        pid: Worker process ID (default: current process PID)

    Returns:
        Worker: Registered worker with ID populated
    """
    worker_id = get_snowflake_id()
    worker = Worker(
        id=worker_id,
        hostname=hostname or socket.gethostname(),
        pid=pid or os.getpid(),
        status=WORKER_ACTIVE,
        last_heartbeat=utc_now(),
        started_at=utc_now(),
    )

    async with get_sql_session() as session:
        session.add(worker)
        await session.commit()
        await session.refresh(worker)

    return worker


async def worker_heartbeat(worker_id: int) -> WorkerStatus | None:
    """
    Update worker's last_heartbeat timestamp.

    Should be called periodically to indicate the worker is alive.
    Updates last_heartbeat and ensures status is ACTIVE (unless STOPPING).

    Args:
        worker_id: Worker ID to update

    Returns:
        The worker's current status after update, or None if worker not found.
    """
    async with get_sql_session() as session:
        result = await session.execute(select(Worker).where(Worker.id == worker_id))
        worker = result.scalar_one_or_none()

        if worker is None:
            return None

        worker.last_heartbeat = utc_now()
        if worker.status != WORKER_STOPPING:
            worker.status = WORKER_ACTIVE
        session.add(worker)
        await session.commit()

    return worker.status


async def request_worker_stop(worker_id: int) -> bool:
    """
    Request a worker to stop gracefully.

    Sets the worker status to STOPPING. The worker will finish its current
    task and exit on the next heartbeat check.

    Args:
        worker_id: Worker ID to stop

    Returns:
        bool: True if worker was found and set to STOPPING,
              False if not found or already in a terminal state
    """
    async with get_sql_session() as session:
        result = await session.execute(select(Worker).where(Worker.id == worker_id))
        worker = result.scalar_one_or_none()

        if worker is None:
            return False

        if worker.status in (WORKER_STOPPED, WORKER_STOPPING):
            return False

        worker.status = WORKER_STOPPING
        session.add(worker)
        await session.commit()

    return True


async def deregister_worker(worker_id: int) -> bool:
    """
    Mark a worker as stopped.

    Updates worker status to STOPPED. Does not delete the record
    to preserve history.

    Args:
        worker_id: Worker ID to deregister

    Returns:
        bool: True if worker was found and updated, False otherwise
    """
    async with get_sql_session() as session:
        result = await session.execute(select(Worker).where(Worker.id == worker_id))
        worker = result.scalar_one_or_none()

        if worker is None:
            return False

        worker.status = WORKER_STOPPED
        session.add(worker)
        await session.commit()

    return True


async def list_workers(status: WorkerStatus | None = None) -> list[Worker]:
    """
    List workers, optionally filtered by status.

    Args:
        status: Filter by worker status (default: all workers)

    Returns:
        list[Worker]: List of workers matching criteria
    """
    async with get_sql_session() as session:
        query = select(Worker)
        if status is not None:
            query = query.where(Worker.status == status)
        query = query.order_by(col(Worker.started_at).desc())

        result = await session.execute(query)
        workers = result.scalars().all()

    return list(workers)


async def get_worker(worker_id: int) -> Worker | None:
    """
    Get a worker by ID.

    Args:
        worker_id: Worker ID

    Returns:
        Worker if found, None otherwise
    """
    async with get_sql_session() as session:
        result = await session.execute(select(Worker).where(Worker.id == worker_id))
        return result.scalar_one_or_none()


async def _increment_worker_stat(worker_id: int, field: str) -> None:
    """Increment a worker stat field (tasks_completed or tasks_failed)."""
    async with get_sql_session() as session:
        result = await session.execute(select(Worker).where(Worker.id == worker_id))
        worker = result.scalar_one_or_none()
        if worker:
            setattr(worker, field, getattr(worker, field) + 1)
            session.add(worker)
            await session.commit()


async def _cancellation_monitor(task_id: int, exec_task: asyncio.Task, expected_epoch: int) -> None:
    """Poll task state in DB and cancel the asyncio.Task if the run is aborted.

    Runs concurrently with task execution. Checks the database every
    POLL_INTERVAL seconds. When cancel_job() marks the task CANCELLED or
    clear_task() bumps its run_epoch past ``expected_epoch``, this monitor
    cancels the asyncio.Task, raising CancelledError at the next await point
    in the running coroutine.

    Note: asyncio cancellation is cooperative — CPU-bound code without
    await points won't be interrupted until it yields.
    """
    while not exec_task.done():
        await asyncio.sleep(POLL_INTERVAL)
        if await check_run_aborted(task_id, expected_epoch):
            exec_task.cancel()
            return


async def _handle_task_result(
    task: Task,
    worker_id: int,
    success: bool,
    result_ref: dict | None,
    log_path: str | None,
    error: str | None,
) -> bool:
    """Process the result of a task execution. Returns True if task succeeded."""
    if success:
        updated = await update_task_status(
            task.id,
            TASK_COMPLETED,
            result=result_ref,
            log_path=log_path,
            expected_epoch=task.run_epoch,
        )
        if not updated:
            logger.info(
                "Worker %s task %s completion discarded (cleared or cancelled)", worker_id, task.id
            )
            return False
        logger.info("Worker %s completed task %s", worker_id, task.id)
        await _increment_worker_stat(worker_id, "tasks_completed")
        async with get_sql_session() as session:
            await try_complete_job(session, task.job_id)
            await session.commit()
        return True

    error = error or "Unknown error"
    logger.warning("Worker %s task %s failed: %s", worker_id, task.id, error)
    await _set_pending_cleanup(task.id, error, expected_epoch=task.run_epoch)
    await _increment_worker_stat(worker_id, "tasks_failed")
    logger.info("Worker %s task %s set to PENDING_CLEANUP", worker_id, task.id)
    return False


async def _worker_loop(
    execute_fn: ExecuteFn,
    worker_id: int | None = None,
    max_tasks: int | None = None,
    install_signal_handlers: bool = True,
    max_empty_polls: int | None = None,
    mode_label: str = "async",
) -> int:
    """Shared worker loop used by both async and multiprocessing workers.

    Claims tasks, delegates execution to ``execute_fn``, and handles
    status updates, retries, and job completion.

    Args:
        execute_fn: Async callable (Task) -> (success, result_ref, log_path, error).
        worker_id: Worker ID (registers new worker if None).
        max_tasks: Maximum tasks to execute (None for unlimited).
        install_signal_handlers: Install SIGTERM/SIGINT handlers.
        max_empty_polls: Exit after N consecutive empty polls (test helper).
        mode_label: Label for log messages (e.g. "async", "mp").

    Returns:
        Number of tasks successfully executed.
    """
    shutdown_requested = False

    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        shutdown_requested = True

    if install_signal_handlers:
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    if worker_id is None:
        worker = await register_worker()
        worker_id = worker.id
        logger.info(
            "Worker %s registered (host=%s, pid=%s, mode=%s)",
            worker_id,
            worker.hostname,
            worker.pid,
            mode_label,
        )
    else:
        logger.info("Worker %s starting (mode=%s)", worker_id, mode_label)

    tasks_executed = 0
    last_heartbeat = utc_now()
    empty_polls = 0

    try:
        while not shutdown_requested:
            if max_tasks is not None and tasks_executed >= max_tasks:
                break

            if max_empty_polls is not None and empty_polls >= max_empty_polls:
                break

            now = utc_now()
            if (now - last_heartbeat).total_seconds() >= HEARTBEAT_INTERVAL:
                status = await worker_heartbeat(worker_id)
                last_heartbeat = now
                if status == WORKER_STOPPING:
                    logger.info("Worker %s received stop request", worker_id)
                    shutdown_requested = True
                    continue

            task = await claim_next_task(worker_id)

            if task is None:
                empty_polls += 1
                await asyncio.sleep(POLL_INTERVAL)
                continue

            empty_polls = 0
            logger.info("Worker %s executing task %s: %s", worker_id, task.id, task.entrypoint)
            await update_task_status(task.id, TASK_RUNNING, expected_epoch=task.run_epoch)

            success, result_ref, log_path, error = await execute_fn(task, worker_id)
            if await _handle_task_result(task, worker_id, success, result_ref, log_path, error):
                tasks_executed += 1

    finally:
        await deregister_worker(worker_id)
        logger.info("Worker %s stopped (executed %s tasks)", worker_id, tasks_executed)

    return tasks_executed


async def _execute_in_process(task: Task, worker_id: int) -> tuple[bool, dict | None, str | None, str | None]:
    """Execute a task in the current async process with cancellation monitoring."""
    exec_task = asyncio.create_task(execute_task(task))
    monitor = asyncio.create_task(_cancellation_monitor(task.id, exec_task, task.run_epoch))

    try:
        data_result, log_path = await exec_task
        data_result = await register_returned_tasks(data_result, task.id, task.job_id)
        result_ref = serialize_task_result(data_result, task.job_id)
        return True, result_ref, log_path, None
    except asyncio.CancelledError:
        logger.info("Task %s cancelled", task.id)
        return False, None, None, None
    except Exception as e:
        return False, None, None, str(e)
    finally:
        monitor.cancel()
        try:
            await monitor
        except asyncio.CancelledError:
            pass


async def worker_main_loop(
    worker_id: int | None = None,
    max_tasks: int | None = None,
    install_signal_handlers: bool = True,
    max_empty_polls: int | None = None,
) -> int:
    """Main worker execution loop (in-process async execution).

    Continuously polls for and executes tasks until shutdown signal
    or max_tasks is reached. Must be called inside an active orch_context.

    Args:
        worker_id: Worker ID (registers new worker if None)
        max_tasks: Maximum tasks to execute (None for unlimited)
        install_signal_handlers: Install SIGTERM/SIGINT handlers (default True)
        max_empty_polls: Exit after N consecutive empty polls (None for unlimited)

    Returns:
        int: Number of tasks executed
    """
    return await _worker_loop(
        execute_fn=_execute_in_process,
        worker_id=worker_id,
        max_tasks=max_tasks,
        install_signal_handlers=install_signal_handlers,
        max_empty_polls=max_empty_polls,
    )
