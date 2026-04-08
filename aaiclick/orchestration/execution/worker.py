"""Worker management for orchestration backend."""

from __future__ import annotations

import asyncio
import os
import signal
import socket
from datetime import datetime, timedelta
from typing import Awaitable, Callable, Optional

from sqlmodel import select

from aaiclick.snowflake_id import get_snowflake_id

from .claiming import check_task_cancelled, claim_next_task, update_job_status, update_task_status
from ..orch_context import get_sql_session
from .runner import execute_task, register_returned_tasks, serialize_task_result
from ..models import JobStatus, Task, TaskStatus, Worker, WorkerStatus

# Task execution strategy used by _worker_loop.
# Args: (task, worker_id). Returns: (success, result_ref, log_path, error).
ExecuteFn = Callable[[Task, int], Awaitable[tuple[bool, Optional[dict], Optional[str], Optional[str]]]]

# Heartbeat interval in seconds
HEARTBEAT_INTERVAL = 30

# Poll interval when no tasks available
POLL_INTERVAL = 1


async def _try_complete_job(job_id: int) -> None:
    """Check if all tasks for a job are done and update job status accordingly."""
    async with get_sql_session() as session:
        result = await session.execute(
            select(Task.status).where(Task.job_id == job_id)
        )
        statuses = [row[0] for row in result.all()]

        if not statuses:
            return

        # If any task is still pending, claimed, or running, job is not done
        if any(s in (TaskStatus.PENDING, TaskStatus.CLAIMED, TaskStatus.RUNNING) for s in statuses):
            return

        # All tasks are in terminal state
        if any(s == TaskStatus.FAILED for s in statuses):
            await update_job_status(job_id, JobStatus.FAILED, error="One or more tasks failed")
        else:
            await update_job_status(job_id, JobStatus.COMPLETED)


async def _schedule_retry(task_id: int, current_attempt: int, error: str) -> None:
    """Reset a failed task to PENDING with incremented attempt and backoff delay."""
    base_delay = 1  # seconds
    delay = base_delay * (2 ** current_attempt)
    retry_after = datetime.utcnow() + timedelta(seconds=delay)

    async with get_sql_session() as session:
        result = await session.execute(
            select(Task).where(Task.id == task_id).with_for_update()
        )
        task = result.scalar_one()
        task.status = TaskStatus.PENDING
        task.attempt = current_attempt + 1
        task.retry_after = retry_after
        task.error = error
        task.worker_id = None
        task.claimed_at = None
        task.started_at = None
        task.completed_at = None
        if task.run_statuses:
            task.run_statuses = [*task.run_statuses[:-1], TaskStatus.FAILED.value]
        session.add(task)
        await session.commit()


async def register_worker(
    hostname: Optional[str] = None,
    pid: Optional[int] = None,
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
        status=WorkerStatus.ACTIVE,
        last_heartbeat=datetime.utcnow(),
        started_at=datetime.utcnow(),
    )

    async with get_sql_session() as session:
        session.add(worker)
        await session.commit()
        await session.refresh(worker)

    return worker


async def worker_heartbeat(worker_id: int) -> Optional[WorkerStatus]:
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
        result = await session.execute(
            select(Worker).where(Worker.id == worker_id)
        )
        worker = result.scalar_one_or_none()

        if worker is None:
            return None

        worker.last_heartbeat = datetime.utcnow()
        if worker.status != WorkerStatus.STOPPING:
            worker.status = WorkerStatus.ACTIVE
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
        result = await session.execute(
            select(Worker).where(Worker.id == worker_id)
        )
        worker = result.scalar_one_or_none()

        if worker is None:
            return False

        if worker.status in (WorkerStatus.STOPPED, WorkerStatus.STOPPING):
            return False

        worker.status = WorkerStatus.STOPPING
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
        result = await session.execute(
            select(Worker).where(Worker.id == worker_id)
        )
        worker = result.scalar_one_or_none()

        if worker is None:
            return False

        worker.status = WorkerStatus.STOPPED
        session.add(worker)
        await session.commit()

    return True


async def list_workers(status: Optional[WorkerStatus] = None) -> list[Worker]:
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
        query = query.order_by(Worker.started_at.desc())

        result = await session.execute(query)
        workers = result.scalars().all()

    return list(workers)


async def get_worker(worker_id: int) -> Optional[Worker]:
    """
    Get a worker by ID.

    Args:
        worker_id: Worker ID

    Returns:
        Worker if found, None otherwise
    """
    async with get_sql_session() as session:
        result = await session.execute(
            select(Worker).where(Worker.id == worker_id)
        )
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


async def _cancellation_monitor(task_id: int, exec_task: asyncio.Task) -> None:
    """Poll task status in DB and cancel the asyncio.Task if cancelled.

    Runs concurrently with task execution. Checks the database every
    POLL_INTERVAL seconds. When cancel_job() marks the task as CANCELLED,
    this monitor cancels the asyncio.Task, raising CancelledError at the
    next await point in the running coroutine.

    Note: asyncio cancellation is cooperative — CPU-bound code without
    await points won't be interrupted until it yields.
    """
    while not exec_task.done():
        await asyncio.sleep(POLL_INTERVAL)
        if await check_task_cancelled(task_id):
            exec_task.cancel()
            return


async def _handle_task_result(
    task: Task,
    worker_id: int,
    success: bool,
    result_ref: Optional[dict],
    log_path: Optional[str],
    error: Optional[str],
) -> bool:
    """Process the result of a task execution. Returns True if task succeeded."""
    if success:
        await update_task_status(
            task.id,
            TaskStatus.COMPLETED,
            result=result_ref,
            log_path=log_path,
        )
        print(f"Worker {worker_id} completed task {task.id}")
        await _increment_worker_stat(worker_id, "tasks_completed")
        await _try_complete_job(task.job_id)
        return True

    error = error or "Unknown error"
    print(f"Worker {worker_id} task {task.id} failed: {error}")
    async with get_sql_session() as session:
        row = await session.execute(
            select(Task.max_retries, Task.attempt).where(Task.id == task.id)
        )
        max_retries, attempt = row.one()
    if attempt < max_retries:
        await _schedule_retry(task.id, attempt, error)
        print(
            f"Worker {worker_id} task {task.id} scheduled for retry "
            f"(attempt {attempt + 1}/{max_retries})"
        )
    else:
        await update_task_status(task.id, TaskStatus.FAILED, error=error)
        await _increment_worker_stat(worker_id, "tasks_failed")
        await _try_complete_job(task.job_id)
    return False


async def _worker_loop(
    execute_fn: ExecuteFn,
    worker_id: Optional[int] = None,
    max_tasks: Optional[int] = None,
    install_signal_handlers: bool = True,
    max_empty_polls: Optional[int] = None,
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
        print(f"Worker {worker_id} registered (host={worker.hostname}, pid={worker.pid}, mode={mode_label})")
    else:
        print(f"Worker {worker_id} starting (mode={mode_label})")

    tasks_executed = 0
    last_heartbeat = datetime.utcnow()
    empty_polls = 0

    try:
        while not shutdown_requested:
            if max_tasks is not None and tasks_executed >= max_tasks:
                break

            if max_empty_polls is not None and empty_polls >= max_empty_polls:
                break

            now = datetime.utcnow()
            if (now - last_heartbeat).total_seconds() >= HEARTBEAT_INTERVAL:
                status = await worker_heartbeat(worker_id)
                last_heartbeat = now
                if status == WorkerStatus.STOPPING:
                    print(f"Worker {worker_id} received stop request")
                    shutdown_requested = True
                    continue

            task = await claim_next_task(worker_id)

            if task is None:
                empty_polls += 1
                await asyncio.sleep(POLL_INTERVAL)
                continue

            empty_polls = 0
            print(f"Worker {worker_id} executing task {task.id}: {task.entrypoint}")
            await update_task_status(task.id, TaskStatus.RUNNING)

            success, result_ref, log_path, error = await execute_fn(task, worker_id)
            if await _handle_task_result(task, worker_id, success, result_ref, log_path, error):
                tasks_executed += 1

    finally:
        await deregister_worker(worker_id)
        print(f"Worker {worker_id} stopped (executed {tasks_executed} tasks)")

    return tasks_executed


async def _execute_in_process(task: Task, worker_id: int) -> tuple[bool, Optional[dict], Optional[str], Optional[str]]:
    """Execute a task in the current async process with cancellation monitoring."""
    exec_task = asyncio.create_task(execute_task(task))
    monitor = asyncio.create_task(_cancellation_monitor(task.id, exec_task))

    try:
        data_result, log_path = await exec_task
        data_result = await register_returned_tasks(data_result, task.id, task.job_id)
        result_ref = serialize_task_result(data_result, task.job_id)
        return True, result_ref, log_path, None
    except asyncio.CancelledError:
        print(f"Task {task.id} cancelled")
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
    worker_id: Optional[int] = None,
    max_tasks: Optional[int] = None,
    install_signal_handlers: bool = True,
    max_empty_polls: Optional[int] = None,
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
