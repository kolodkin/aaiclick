"""Worker management for orchestration backend."""

from __future__ import annotations

import asyncio
import os
import signal
import socket
from datetime import datetime, timedelta
from typing import Optional

from sqlmodel import select

from aaiclick.snowflake_id import get_snowflake_id

from .claiming import check_task_cancelled, claim_next_task, update_job_status, update_task_status
from ..orch_context import get_sql_session
from .runner import execute_task, register_returned_tasks, serialize_task_result
from ..models import Job, JobStatus, Task, TaskStatus, Worker, WorkerStatus

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


async def _set_worker_status(
    worker_id: int,
    new_status: WorkerStatus,
    reject_statuses: tuple[WorkerStatus, ...] = (),
) -> bool:
    """Update a worker's status, optionally rejecting certain current statuses.

    Returns True if the worker was found and updated, False if not found
    or if its current status is in reject_statuses.
    """
    async with get_sql_session() as session:
        result = await session.execute(
            select(Worker).where(Worker.id == worker_id)
        )
        worker = result.scalar_one_or_none()

        if worker is None:
            return False

        if worker.status in reject_statuses:
            return False

        worker.status = new_status
        session.add(worker)
        await session.commit()

    return True


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
    return await _set_worker_status(
        worker_id,
        WorkerStatus.STOPPING,
        reject_statuses=(WorkerStatus.STOPPED, WorkerStatus.STOPPING),
    )


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
    return await _set_worker_status(worker_id, WorkerStatus.STOPPED)


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


async def worker_main_loop(
    worker_id: Optional[int] = None,
    max_tasks: Optional[int] = None,
    install_signal_handlers: bool = True,
    max_empty_polls: Optional[int] = None,
) -> int:
    """
    Main worker execution loop.

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
    shutdown_requested = False

    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        shutdown_requested = True

    # Register signal handlers for graceful shutdown (optional for tests)
    if install_signal_handlers:
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    # Default path: auto-register a new worker in the DB.
    # Tests pass an explicit worker_id to reuse an existing record.
    if worker_id is None:
        worker = await register_worker()
        worker_id = worker.id
        print(f"Worker {worker_id} registered (host={worker.hostname}, pid={worker.pid})")
    else:
        print(f"Worker {worker_id} starting")

    tasks_executed = 0
    last_heartbeat = datetime.utcnow()
    empty_polls = 0

    try:
        while not shutdown_requested:
            # Check if we've reached max_tasks
            if max_tasks is not None and tasks_executed >= max_tasks:
                break

            # Testing-only exit: allow tests to stop the loop after N empty polls
            # instead of polling forever. In production max_empty_polls is None.
            if max_empty_polls is not None and empty_polls >= max_empty_polls:
                break

            # Send heartbeat if needed; also check for graceful stop requests
            now = datetime.utcnow()
            if (now - last_heartbeat).total_seconds() >= HEARTBEAT_INTERVAL:
                status = await worker_heartbeat(worker_id)
                last_heartbeat = now
                if status == WorkerStatus.STOPPING:
                    print(f"Worker {worker_id} received stop request")
                    shutdown_requested = True
                    continue

            # Try to claim a task
            task = await claim_next_task(worker_id)

            if task is None:
                # No tasks available, wait before polling again
                empty_polls += 1
                await asyncio.sleep(POLL_INTERVAL)
                continue

            # Reset empty polls counter when we find a task
            empty_polls = 0

            print(f"Worker {worker_id} executing task {task.id}: {task.entrypoint}")
            await update_task_status(task.id, TaskStatus.RUNNING)

            # Wrap execution in an asyncio.Task with a cancellation monitor
            # so that cancel_job() can interrupt running tasks.
            async def _run_task(t):
                return await execute_task(t)

            exec_task = asyncio.create_task(_run_task(task))
            monitor = asyncio.create_task(
                _cancellation_monitor(task.id, exec_task)
            )

            try:
                data_result, log_path = await exec_task

                data_result = await register_returned_tasks(data_result, task.id, task.job_id)

                result_ref = serialize_task_result(data_result, task.job_id)

                await update_task_status(
                    task.id,
                    TaskStatus.COMPLETED,
                    result=result_ref,
                    log_path=log_path,
                )

                tasks_executed += 1
                print(f"Worker {worker_id} completed task {task.id}")
                await _increment_worker_stat(worker_id, "tasks_completed")
                await _try_complete_job(task.job_id)

            except asyncio.CancelledError:
                print(f"Worker {worker_id} task {task.id} cancelled")

            except Exception as e:
                print(f"Worker {worker_id} task {task.id} failed: {e}")
                async with get_sql_session() as session:
                    row = await session.execute(
                        select(Task.max_retries, Task.attempt).where(
                            Task.id == task.id
                        )
                    )
                    max_retries, attempt = row.one()
                if attempt < max_retries:
                    await _schedule_retry(task.id, attempt, str(e))
                    print(
                        f"Worker {worker_id} task {task.id} scheduled for retry "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                else:
                    await update_task_status(task.id, TaskStatus.FAILED, error=str(e))
                    await _increment_worker_stat(worker_id, "tasks_failed")
                    await _try_complete_job(task.job_id)

            finally:
                monitor.cancel()
                try:
                    await monitor
                except asyncio.CancelledError:
                    pass

    finally:
        # Deregister worker on exit
        await deregister_worker(worker_id)
        print(f"Worker {worker_id} stopped (executed {tasks_executed} tasks)")

    return tasks_executed
