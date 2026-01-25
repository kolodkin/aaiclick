"""Worker management for orchestration backend."""

import asyncio
import os
import signal
import socket
from datetime import datetime
from typing import Optional

from sqlmodel import select

from aaiclick.snowflake_id import get_snowflake_id

from .context import OrchContext, get_orch_context_session
from .models import TaskStatus, Worker, WorkerStatus

# Heartbeat interval in seconds
HEARTBEAT_INTERVAL = 30

# Poll interval when no tasks available
POLL_INTERVAL = 1


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

    async with get_orch_context_session() as session:
        session.add(worker)
        await session.commit()
        await session.refresh(worker)

    return worker


async def worker_heartbeat(worker_id: int) -> bool:
    """
    Update worker's last_heartbeat timestamp.

    Should be called periodically to indicate the worker is alive.
    Updates both last_heartbeat and ensures status is ACTIVE.

    Args:
        worker_id: Worker ID to update

    Returns:
        bool: True if worker was found and updated, False otherwise
    """
    async with get_orch_context_session() as session:
        result = await session.execute(
            select(Worker).where(Worker.id == worker_id)
        )
        worker = result.scalar_one_or_none()

        if worker is None:
            return False

        worker.last_heartbeat = datetime.utcnow()
        worker.status = WorkerStatus.ACTIVE
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
    async with get_orch_context_session() as session:
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
    async with get_orch_context_session() as session:
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
    async with get_orch_context_session() as session:
        result = await session.execute(
            select(Worker).where(Worker.id == worker_id)
        )
        return result.scalar_one_or_none()


async def worker_main_loop(
    worker_id: Optional[int] = None,
    max_tasks: Optional[int] = None,
    install_signal_handlers: bool = True,
) -> int:
    """
    Main worker execution loop.

    Continuously polls for and executes tasks until shutdown signal
    or max_tasks is reached.

    Args:
        worker_id: Worker ID (registers new worker if None)
        max_tasks: Maximum tasks to execute (None for unlimited)
        install_signal_handlers: Install SIGTERM/SIGINT handlers (default True)

    Returns:
        int: Number of tasks executed
    """
    from .claiming import claim_next_task
    from .execution import execute_task

    shutdown_requested = False

    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        shutdown_requested = True

    # Register signal handlers for graceful shutdown (optional for tests)
    if install_signal_handlers:
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    # Register worker if not provided
    if worker_id is None:
        worker = await register_worker()
        worker_id = worker.id
        print(f"Worker {worker_id} registered (host={worker.hostname}, pid={worker.pid})")
    else:
        print(f"Worker {worker_id} starting")

    tasks_executed = 0
    last_heartbeat = datetime.utcnow()

    try:
        while not shutdown_requested:
            # Check if we've reached max_tasks
            if max_tasks is not None and tasks_executed >= max_tasks:
                break

            # Send heartbeat if needed
            now = datetime.utcnow()
            if (now - last_heartbeat).total_seconds() >= HEARTBEAT_INTERVAL:
                await worker_heartbeat(worker_id)
                last_heartbeat = now

            # Try to claim a task
            task = await claim_next_task(worker_id)

            if task is None:
                # No tasks available, wait before polling again
                await asyncio.sleep(POLL_INTERVAL)
                continue

            print(f"Worker {worker_id} executing task {task.id}: {task.entrypoint}")

            try:
                from aaiclick import DataContext, create_object_from_value
                from .claiming import update_task_status

                result = await execute_task(task)

                # Convert result to Object reference if present
                result_ref = None
                if result is not None:
                    async with DataContext():
                        obj = await create_object_from_value(result)
                        result_ref = {"object_type": "object", "table_id": obj.table_id}

                # Update task status to COMPLETED
                await update_task_status(
                    task.id,
                    TaskStatus.COMPLETED,
                    result=result_ref,
                )

                tasks_executed += 1
                print(f"Worker {worker_id} completed task {task.id}")

                # Update worker stats
                async with get_orch_context_session() as session:
                    result_query = await session.execute(
                        select(Worker).where(Worker.id == worker_id)
                    )
                    worker = result_query.scalar_one_or_none()
                    if worker:
                        worker.tasks_completed += 1
                        session.add(worker)
                        await session.commit()

            except Exception as e:
                print(f"Worker {worker_id} task {task.id} failed: {e}")

                # Update task status to FAILED
                from .claiming import update_task_status

                await update_task_status(task.id, TaskStatus.FAILED, error=str(e))

                # Update worker stats
                async with get_orch_context_session() as session:
                    result_query = await session.execute(
                        select(Worker).where(Worker.id == worker_id)
                    )
                    worker = result_query.scalar_one_or_none()
                    if worker:
                        worker.tasks_failed += 1
                        session.add(worker)
                        await session.commit()

    finally:
        # Deregister worker on exit
        await deregister_worker(worker_id)
        print(f"Worker {worker_id} stopped (executed {tasks_executed} tasks)")

    return tasks_executed


async def run_worker() -> int:
    """
    Start a worker process with OrchContext.

    This is the main entry point for running a worker.
    Sets up OrchContext and runs the worker main loop.

    Returns:
        int: Number of tasks executed
    """
    async with OrchContext():
        return await worker_main_loop()
