"""Worker management for orchestration backend."""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import sys
import tempfile
from datetime import datetime
from typing import Optional

from sqlmodel import select

from aaiclick.backend import is_chdb
from aaiclick.snowflake_id import get_snowflake_id

from .claiming import check_task_cancelled, claim_next_task, update_task_status
from ..orch_context import get_sql_session, orch_context
from .worker_helpers import increment_worker_stat, try_complete_job
from ..models import Task, TaskStatus, Worker, WorkerStatus

# Heartbeat interval in seconds
HEARTBEAT_INTERVAL = 30

# Poll interval when no tasks available
POLL_INTERVAL = 1

# Module path for subprocess_runner (invoked via python -m)
_SUBPROCESS_MODULE = "aaiclick.orchestration.execution.subprocess_runner"


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


async def _cancellation_monitor(task_id: int, proc: asyncio.subprocess.Process) -> None:
    """Poll task status in DB and terminate the subprocess if cancelled.

    Runs concurrently with task execution. Checks the database every
    POLL_INTERVAL seconds. When cancel_job() marks the task as CANCELLED,
    this monitor sends SIGTERM to the subprocess.
    """
    while proc.returncode is None:
        await asyncio.sleep(POLL_INTERVAL)
        if await check_task_cancelled(task_id):
            proc.send_signal(signal.SIGTERM)
            return


def _subprocess_env() -> dict[str, str]:
    """Build environment for the task subprocess.

    When using chdb, assigns the subprocess its own temp data directory
    so it doesn't conflict with the parent's chdb lock (held by the
    snowflake ID generator singleton).
    """
    env = os.environ.copy()
    if is_chdb():
        env["AAICLICK_CH_URL"] = f"chdb://{tempfile.mkdtemp(prefix='aaiclick_task_')}"
    return env


async def worker_main_loop(
    worker_id: Optional[int] = None,
    max_tasks: Optional[int] = None,
    install_signal_handlers: bool = True,
    max_empty_polls: Optional[int] = None,
) -> int:
    """
    Main worker execution loop.

    Uses orch_context(with_ch=False) for SQL-only operations (claim, heartbeat).
    Each claimed task is executed in a fresh Python subprocess via
    asyncio.create_subprocess_exec, which creates its own full orch_context
    (SQL + ClickHouse).

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

    if install_signal_handlers:
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    async with orch_context(with_ch=False):
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

                print(f"Worker {worker_id} dispatching task {task.id}: {task.entrypoint}")

                env = _subprocess_env()
                proc = await asyncio.create_subprocess_exec(
                    sys.executable, "-m", _SUBPROCESS_MODULE, str(task.id),
                    env=env,
                )

                monitor = asyncio.create_task(
                    _cancellation_monitor(task.id, proc)
                )

                try:
                    await proc.wait()

                    if proc.returncode == 0:
                        tasks_executed += 1
                        print(f"Worker {worker_id} completed task {task.id}")
                    elif proc.returncode is not None and proc.returncode != 0:
                        print(f"Worker {worker_id} task {task.id} subprocess exited with code {proc.returncode}")
                        # If subprocess crashed without updating DB (exitcode 2),
                        # mark task as failed from the worker side.
                        if proc.returncode == 2:
                            await update_task_status(
                                task.id, TaskStatus.FAILED,
                                error="Subprocess crashed unexpectedly",
                            )
                            await increment_worker_stat(worker_id, "tasks_failed")
                            await try_complete_job(task.job_id)
                finally:
                    monitor.cancel()
                    try:
                        await monitor
                    except asyncio.CancelledError:
                        pass

        finally:
            await deregister_worker(worker_id)
            print(f"Worker {worker_id} stopped (executed {tasks_executed} tasks)")

    return tasks_executed
