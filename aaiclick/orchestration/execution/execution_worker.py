"""ExecutionWorker management for orchestration backend."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
from collections.abc import Awaitable, Callable
from typing import Any, NamedTuple, Protocol, TypeVar, cast

from sqlalchemy import update
from sqlalchemy.engine import CursorResult
from sqlmodel import col, select

from aaiclick.async_wait import wait_or_timeout
from aaiclick.snowflake import get_snowflake_id

from ...datetime_utils import utc_now
from ..models import (
    CANCELLING_TASK_STATUSES,
    EXECUTION_WORKER_ACTIVE,
    EXECUTION_WORKER_STOPPED,
    EXECUTION_WORKER_STOPPING,
    TASK_FAILED,
    TASK_PENDING_FAILURE_CLEANUP,
    TASK_RUNNING,
    ExecutionWorker,
    ExecutionWorkerStatus,
    RunnerMode,
    Task,
)
from ..orch_context import get_sql_session
from ..runner_config import ENTRY_MODULE, EntryType, ImageSourceT
from .claiming import (
    check_run_aborted,
    claim_next_task,
    complete_task_and_roll_up,
    release_cancelled_run,
    update_task_status,
)
from .runner import execute_task, serialize_task_result

logger = logging.getLogger(__name__)

# Task execution strategy used by _execution_worker_loop.
# Args: (task, execution_worker_id). Returns: (success, result_ref, error).
ExecuteFn = Callable[[Task, int], Awaitable[tuple[bool, dict | None, str | None]]]

# Heartbeat interval in seconds
HEARTBEAT_INTERVAL = 30

# Poll interval when no tasks available
POLL_INTERVAL = 1


def parse_task_timeout() -> float | None:
    """Per-task wall-clock cap from ``AAICLICK_TASK_TIMEOUT`` (seconds), or None.

    Shared by every runner so the three ExecuteFns parse it identically (an
    empty value reads as no timeout)."""
    raw = os.environ.get("AAICLICK_TASK_TIMEOUT")
    return float(raw) if raw else None


class RunnerResult(NamedTuple):
    """Outcome of running a single task on a vehicle.

    Shared by every runner so the driver and ``_handle_task_result`` speak
    one shape instead of an anonymous tuple per module."""

    success: bool
    result_ref: dict | None
    error: str | None


class JobDispatch(NamedTuple):
    """A task's runner choice plus the launch spec its runner needs.

    Loaded once per task (in ``dispatch._resolve_dispatch``) so the image-based
    runners don't re-query the ``Job`` for ``kubernetes_config`` after dispatch
    already read the row to pick the runner. The launch tag is derived from
    ``image_source`` by ``docker_build.resolve_launch_image``."""

    runner_mode: RunnerMode
    kubernetes_config: dict | None
    entry_type: EntryType = ENTRY_MODULE
    command: list[str] | None = None
    command_env: dict[str, str] | None = None
    image_source: ImageSourceT | None = None


# A vehicle's opaque handle (``H``) and ``wait`` payload (``P``) are
# runner-specific: the driver only shuttles them between the vehicle's own
# methods, so they stay generic rather than ``Any``.
H = TypeVar("H")
P = TypeVar("P")


class TaskVehicle(Protocol[H, P]):
    """The varying surface between runners (subprocess / docker / k8s).

    Everything generic — heartbeating, cancellation polling, terminate-on-
    cancel, result override — lives in ``drive_vehicle``. A runner only has
    to spawn an execution vehicle, wait for it, kill it, and read its
    result back."""

    async def launch(self, task: Task, execution_worker_id: int) -> H:
        """Start the vehicle; return an opaque handle the other ops use."""
        ...

    async def wait(self, handle: H, timeout: float | None) -> tuple[int, str | None, P]:
        """Block until the vehicle exits; return ``(exit_code, error, payload)``.

        ``error`` is non-None when the vehicle itself failed to run to
        completion (timeout, crash) rather than the task failing. ``payload`` is
        a runner-specific result object passed straight to ``collect`` (so the
        result read happens here, in async context, not in the sync ``collect``)."""
        ...

    async def poll_cancelled(self, task: Task) -> bool:
        """Return True if the run was aborted and the vehicle should die."""
        ...

    async def terminate(self, handle: H) -> None:
        """Forcibly stop the vehicle (cancellation / timeout path)."""
        ...

    def collect(self, handle: H, exit_code: int, error: str | None, was_cancelled: bool, payload: P) -> RunnerResult:
        """Translate the exited vehicle (and ``wait``'s ``payload``) into a ``RunnerResult``."""
        ...

    async def cleanup(self, handle: H) -> None:
        """Release vehicle resources (e.g. ``docker rm``). Always runs."""
        ...


async def _heartbeat_while_waiting(
    execution_worker_id: int,
    done: asyncio.Event,
    interval: float,
    heartbeat_fn: Callable[[int], Awaitable[Any]],
) -> None:
    """Heartbeat every ``interval`` seconds until ``done`` is set.

    A failed heartbeat is logged and retried next tick; one transient DB error
    must not get a long task's worker declared dead."""
    while not await wait_or_timeout(done, interval):
        try:
            await heartbeat_fn(execution_worker_id)
        except Exception:
            logger.exception("ExecutionWorker %s heartbeat failed; retrying next tick", execution_worker_id)


async def _watch_for_cancellation(
    vehicle: TaskVehicle[H, P],
    task: Task,
    handle: H,
    done: asyncio.Event,
    cancelled: asyncio.Event,
    poll_interval: float,
) -> None:
    """Poll for cancellation; terminate the vehicle and set ``cancelled``.

    ``cancelled`` is the source of truth the driver reads after gather —
    reading this task's return value is racy (it may still be sleeping in
    ``wait_for`` when ``done`` is set externally)."""
    while not await wait_or_timeout(done, poll_interval):
        try:
            aborted = await vehicle.poll_cancelled(task)
        except Exception:
            logger.exception("Task %s cancellation poll failed; retrying next tick", task.id)
            continue
        if aborted:
            cancelled.set()
            await vehicle.terminate(handle)
            return


async def drive_vehicle(
    task: Task,
    execution_worker_id: int,
    vehicle: TaskVehicle[H, P],
    *,
    timeout: float | None,
    poll_interval: float,
    heartbeat_fn: Callable[[int], Awaitable[Any]],
    heartbeat_interval: float = HEARTBEAT_INTERVAL,
) -> RunnerResult:
    """Run ``task`` on ``vehicle``, owning the generic lifecycle.

    Launches the vehicle, heartbeats and polls for cancellation
    concurrently while it runs, then reads the result back. A fired
    cancellation overrides whatever the vehicle wrote — the host's
    explicit kill is the source of truth."""
    handle = await vehicle.launch(task, execution_worker_id)
    done = asyncio.Event()
    cancelled = asyncio.Event()
    heartbeat = asyncio.create_task(
        _heartbeat_while_waiting(execution_worker_id, done, heartbeat_interval, heartbeat_fn)
    )
    cancel_watcher = asyncio.create_task(_watch_for_cancellation(vehicle, task, handle, done, cancelled, poll_interval))

    try:
        exit_code, error, payload = await vehicle.wait(handle, timeout)
        done.set()
        await asyncio.gather(heartbeat, cancel_watcher, return_exceptions=True)
        return vehicle.collect(handle, exit_code, error, cancelled.is_set(), payload)
    finally:
        done.set()
        await asyncio.gather(heartbeat, cancel_watcher, return_exceptions=True)
        await vehicle.cleanup(handle)


async def _set_pending_failure_cleanup(task_id: int, error: str, expected_epoch: int | None = None) -> bool:
    """Transition a failed task to PENDING_FAILURE_CLEANUP for background ref cleanup.

    Returns False without writing when the run no longer owns the task: it is
    cancelling or CANCELLED (a killed run's failure report must not resurrect
    it as a retry — mirrors ``update_task_status``), or ``expected_epoch`` no
    longer matches ``run_epoch`` (``clear_task`` reset it).

    Both guards sit in the UPDATE's WHERE clause, so they hold atomically on
    every backend, not only where ``FOR UPDATE`` locks the row; the rowcount
    is the verdict. The prior SELECT only feeds ``run_statuses``.
    """
    async with get_sql_session() as session:
        task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
        if task is None:
            return False
        values: dict[str, str | list[str]] = {"status": TASK_PENDING_FAILURE_CLEANUP, "error": error}
        if task.run_statuses:
            values["run_statuses"] = [*task.run_statuses[:-1], TASK_FAILED]
        stmt = update(Task).where(col(Task.id) == task_id, col(Task.status).not_in(CANCELLING_TASK_STATUSES))
        if expected_epoch is not None:
            stmt = stmt.where(col(Task.run_epoch) == expected_epoch)
        result = await session.execute(stmt.values(**values))
        await session.commit()
        return cast(CursorResult, result).rowcount > 0


async def register_execution_worker(
    hostname: str | None = None,
    pid: int | None = None,
) -> ExecutionWorker:
    """
    Register a new execution_worker process.

    Creates a ExecutionWorker record in the database with ACTIVE status.
    Uses system hostname and PID if not provided.

    Args:
        hostname: ExecutionWorker hostname (default: system hostname)
        pid: ExecutionWorker process ID (default: current process PID)

    Returns:
        ExecutionWorker: Registered execution_worker with ID populated
    """
    execution_worker_id = get_snowflake_id()
    execution_worker = ExecutionWorker(
        id=execution_worker_id,
        hostname=hostname or socket.gethostname(),
        pid=pid or os.getpid(),
        status=EXECUTION_WORKER_ACTIVE,
        last_heartbeat=utc_now(),
        started_at=utc_now(),
    )

    async with get_sql_session() as session:
        session.add(execution_worker)
        await session.commit()
        await session.refresh(execution_worker)

    return execution_worker


async def execution_worker_heartbeat(execution_worker_id: int) -> ExecutionWorkerStatus | None:
    """
    Update execution_worker's last_heartbeat timestamp.

    Should be called periodically to indicate the execution_worker is alive.
    Updates last_heartbeat and ensures status is ACTIVE (unless STOPPING).

    Args:
        execution_worker_id: ExecutionWorker ID to update

    Returns:
        The execution_worker's current status after update, or None if execution_worker not found.
    """
    async with get_sql_session() as session:
        result = await session.execute(select(ExecutionWorker).where(ExecutionWorker.id == execution_worker_id))
        execution_worker = result.scalar_one_or_none()

        if execution_worker is None:
            return None

        execution_worker.last_heartbeat = utc_now()
        if execution_worker.status != EXECUTION_WORKER_STOPPING:
            execution_worker.status = EXECUTION_WORKER_ACTIVE
        session.add(execution_worker)
        await session.commit()

    return execution_worker.status


async def request_execution_worker_stop(execution_worker_id: int) -> bool:
    """
    Request a execution_worker to stop gracefully.

    Sets the execution_worker status to STOPPING. The execution_worker will finish its current
    task and exit on the next heartbeat check.

    Args:
        execution_worker_id: ExecutionWorker ID to stop

    Returns:
        bool: True if execution_worker was found and set to STOPPING,
              False if not found or already in a terminal state
    """
    async with get_sql_session() as session:
        result = await session.execute(select(ExecutionWorker).where(ExecutionWorker.id == execution_worker_id))
        execution_worker = result.scalar_one_or_none()

        if execution_worker is None:
            return False

        if execution_worker.status in (EXECUTION_WORKER_STOPPED, EXECUTION_WORKER_STOPPING):
            return False

        execution_worker.status = EXECUTION_WORKER_STOPPING
        session.add(execution_worker)
        await session.commit()

    return True


async def deregister_execution_worker(execution_worker_id: int) -> bool:
    """
    Mark a execution_worker as stopped.

    Updates execution_worker status to STOPPED. Does not delete the record
    to preserve history.

    Args:
        execution_worker_id: ExecutionWorker ID to deregister

    Returns:
        bool: True if execution_worker was found and updated, False otherwise
    """
    async with get_sql_session() as session:
        result = await session.execute(select(ExecutionWorker).where(ExecutionWorker.id == execution_worker_id))
        execution_worker = result.scalar_one_or_none()

        if execution_worker is None:
            return False

        execution_worker.status = EXECUTION_WORKER_STOPPED
        session.add(execution_worker)
        await session.commit()

    return True


async def list_execution_workers(status: ExecutionWorkerStatus | None = None) -> list[ExecutionWorker]:
    """
    List execution_workers, optionally filtered by status.

    Args:
        status: Filter by execution_worker status (default: all execution_workers)

    Returns:
        list[ExecutionWorker]: List of execution_workers matching criteria
    """
    async with get_sql_session() as session:
        query = select(ExecutionWorker)
        if status is not None:
            query = query.where(ExecutionWorker.status == status)
        query = query.order_by(col(ExecutionWorker.started_at).desc())

        result = await session.execute(query)
        execution_workers = result.scalars().all()

    return list(execution_workers)


async def get_execution_worker(execution_worker_id: int) -> ExecutionWorker | None:
    """
    Get a execution_worker by ID.

    Args:
        execution_worker_id: ExecutionWorker ID

    Returns:
        ExecutionWorker if found, None otherwise
    """
    async with get_sql_session() as session:
        result = await session.execute(select(ExecutionWorker).where(ExecutionWorker.id == execution_worker_id))
        return result.scalar_one_or_none()


async def _increment_execution_worker_stat(execution_worker_id: int, field: str) -> None:
    """Increment a execution_worker stat field (tasks_completed or tasks_failed)."""
    async with get_sql_session() as session:
        result = await session.execute(select(ExecutionWorker).where(ExecutionWorker.id == execution_worker_id))
        execution_worker = result.scalar_one_or_none()
        if execution_worker:
            setattr(execution_worker, field, getattr(execution_worker, field) + 1)
            session.add(execution_worker)
            await session.commit()


async def _cancellation_monitor(task_id: int, exec_task: asyncio.Task, expected_epoch: int) -> bool:
    """Poll task state in DB and cancel the asyncio.Task if the run is aborted.

    Returns True when this monitor cancelled ``exec_task``, False when the
    task finished (or was cancelled by someone else) first — the runner uses
    it to tell a cancelled run from a cancelled worker.

    Runs concurrently with task execution. Checks the database every
    POLL_INTERVAL seconds. When cancel_job() marks the task
    PENDING_CANCELLED_CLEANUP or clear_task() bumps its run_epoch past
    ``expected_epoch``, this monitor
    cancels the asyncio.Task, raising CancelledError at the next await point
    in the running coroutine.

    ``exec_task`` doubles as the stop signal: the poll sleep waits on it with
    a timeout, so the monitor wakes the moment the task finishes and returns
    on its own — callers just ``await`` it, no cancel needed.

    Note: asyncio cancellation is cooperative — CPU-bound code without
    await points won't be interrupted until it yields.
    """
    while not exec_task.done():
        await asyncio.wait({exec_task}, timeout=POLL_INTERVAL)
        if exec_task.done():
            return False
        try:
            aborted = await check_run_aborted(task_id, expected_epoch)
        except Exception:
            logger.exception("Task %s cancellation poll failed; retrying next tick", task_id)
            continue
        if aborted:
            exec_task.cancel()
            return True
    return False


async def _discard_run(task: Task, execution_worker_id: int, outcome: str) -> None:
    """A refused status write means the run no longer owns the task (cleared or
    cancelled); a cancelled task still needs its ownership released."""
    await release_cancelled_run(task.id, expected_epoch=task.run_epoch)
    logger.info("ExecutionWorker %s task %s %s discarded (cleared or cancelled)", execution_worker_id, task.id, outcome)


async def _handle_task_result(
    task: Task,
    execution_worker_id: int,
    success: bool,
    result_ref: dict | None,
    error: str | None,
) -> bool:
    """Process the result of a task execution. Returns True if task succeeded."""
    if success:
        if not await complete_task_and_roll_up(task.id, task.job_id, result_ref, expected_epoch=task.run_epoch):
            await _discard_run(task, execution_worker_id, "completion")
            return False
        logger.info("ExecutionWorker %s completed task %s", execution_worker_id, task.id)
        await _increment_execution_worker_stat(execution_worker_id, "tasks_completed")
        return True

    error = error or "Unknown error"
    logger.warning("ExecutionWorker %s task %s failed: %s", execution_worker_id, task.id, error)
    if not await _set_pending_failure_cleanup(task.id, error, expected_epoch=task.run_epoch):
        await _discard_run(task, execution_worker_id, "failure")
        return False
    await _increment_execution_worker_stat(execution_worker_id, "tasks_failed")
    logger.info("ExecutionWorker %s task %s set to PENDING_FAILURE_CLEANUP", execution_worker_id, task.id)
    return False


async def _execution_worker_loop(
    execute_fn: ExecuteFn,
    execution_worker_id: int | None = None,
    max_tasks: int | None = None,
    install_signal_handlers: bool = True,
    max_empty_polls: int | None = None,
    mode_label: str = "async",
) -> int:
    """Shared execution_worker loop used by both async and multiprocessing execution_workers.

    Claims tasks, delegates execution to ``execute_fn``, and handles
    status updates, retries, and job completion.

    Args:
        execute_fn: Async callable (Task) -> (success, result_ref, error).
        execution_worker_id: ExecutionWorker ID (registers new execution_worker if None).
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

    if execution_worker_id is None:
        execution_worker = await register_execution_worker()
        execution_worker_id = execution_worker.id
        logger.info(
            "ExecutionWorker %s registered (host=%s, pid=%s, mode=%s)",
            execution_worker_id,
            execution_worker.hostname,
            execution_worker.pid,
            mode_label,
        )
    else:
        logger.info("ExecutionWorker %s starting (mode=%s)", execution_worker_id, mode_label)

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
                status = await execution_worker_heartbeat(execution_worker_id)
                last_heartbeat = now
                if status == EXECUTION_WORKER_STOPPING:
                    logger.info("ExecutionWorker %s received stop request", execution_worker_id)
                    shutdown_requested = True
                    continue

            task = await claim_next_task(execution_worker_id)

            if task is None:
                empty_polls += 1
                await asyncio.sleep(POLL_INTERVAL)
                continue

            empty_polls = 0
            logger.info("ExecutionWorker %s executing task %s: %s", execution_worker_id, task.id, task.entrypoint)
            await update_task_status(task.id, TASK_RUNNING, expected_epoch=task.run_epoch)

            try:
                success, result_ref, error = await execute_fn(task, execution_worker_id)
            except Exception as e:
                # Dispatch failures (missing image tag, rejected ``kubectl
                # apply``) are the task's, not the worker's: escaping would stop
                # the loop and leave the task RUNNING under a STOPPED worker the
                # dead-worker sweep never revisits.
                logger.exception("ExecutionWorker %s task %s dispatch raised", execution_worker_id, task.id)
                success, result_ref, error = False, None, f"{type(e).__name__}: {e}"
            if await _handle_task_result(task, execution_worker_id, success, result_ref, error):
                tasks_executed += 1

    finally:
        await deregister_execution_worker(execution_worker_id)
        logger.info("ExecutionWorker %s stopped (executed %s tasks)", execution_worker_id, tasks_executed)

    return tasks_executed


async def _execute_in_process(task: Task, execution_worker_id: int) -> tuple[bool, dict | None, str | None]:
    """Execute a task in the current async process with cancellation monitoring.

    Heartbeats while the task runs; the loop only heartbeats between claims,
    so a task longer than the dead-worker timeout would otherwise be declared
    dead and run twice.

    ``CancelledError`` is either the monitor aborting the run (``cancel_job``
    / ``clear_task``), reported as a non-failure so the loop moves on, or the
    worker itself being cancelled (``local start`` shutdown), which propagates
    so the loop exits instead of orphaning the task.
    """
    exec_task = asyncio.create_task(execute_task(task))
    monitor = asyncio.create_task(_cancellation_monitor(task.id, exec_task, task.run_epoch))
    done = asyncio.Event()
    heartbeat = asyncio.create_task(
        _heartbeat_while_waiting(execution_worker_id, done, HEARTBEAT_INTERVAL, execution_worker_heartbeat)
    )

    try:
        data_result = await exec_task
        result_ref = serialize_task_result(data_result, task.job_id)
        return True, result_ref, None
    except asyncio.CancelledError:
        if not await monitor:  # self-terminates once exec_task is done
            raise
        logger.info("Task %s cancelled", task.id)
        return False, None, None
    except Exception as e:
        return False, None, str(e)
    finally:
        done.set()
        await asyncio.gather(monitor, heartbeat, return_exceptions=True)


async def execution_worker_main_loop(
    execution_worker_id: int | None = None,
    max_tasks: int | None = None,
    install_signal_handlers: bool = True,
    max_empty_polls: int | None = None,
) -> int:
    """Main execution_worker execution loop (in-process async execution).

    Continuously polls for and executes tasks until shutdown signal
    or max_tasks is reached. Must be called inside an active orch_context.

    Args:
        execution_worker_id: ExecutionWorker ID (registers new execution_worker if None)
        max_tasks: Maximum tasks to execute (None for unlimited)
        install_signal_handlers: Install SIGTERM/SIGINT handlers (default True)
        max_empty_polls: Exit after N consecutive empty polls (None for unlimited)

    Returns:
        int: Number of tasks executed
    """
    return await _execution_worker_loop(
        execute_fn=_execute_in_process,
        execution_worker_id=execution_worker_id,
        max_tasks=max_tasks,
        install_signal_handlers=install_signal_handlers,
        max_empty_polls=max_empty_polls,
    )
