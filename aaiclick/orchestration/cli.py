"""CLI helper functions for orchestration commands.

Encapsulates the async startup logic used by ``__main__.py`` so the CLI
entry point stays thin. Job commands live in ``aaiclick.internal_api.jobs``.
"""

from __future__ import annotations

import asyncio
import signal
from typing import Any

from aaiclick.backend import is_local

from .background import BackgroundWorker
from .execution import list_workers, mp_worker_main_loop, request_worker_stop, worker_main_loop
from .models import PreservationMode
from .orch_context import orch_context
from .registered_jobs import (
    disable_job,
    enable_job,
    list_registered_jobs,
    register_job,
)


async def show_workers() -> None:
    """List all registered workers."""
    async with orch_context(with_ch=False):
        workers = await list_workers()
        if not workers:
            print("No workers found")
            return

        print(f"{'ID':<20} {'Status':<10} {'Host':<20} {'PID':<8} {'Completed':<10} {'Failed':<8}")
        print("-" * 80)
        for w in workers:
            print(
                f"{w.id:<20} {w.status.value:<10} {w.hostname:<20} {w.pid:<8} {w.tasks_completed:<10} {w.tasks_failed:<8}"
            )


async def stop_worker_cmd(worker_id_str: str) -> None:
    """Request a worker to stop gracefully after its current task."""
    try:
        worker_id = int(worker_id_str)
    except ValueError:
        print(f"Invalid worker ID: {worker_id_str}")
        return

    async with orch_context(with_ch=False):
        success = await request_worker_stop(worker_id)
        if success:
            print(f"Stop requested for worker {worker_id}")
        else:
            print(f"Worker {worker_id} not found or already stopped")


async def start_worker(max_tasks: int | None = None) -> None:
    """Start a distributed worker process.

    Each task runs in a dedicated child process for isolation.  The main
    process handles SQL (claim/status), the child process connects to
    ClickHouse.  Run ``background start`` separately for table cleanup
    and job scheduling.

    Requires distributed backends (ClickHouse server + PostgreSQL).

    Args:
        max_tasks: Maximum tasks to execute (None for unlimited).

    Raises:
        RuntimeError: If running in local mode (chdb + SQLite).
    """
    if is_local():
        raise RuntimeError(
            "'worker start' requires distributed backends (ClickHouse server + PostgreSQL). "
            "Use 'local start' for local mode (chdb + SQLite)."
        )
    async with orch_context(with_ch=False):
        await mp_worker_main_loop(max_tasks=max_tasks)


async def start_local(max_tasks: int | None = None) -> None:
    """Start worker + background cleanup in a single process (local mode).

    Everything runs in one process: the background worker, task claiming,
    and task execution all share one chdb session via the process-level
    singleton.  This avoids the file-lock conflict that occurs when
    multiple OS processes open the same chdb data directory.

    Automatically runs setup if it hasn't been run yet.

    Args:
        max_tasks: Maximum tasks to execute (None for unlimited).
    """
    from aaiclick.__main__ import setup_done

    if not setup_done():
        from aaiclick.__main__ import _run_setup

        print("Setup not yet run — running setup automatically...\n")
        _run_setup()
        print()

    background = BackgroundWorker()
    await background.start()
    try:
        async with orch_context(with_ch=True):
            await worker_main_loop(max_tasks=max_tasks)
    finally:
        await background.stop()


async def start_background(poll_interval: float = 10.0) -> None:
    """Start a standalone background cleanup worker.

    Runs until SIGTERM or SIGINT is received.

    Args:
        poll_interval: Cleanup poll interval in seconds.

    Raises:
        RuntimeError: If running in local mode (chdb + SQLite).
    """
    if is_local():
        raise RuntimeError(
            "'background start' requires distributed backends (ClickHouse server + PostgreSQL). "
            "Use 'local start' for local mode (chdb + SQLite) — it includes background cleanup."
        )
    cleanup = BackgroundWorker(poll_interval=poll_interval)
    await cleanup.start()
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.set)
    print(f"Background cleanup worker started (poll_interval={poll_interval}s)")
    await shutdown.wait()
    print("Shutting down background cleanup worker...")
    await cleanup.stop()


async def register_job_cmd(
    entrypoint: str,
    *,
    name: str | None = None,
    schedule: str | None = None,
    kwargs_json: str | None = None,
    preservation_mode: str | None = None,
) -> None:
    """Register a job in the catalog."""
    import json

    resolved_name = name or entrypoint.rsplit(".", 1)[-1]
    default_kwargs: dict[str, Any] | None = None
    if kwargs_json:
        default_kwargs = json.loads(kwargs_json)

    mode: PreservationMode | None = None
    if preservation_mode is not None:
        mode = PreservationMode(preservation_mode.upper())

    async with orch_context(with_ch=False):
        job = await register_job(
            name=resolved_name,
            entrypoint=entrypoint,
            schedule=schedule,
            default_kwargs=default_kwargs,
            preservation_mode=mode,
        )
    print(f"Registered job '{job.name}' (id={job.id})")
    if job.schedule:
        print(f"  Schedule:         {job.schedule}")
    if job.preservation_mode:
        print(f"  Preservation:     {job.preservation_mode.value}")
    if job.next_run_at:
        print(f"  Next run at:      {job.next_run_at}")


async def enable_job_cmd(name: str) -> None:
    """Enable a registered job."""
    async with orch_context(with_ch=False):
        job_id = await enable_job(name)
    print(f"Job '{name}' enabled (id={job_id})")


async def disable_job_cmd(name: str) -> None:
    """Disable a registered job."""
    async with orch_context(with_ch=False):
        job_id = await disable_job(name)
    print(f"Job '{name}' disabled (id={job_id})")


async def show_registered_jobs() -> None:
    """List registered jobs."""
    async with orch_context(with_ch=False):
        jobs = await list_registered_jobs()

    if not jobs:
        print("No registered jobs found")
        return

    print(f"{'ID':<20} {'Name':<25} {'Enabled':<9} {'Schedule':<15} {'Next Run':<20}")
    print("-" * 89)
    for j in jobs:
        next_run = j.next_run_at.strftime("%Y-%m-%d %H:%M:%S") if j.next_run_at else "-"
        print(f"{j.id:<20} {j.name:<25} {str(j.enabled):<9} {j.schedule or '-':<15} {next_run:<20}")
