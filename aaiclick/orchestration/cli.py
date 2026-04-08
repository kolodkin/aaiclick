"""CLI helper functions for orchestration commands.

Encapsulates the async startup logic used by __main__.py so that
the CLI entry point stays thin.
"""

from __future__ import annotations

import asyncio
import json
import signal
from typing import Any, Dict, Optional

from .execution import cancel_job, list_workers, mp_worker_main_loop, request_worker_stop
from .orch_context import orch_context
from .jobs import count_jobs, compute_job_stats, get_tasks_for_job, list_jobs, print_job_stats, resolve_job
from .background import BackgroundWorker
from .models import JobStatus
from .registered_jobs import (
    disable_job,
    enable_job,
    list_registered_jobs,
    register_job,
    run_job,
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
            print(f"{w.id:<20} {w.status.value:<10} {w.hostname:<20} {w.pid:<8} {w.tasks_completed:<10} {w.tasks_failed:<8}")


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


async def show_job(job_ref: str) -> None:
    """Show details for a single job."""
    async with orch_context(with_ch=False):
        job = await resolve_job(job_ref)
        if job is None:
            print(f"Job not found: {job_ref}")
            return

        print(f"ID:           {job.id}")
        print(f"Name:         {job.name}")
        print(f"Status:       {job.status.value}")
        print(f"Run type:     {job.run_type.value}")
        print(f"Registered:   {job.registered_job_id or '-'}")
        print(f"Created at:   {job.created_at}")
        print(f"Started at:   {job.started_at or '-'}")
        print(f"Completed at: {job.completed_at or '-'}")
        if job.error:
            print(f"Error:        {job.error}")


async def show_jobs(
    *,
    status: Optional[str] = None,
    name_like: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> None:
    """List jobs with optional filtering and pagination."""
    job_status = JobStatus(status) if status else None

    async with orch_context(with_ch=False):
        total = await count_jobs(status=job_status, name_like=name_like)
        jobs = await list_jobs(
            status=job_status,
            name_like=name_like,
            limit=limit,
            offset=offset,
        )
        if not jobs:
            print("No jobs found")
            return

        print(f"{'ID':<20} {'Name':<25} {'Status':<12} {'Type':<10} {'Created':<20}")
        print("-" * 87)
        for j in jobs:
            created = j.created_at.strftime("%Y-%m-%d %H:%M:%S")
            print(f"{j.id:<20} {j.name:<25} {j.status.value:<12} {j.run_type.value:<10} {created:<20}")
        print(f"\nShowing {offset + 1}-{offset + len(jobs)} of {total}")


async def start_worker(max_tasks: Optional[int] = None) -> None:
    """Start a worker process with cleanup and lifecycle support.

    Each task runs in a dedicated child process for isolation.
    The main process handles SQLite (claim/status), the child process
    handles chdb + task execution.

    Args:
        max_tasks: Maximum tasks to execute (None for unlimited).
    """
    background = BackgroundWorker()
    await background.start()
    try:
        async with orch_context(with_ch=False):
            await mp_worker_main_loop(max_tasks=max_tasks)
    finally:
        await background.stop()


async def show_job_stats(job_ref: str) -> None:
    """Show execution stats for a job."""
    async with orch_context(with_ch=False):
        job = await resolve_job(job_ref)
        if job is None:
            print(f"Job not found: {job_ref}")
            return

        tasks = await get_tasks_for_job(job.id)
        stats = compute_job_stats(job, tasks)
        print_job_stats(stats)


async def cancel_job_cmd(job_ref: str) -> None:
    """Cancel a job and all its non-terminal tasks."""
    async with orch_context(with_ch=False):
        job = await resolve_job(job_ref)
        if job is None:
            print(f"Job not found: {job_ref}")
            return
        success = await cancel_job(job.id)
        if success:
            print(f"Job {job.id} cancelled")
        else:
            print(f"Job {job.id} already in terminal state")


async def start_background(poll_interval: float = 10.0) -> None:
    """Start a standalone background cleanup worker.

    Runs until SIGTERM or SIGINT is received.

    Args:
        poll_interval: Cleanup poll interval in seconds.
    """
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
    name: Optional[str] = None,
    schedule: Optional[str] = None,
    kwargs_json: Optional[str] = None,
) -> None:
    """Register a job in the catalog."""
    resolved_name = name or entrypoint.rsplit(".", 1)[-1]
    default_kwargs: Optional[Dict[str, Any]] = None
    if kwargs_json:
        default_kwargs = json.loads(kwargs_json)

    async with orch_context(with_ch=False):
        job = await register_job(
            name=resolved_name,
            entrypoint=entrypoint,
            schedule=schedule,
            default_kwargs=default_kwargs,
        )
    print(f"Registered job '{job.name}' (id={job.id})")
    if job.schedule:
        print(f"  Schedule:    {job.schedule}")
    if job.next_run_at:
        print(f"  Next run at: {job.next_run_at}")


async def run_job_cmd(
    name_or_entrypoint: str,
    *,
    kwargs_json: Optional[str] = None,
) -> None:
    """Run a job immediately."""
    kwargs: Optional[Dict[str, Any]] = None
    if kwargs_json:
        kwargs = json.loads(kwargs_json)

    # If it looks like a dotted path, use as entrypoint; otherwise treat as name
    if "." in name_or_entrypoint:
        entrypoint = name_or_entrypoint
        name = name_or_entrypoint.rsplit(".", 1)[-1]
    else:
        name = name_or_entrypoint
        entrypoint = name_or_entrypoint

    async with orch_context(with_ch=False):
        job = await run_job(name=name, entrypoint=entrypoint, kwargs=kwargs)
    print(f"Job '{job.name}' created (id={job.id}, run_type={job.run_type.value})")


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
