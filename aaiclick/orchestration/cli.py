"""CLI helper functions for orchestration commands.

Encapsulates the async startup logic used by __main__.py so that
the CLI entry point stays thin.
"""

from __future__ import annotations

import asyncio
import signal
from typing import Optional

from .claiming import cancel_job
from .context import orch_context
from .job_queries import count_jobs, get_tasks_for_job, list_jobs, resolve_job
from .job_stats import compute_job_stats, print_job_stats
from .models import JobStatus
from .pg_cleanup import PgCleanupWorker
from .pg_lifecycle import PgLifecycleHandler
from .worker import list_workers, worker_main_loop


async def show_workers() -> None:
    """List all registered workers."""
    async with orch_context():
        workers = await list_workers()
        if not workers:
            print("No workers found")
            return

        print(f"{'ID':<20} {'Status':<10} {'Host':<20} {'PID':<8} {'Completed':<10} {'Failed':<8}")
        print("-" * 80)
        for w in workers:
            print(f"{w.id:<20} {w.status.value:<10} {w.hostname:<20} {w.pid:<8} {w.tasks_completed:<10} {w.tasks_failed:<8}")


async def show_job(job_ref: str) -> None:
    """Show details for a single job."""
    async with orch_context():
        job = await resolve_job(job_ref)
        if job is None:
            print(f"Job not found: {job_ref}")
            return

        print(f"ID:           {job.id}")
        print(f"Name:         {job.name}")
        print(f"Status:       {job.status.value}")
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

    async with orch_context():
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

        print(f"{'ID':<20} {'Name':<30} {'Status':<12} {'Created':<20}")
        print("-" * 82)
        for j in jobs:
            created = j.created_at.strftime("%Y-%m-%d %H:%M:%S")
            print(f"{j.id:<20} {j.name:<30} {j.status.value:<12} {created:<20}")
        print(f"\nShowing {offset + 1}-{offset + len(jobs)} of {total}")


async def start_worker(max_tasks: Optional[int] = None) -> None:
    """Start a worker process with cleanup and lifecycle support.

    Args:
        max_tasks: Maximum tasks to execute (None for unlimited).
    """
    pg_cleanup = PgCleanupWorker()
    await pg_cleanup.start()
    try:
        async with orch_context():
            await worker_main_loop(
                max_tasks=max_tasks,
                lifecycle_factory=lambda job_id: PgLifecycleHandler(job_id),
            )
    finally:
        await pg_cleanup.stop()


async def show_job_stats(job_ref: str) -> None:
    """Show execution stats for a job."""
    async with orch_context():
        job = await resolve_job(job_ref)
        if job is None:
            print(f"Job not found: {job_ref}")
            return

        tasks = await get_tasks_for_job(job.id)
        stats = compute_job_stats(job, tasks)
        print_job_stats(stats)


async def cancel_job_cmd(job_ref: str) -> None:
    """Cancel a job and all its non-terminal tasks."""
    async with orch_context():
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
    cleanup = PgCleanupWorker(poll_interval=poll_interval)
    await cleanup.start()
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.set)
    print(f"Background cleanup worker started (poll_interval={poll_interval}s)")
    await shutdown.wait()
    print("Shutting down background cleanup worker...")
    await cleanup.stop()
