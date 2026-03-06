"""CLI helper functions for orchestration commands.

Encapsulates the async startup logic used by __main__.py so that
the CLI entry point stays thin.
"""

from __future__ import annotations

import asyncio
import signal
from typing import Optional

from .context import OrchContext
from .pg_cleanup import PgCleanupWorker
from .pg_lifecycle import PgLifecycleHandler
from .worker import list_workers, worker_main_loop


async def show_workers() -> None:
    """List all registered workers."""
    async with OrchContext():
        workers = await list_workers()
        if not workers:
            print("No workers found")
            return

        print(f"{'ID':<20} {'Status':<10} {'Host':<20} {'PID':<8} {'Completed':<10} {'Failed':<8}")
        print("-" * 80)
        for w in workers:
            print(f"{w.id:<20} {w.status.value:<10} {w.hostname:<20} {w.pid:<8} {w.tasks_completed:<10} {w.tasks_failed:<8}")


async def start_worker(max_tasks: Optional[int] = None) -> None:
    """Start a worker process with cleanup and lifecycle support.

    Args:
        max_tasks: Maximum tasks to execute (None for unlimited).
    """
    pg_cleanup = PgCleanupWorker()
    await pg_cleanup.start()
    try:
        async with OrchContext():
            await worker_main_loop(
                max_tasks=max_tasks,
                lifecycle_factory=lambda job_id: PgLifecycleHandler(job_id),
            )
    finally:
        await pg_cleanup.stop()


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
