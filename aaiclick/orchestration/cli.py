"""CLI helper functions for orchestration commands.

Encapsulates the async startup logic used by ``__main__.py`` so the CLI
entry point stays thin. Job + registered-job commands live in
``aaiclick.internal_api``.
"""

from __future__ import annotations

import asyncio
import signal

from aaiclick.backend import is_local

from .background import BackgroundWorker
from .execution import list_workers, mp_worker_main_loop, request_worker_stop, worker_main_loop
from .orch_context import orch_context


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
