"""CLI helper functions for orchestration commands.

Encapsulates the async startup logic used by ``__main__.py`` so the CLI
entry point stays thin. Worker list / stop, job, and registered-job
commands live in ``aaiclick.internal_api``. This module holds the
long-running process loops (``worker start``, ``local start``,
``background start``) that do not fit the request/response pattern.
"""

from __future__ import annotations

import asyncio
import signal

from aaiclick.backend import is_local

from .background import BackgroundWorker
from .execution import mp_worker_main_loop
from .orch_context import orch_context


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


async def start_local(host: str = "127.0.0.1", port: int = 5255) -> None:
    """Run the combined REST + MCP server with workers in a single local-mode process.

    Local mode only — chdb's file lock requires a single process. The
    server's lifespan starts the BackgroundWorker and the execution
    worker via local_runtime(). uvicorn handles SIGTERM / SIGINT.
    """
    if not is_local():
        raise RuntimeError(
            "'local start' requires local mode (chdb + SQLite). "
            "Use `worker start` + `background start` + "
            "`uvicorn aaiclick.server.app:app` in distributed mode."
        )

    # Inline: keep the [server] extra optional — users without it never hit this import.
    try:
        import uvicorn

        from aaiclick.server.app import app
    except ImportError as exc:
        raise ImportError(
            "`local start` requires the [server] extra. Install it with "
            "`pip install 'aaiclick[server]'` (or `uv add 'aaiclick[server]'`)."
        ) from exc

    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    await uvicorn.Server(config).serve()


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
