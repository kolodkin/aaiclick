"""CLI helper functions for orchestration commands.

See docs/designs/orchestration.md — "CLI".

Encapsulates the async startup logic used by ``__main__.py`` so the CLI
entry point stays thin. Execution worker list / stop, job, and registered-job
commands live in ``aaiclick.internal_api``. This module holds the
long-running process loops (``execution-worker start``, ``local start``,
``background start``) that do not fit the request/response pattern.
"""

from __future__ import annotations

import asyncio
import copy
import logging
import logging.config
import signal

from aaiclick.backend import LOOPBACK_HOSTS, is_local

from .background import BackgroundWorker
from .execution import mp_worker_main_loop
from .execution.runner_env import validate_worker_runner
from .orch_context import orch_context

logger = logging.getLogger(__name__)


async def start_execution_worker(max_tasks: int | None = None) -> None:
    """Start a distributed execution worker process.

    Each task runs in a dedicated child process for isolation.  The main
    process handles SQL (claim/status), the child process connects to
    ClickHouse.  Run ``background start`` separately for table cleanup
    and job scheduling.

    Requires distributed backends (ClickHouse server + PostgreSQL).

    Args:
        max_tasks: Maximum tasks to execute (None for unlimited).

    Raises:
        RuntimeError: If running in local mode (chdb + SQLite).
        ValueError: If ``AAICLICK_RUNNER`` is unknown or inconsistent.
    """
    validate_worker_runner()
    if is_local():
        raise RuntimeError(
            "'execution-worker start' requires distributed backends (ClickHouse server + PostgreSQL). "
            "Use 'local start' for local mode (chdb + SQLite)."
        )
    configure_logging()
    async with orch_context():
        await mp_worker_main_loop(max_tasks=max_tasks)


# INFO stderr logging for the ``aaiclick`` logger tree. Python's WARNING
# default would drop the workers' "executing task" / "completed task" lines;
# the timestamp makes a gap between two of them visible from the log alone.
_AAICLICK_LOGGING = {
    "formatters": {"aaiclick": {"format": "%(asctime)s %(levelname)s %(name)s: %(message)s"}},
    "handlers": {"aaiclick": {"class": "logging.StreamHandler", "formatter": "aaiclick", "stream": "ext://sys.stderr"}},
    "loggers": {"aaiclick": {"handlers": ["aaiclick"], "level": "INFO", "propagate": False}},
}


def configure_logging() -> None:
    """Apply ``_AAICLICK_LOGGING`` to this process (the standalone worker loops)."""
    logging.config.dictConfig({"version": 1, "disable_existing_loggers": False, **_AAICLICK_LOGGING})


def local_log_config(base: dict) -> dict:
    """Return uvicorn's logging config (``base``) extended with ``_AAICLICK_LOGGING``.

    Handed to uvicorn rather than applied here so it also reaches the
    ``--reload`` child process, where the workers actually run.
    """
    config = copy.deepcopy(base)
    for section, entries in _AAICLICK_LOGGING.items():
        config[section].update(entries)
    return config


async def start_local(host: str = "127.0.0.1", port: int = 5255, reload: bool = False) -> None:
    """Run the combined REST + MCP server with workers in a single local-mode process.

    Local mode only — chdb's file lock requires a single process. The
    server's lifespan starts the BackgroundWorker and the execution
    worker via local_runtime(). uvicorn handles SIGTERM / SIGINT.

    Pass ``reload=True`` for dev: uvicorn watches the source tree and
    restarts the server on every save. Reload mode needs the app as
    an import string, not an object.
    """
    if not is_local():
        raise RuntimeError(
            "'local start' requires local mode (chdb + SQLite). "
            "Use `worker start` + `background start` + "
            "`uvicorn aaiclick.server.app:app` in distributed mode."
        )
    if host not in LOOPBACK_HOSTS:
        raise RuntimeError(
            f"'local start' runs with auth disabled and binds loopback only, not {host!r}. "
            "Use distributed mode to serve other machines."
        )

    # Inline: keep the [server] extra optional — users without it never hit this import.
    try:
        import uvicorn  # noqa: PLC0415
        from uvicorn.config import LOGGING_CONFIG  # noqa: PLC0415

        if not reload:
            # The app module also needs the [server] extra.
            from aaiclick.server.app import app  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "`local start` requires the [server] extra. Install it with "
            "`pip install 'aaiclick[server]'` (or `uv add 'aaiclick[server]'`)."
        ) from exc

    # Reload watches the filesystem and re-imports — uvicorn needs the app as an
    # import string in that mode. In normal mode we hand it the app object directly.
    target = "aaiclick.server.app:app" if reload else app
    config = uvicorn.Config(
        target, host=host, port=port, log_level="info", log_config=local_log_config(LOGGING_CONFIG), reload=reload
    )
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
    configure_logging()
    cleanup = BackgroundWorker(poll_interval=poll_interval)
    await cleanup.start()
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.set)
    logger.info("Background cleanup worker started (poll_interval=%ss)", poll_interval)
    await shutdown.wait()
    logger.info("Shutting down background cleanup worker...")
    await cleanup.stop()
