"""Local-mode worker startup/shutdown helper.

See docs/designs/orchestration.md — "Topology".

Used by the FastAPI lifespan in ``aaiclick.server.app`` to start the
``BackgroundWorker`` and the execution ``execution_worker_main_loop`` for the
duration of a single local-mode (chdb + sqlite) server process.

The helper is strict: it raises ``RuntimeError`` if called outside
local mode. Distributed-mode callers run ``worker start`` and
``background start`` as separate processes instead.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from aaiclick.backend import is_local
from aaiclick.cli_renderers import render_setup_result
from aaiclick.internal_api.setup import (
    STALE_DB_REMEDY,
    is_setup_done,
    missing_local_tables,
    setup,
    stale_local_db_reason,
)

from .background import BackgroundWorker
from .execution import execution_worker_main_loop
from .orch_context import orch_context

# How long an idle or claiming worker gets to notice the stop before it is cancelled.
WORKER_STOP_GRACE = 1.0


@asynccontextmanager
async def local_runtime() -> AsyncIterator[None]:
    """Run BackgroundWorker + execution worker for the duration of the block."""
    if not is_local():
        raise RuntimeError(
            "local_runtime() requires local mode (chdb + sqlite). "
            "In distributed mode, run `worker start` and `background start` "
            "as separate processes."
        )
    # The marker can outlive the database it vouches for — an interrupted setup
    # or a wiped data dir leaves it beside an empty local.db. Re-running setup()
    # is idempotent (``create_all`` adds only missing tables), so treat that as
    # not-set-up rather than starting workers that fail on "no such table".
    if not is_setup_done() or missing_local_tables():
        render_setup_result(setup())
    else:
        # The marker carries no schema version, so an upgrade over an existing
        # install skips setup() entirely. Without this check the workers start
        # against a database missing columns and fail on the first query.
        reason = stale_local_db_reason()
        if reason:
            raise RuntimeError(f"{reason} {STALE_DB_REMEDY}")

    # One orch_context for both workers: the BackgroundWorker shares its SQL
    # engine and CH client instead of opening a second pool on local.db.
    async with orch_context():
        background = BackgroundWorker()
        await background.start()
        try:
            # uvicorn (or the outer process) owns SIGTERM/SIGINT — the worker must not steal them.
            stop = asyncio.Event()
            worker_task = asyncio.create_task(execution_worker_main_loop(install_signal_handlers=False, stop=stop))
            try:
                yield
            finally:
                # Let the worker stop between DB writes: a cancel landing inside
                # one strands SQLite's write lock. Only a running task is cancelled.
                stop.set()
                done, _ = await asyncio.wait({worker_task}, timeout=WORKER_STOP_GRACE)
                if not done:
                    worker_task.cancel()
                with suppress(asyncio.CancelledError):
                    await worker_task
        finally:
            await background.stop()
