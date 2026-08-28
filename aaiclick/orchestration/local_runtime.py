"""Local-mode worker startup/shutdown helper.

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
    setup,
    stale_local_db,
    stale_local_db_message,
)

from .background import BackgroundWorker
from .execution import execution_worker_main_loop
from .orch_context import orch_context


@asynccontextmanager
async def local_runtime() -> AsyncIterator[None]:
    """Run BackgroundWorker + execution worker for the duration of the block."""
    if not is_local():
        raise RuntimeError(
            "local_runtime() requires local mode (chdb + sqlite). "
            "In distributed mode, run `worker start` and `background start` "
            "as separate processes."
        )
    if not is_setup_done():
        render_setup_result(setup())
    else:
        # The marker carries no schema version, so an upgrade over an existing
        # install skips setup() entirely. Without this check the workers start
        # against a database missing columns and fail on the first query.
        stale = stale_local_db()
        if stale:
            raise RuntimeError(f"{stale_local_db_message(stale)} {STALE_DB_REMEDY}")

    background = BackgroundWorker()
    await background.start()
    try:
        async with orch_context(with_ch=True):
            # uvicorn (or the outer process) owns SIGTERM/SIGINT — the worker must not steal them.
            worker_task = asyncio.create_task(execution_worker_main_loop(install_signal_handlers=False))
            try:
                yield
            finally:
                worker_task.cancel()
                with suppress(asyncio.CancelledError):
                    await worker_task
    finally:
        await background.stop()
