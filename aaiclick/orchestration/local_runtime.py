"""Local-mode worker startup/shutdown helper.

Used by the FastAPI lifespan in ``aaiclick.server.app`` to start the
``BackgroundWorker`` and the execution ``worker_main_loop`` for the
duration of a single local-mode (chdb + sqlite) server process.

The helper is strict: it raises ``RuntimeError`` if called outside
local mode. Distributed-mode callers run ``worker start`` and
``background start`` as separate processes instead.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from aaiclick.backend import is_local
from aaiclick.cli_renderers import render_setup_result
from aaiclick.internal_api.setup import is_setup_done, setup

from .background import BackgroundWorker
from .execution import worker_main_loop
from .orch_context import orch_context


@asynccontextmanager
async def local_runtime() -> AsyncIterator[None]:
    """Run BackgroundWorker + execution worker for the duration of the block.

    Local mode only — raises ``RuntimeError`` if ``is_local()`` is False.
    Auto-runs ``setup()`` on first use. The execution worker runs as a
    background ``asyncio.Task`` and is cancelled on shutdown.
    """
    if not is_local():
        raise RuntimeError(
            "local_runtime() requires local mode (chdb + sqlite). "
            "In distributed mode, run `worker start` and `background start` "
            "as separate processes."
        )
    if not is_setup_done():
        render_setup_result(setup())

    background = BackgroundWorker()
    await background.start()
    try:
        async with orch_context(with_ch=True):
            worker_task = asyncio.create_task(worker_main_loop(install_signal_handlers=False))
            try:
                yield
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task
    finally:
        await background.stop()
