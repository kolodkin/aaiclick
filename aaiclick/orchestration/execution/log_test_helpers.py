"""Test helpers for shell-log capture tests.

``read_logs_via_child``: for mp-module tests (``orch_ctx_no_ch``) the parent
holds no chdb session, so reading back what a worker/flush child wrote requires
another child that opens its own ``orch_context``.

``flush_recorder``: a fake ``flush_shell_logs`` plus the dict it records into,
for vehicle tests that monkeypatch the flush.
"""

from __future__ import annotations

import asyncio
import multiprocessing
from collections.abc import Awaitable, Callable

from ..logging import read_task_logs

_mp_ctx = multiprocessing.get_context("spawn")


def _read_logs_child_target(task_id: int, run_id: int, queue: multiprocessing.Queue) -> None:
    from ..orch_context import orch_context  # Circular dep: orch_context imports the execution package at top level.

    async def _run() -> None:
        async with orch_context():
            lines = await read_task_logs(task_id, run_id)
            queue.put([line.text for line in lines])

    asyncio.run(_run())


def read_logs_via_child(task_id: int, run_id: int) -> list[str]:
    queue = _mp_ctx.Queue()
    proc = _mp_ctx.Process(target=_read_logs_child_target, args=(task_id, run_id, queue), daemon=True)
    proc.start()
    texts = queue.get(timeout=60)
    proc.join()
    return texts


def flush_recorder() -> tuple[dict, Callable[[int, int, int, str], Awaitable[None]]]:
    """Return ``(flushed, fake_flush)`` — patch ``flush_shell_logs`` with the
    latter and assert on the former."""
    flushed: dict = {}

    async def fake_flush(task_id: int, job_id: int, run_id: int, text: str) -> None:
        flushed.update(task_id=task_id, job_id=job_id, run_id=run_id, text=text)

    return flushed, fake_flush
