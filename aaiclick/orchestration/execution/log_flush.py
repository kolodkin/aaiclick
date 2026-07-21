"""Flush shell-task logs to ClickHouse from a spawned child process.

The worker parent runs ``orch_context(with_ch=False)`` — it must never hold
the chdb session the task children need (see ``docs/designs/testing.md``,
"chdb Session Constraint"). Shell tasks produce their output on the host side
(host pipe, ``docker logs``, ``kubectl logs``), so the CH write happens in a
short-lived spawned child that opens its own context — the same isolation
pattern as ``mp_worker``'s task child. One code path for local (chdb) and
distributed (remote CH) modes.
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing

from aaiclick.data.data_context import get_ch_client
from aaiclick.oplog.models import init_oplog_tables

from ..logging import flush_task_logs, shell_text_to_lines

logger = logging.getLogger(__name__)

# "spawn" starts a fresh interpreter — no inherited chdb C++ singleton.
_mp_ctx = multiprocessing.get_context("spawn")


def _flush_child_target(task_id: int, job_id: int, run_id: int, text: str) -> None:
    """Sync entry point for the flush child — opens its own orch_context."""
    from ..orch_context import orch_context  # Circular dep: orch_context imports the execution package at top level.

    async def _run() -> None:
        async with orch_context():
            await init_oplog_tables(get_ch_client())
            await flush_task_logs(task_id, job_id, run_id, shell_text_to_lines(text))

    asyncio.run(_run())


async def flush_shell_logs(task_id: int, job_id: int, run_id: int, text: str) -> None:
    """Best-effort write of a shell task's captured output to CH ``task_logs``.

    Same contract as ``flush_task_logs``: a failed write must not fail the
    task, so errors are logged and swallowed.
    """
    if not text:
        return
    proc = _mp_ctx.Process(target=_flush_child_target, args=(task_id, job_id, run_id, text), daemon=True)
    proc.start()
    await asyncio.to_thread(proc.join)
    if proc.exitcode != 0:
        logger.error("Shell log flush child exited %s for task %s run %s", proc.exitcode, task_id, run_id)
