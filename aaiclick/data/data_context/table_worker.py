"""
aaiclick.data.table_worker - Async worker for table lifecycle management.

This module provides an async task that manages table reference counting
and automatic cleanup when tables are no longer referenced.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum, auto

from ..scope import is_persistent_table
from .ch_client import ChClient


class TableOp(Enum):
    """Operations for the table worker."""

    INCREF = auto()
    DECREF = auto()
    FLUSH = auto()
    SHUTDOWN = auto()


@dataclass
class TableMessage:
    """Message passed to worker via queue."""

    op: TableOp
    table_name: str
    event: asyncio.Event | None = None


class AsyncTableWorker:
    """Async worker that manages table lifecycle via refcounting.

    Runs as an asyncio Task in the main event loop. Uses the shared async
    ChClient for DROP TABLE operations — no background thread or sync client
    needed.

    incref/decref are sync (safe to call from __del__) and schedule work
    onto the event loop via call_soon_threadsafe.
    """

    def __init__(self, ch_client: ChClient):
        self._ch_client = ch_client
        self._queue: asyncio.Queue[TableMessage] = asyncio.Queue()
        self._task: asyncio.Task | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._refcounts: dict[str, int] = {}

    async def start(self) -> None:
        """Start the worker task in the running event loop."""
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Send SHUTDOWN and wait for the worker task to finish.

        SHUTDOWN is scheduled via call_soon_threadsafe to guarantee ordering
        after any pending incref/decref messages that were also scheduled via
        call_soon_threadsafe.
        """
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, TableMessage(TableOp.SHUTDOWN, ""))
        if self._task is not None:
            await self._task

    def incref(self, table_name: str) -> None:
        """Increment reference count for table. Non-blocking, safe from any thread."""
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, TableMessage(TableOp.INCREF, table_name))

    def decref(self, table_name: str) -> None:
        """Decrement reference count for table. Non-blocking, safe from any thread."""
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, TableMessage(TableOp.DECREF, table_name))

    async def flush(self) -> None:
        """Wait until all pending incref/decref messages have been processed.

        Enqueues a FLUSH sentinel and awaits its event. Since the worker
        processes the queue in order, the event fires only after every
        message that was already in the queue has been handled — meaning
        all pending DROP TABLEs have completed.
        """
        if self._loop is None:
            return
        event = asyncio.Event()
        self._loop.call_soon_threadsafe(self._queue.put_nowait, TableMessage(TableOp.FLUSH, "", event))
        await event.wait()

    async def _run(self) -> None:
        """Worker loop — runs as an asyncio Task."""
        while True:
            msg = await self._queue.get()

            if msg.op == TableOp.SHUTDOWN:
                await self._cleanup_all()
                break

            elif msg.op == TableOp.FLUSH:
                if msg.event is not None:
                    msg.event.set()
                continue

            elif msg.op == TableOp.INCREF:
                self._refcounts[msg.table_name] = self._refcounts.get(msg.table_name, 0) + 1

            elif msg.op == TableOp.DECREF:
                if msg.table_name in self._refcounts:
                    self._refcounts[msg.table_name] -= 1
                    if self._refcounts[msg.table_name] <= 0:
                        if not is_persistent_table(msg.table_name):
                            await self._drop_table(msg.table_name)
                        del self._refcounts[msg.table_name]

    async def _drop_table(self, table_name: str) -> None:
        """Drop single table. Best effort."""
        try:
            await self._ch_client.command(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass  # Best effort - table may already be gone

    async def _cleanup_all(self) -> None:
        """Drop remaining non-persistent tables on shutdown.

        Skips ``p_*`` (user-managed) and ``j_<id>_*`` (job-scoped) tables,
        which outlive the local process.
        """
        for table_name in list(self._refcounts.keys()):
            if not is_persistent_table(table_name):
                await self._drop_table(table_name)
        self._refcounts.clear()
