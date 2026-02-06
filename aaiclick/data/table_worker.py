"""
aaiclick.data.table_worker - Background worker for table lifecycle management.

This module provides a background thread that manages table reference counting
and automatic cleanup when tables are no longer referenced.
"""

from __future__ import annotations

import queue
import threading
from dataclasses import dataclass
from enum import Enum, auto

from clickhouse_connect import get_client

from .models import ClickHouseCreds


class TableOp(Enum):
    """Operations for the table worker."""

    INCREF = auto()
    DECREF = auto()
    SHUTDOWN = auto()


@dataclass
class TableMessage:
    """Message passed to worker via queue."""

    op: TableOp
    table_name: str


class TableWorker:
    """Background worker that manages table lifecycle via refcounting."""

    def __init__(self, creds: ClickHouseCreds):
        """Initialize worker with ClickHouse credentials."""
        self._creds = creds
        self._ch_client = None  # Created in thread
        self._queue: queue.Queue[TableMessage] = queue.Queue()
        self._refcounts: dict[str, int] = {}
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        """Start the worker thread."""
        self._thread.start()

    def stop(self) -> None:
        """Stop worker and wait for completion. Blocks until done."""
        self._queue.put(TableMessage(TableOp.SHUTDOWN, ""))
        self._thread.join()

    def incref(self, table_name: str) -> None:
        """Increment reference count for table. Non-blocking."""
        self._queue.put(TableMessage(TableOp.INCREF, table_name))

    def decref(self, table_name: str) -> None:
        """Decrement reference count for table. Non-blocking."""
        self._queue.put(TableMessage(TableOp.DECREF, table_name))

    def _run(self) -> None:
        """Worker loop - runs in background thread."""
        # Create sync client in worker thread
        self._ch_client = get_client(
            host=self._creds.host,
            port=self._creds.port,
            username=self._creds.user,
            password=self._creds.password,
            database=self._creds.database,
        )

        try:
            while True:
                msg = self._queue.get()

                if msg.op == TableOp.SHUTDOWN:
                    self._cleanup_all()
                    break

                elif msg.op == TableOp.INCREF:
                    self._refcounts[msg.table_name] = (
                        self._refcounts.get(msg.table_name, 0) + 1
                    )

                elif msg.op == TableOp.DECREF:
                    if msg.table_name in self._refcounts:
                        self._refcounts[msg.table_name] -= 1
                        if self._refcounts[msg.table_name] <= 0:
                            self._drop_table(msg.table_name)
                            del self._refcounts[msg.table_name]
        finally:
            if self._ch_client:
                self._ch_client.close()

    def _drop_table(self, table_name: str) -> None:
        """Drop single table. Best effort."""
        try:
            self._ch_client.command(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass  # Best effort - table may already be gone

    def _cleanup_all(self) -> None:
        """Drop all remaining tables on shutdown."""
        for table_name in list(self._refcounts.keys()):
            self._drop_table(table_name)
        self._refcounts.clear()
