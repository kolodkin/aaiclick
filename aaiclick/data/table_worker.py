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


def _create_sync_client(connection_string: str) -> object:
    """Create a sync ClickHouse client from a connection string.

    Supports:
    - chdb:///path/to/data → ChdbSyncClient
    - clickhouse://user:pass@host:port/db → clickhouse-connect sync client
    """
    if connection_string.startswith("chdb://"):
        from .chdb_client import ChdbSyncClient, create_chdb_session

        path = connection_string[len("chdb://"):]
        session = create_chdb_session(path)
        return ChdbSyncClient(session)

    from urllib.parse import urlparse

    from clickhouse_connect import get_client

    parsed = urlparse(connection_string)
    return get_client(
        host=parsed.hostname or "localhost",
        port=parsed.port or 8123,
        username=parsed.username or "default",
        password=parsed.password or "",
        database=parsed.path.lstrip("/") or "default",
    )


class TableWorker:
    """Background worker that manages table lifecycle via refcounting.

    Creates its own sync client from a connection string.
    """

    def __init__(self, connection_string: str):
        """Initialize worker with a connection string.

        Args:
            connection_string: ClickHouse or chdb connection URL.
        """
        self._connection_string = connection_string
        self._ch_client: object = None
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
        self._ch_client = _create_sync_client(self._connection_string)

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
                            if not msg.table_name.startswith("p_"):
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
        """Drop all remaining tables on shutdown. Skips persistent (p_) tables."""
        for table_name in list(self._refcounts.keys()):
            if not table_name.startswith("p_"):
                self._drop_table(table_name)
        self._refcounts.clear()
