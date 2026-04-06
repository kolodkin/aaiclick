"""
aaiclick.data.lifecycle - Abstract lifecycle handler and local implementation.

This module defines the LifecycleHandler interface for Object table lifecycle
management (incref/decref) and the LocalLifecycleHandler that wraps AsyncTableWorker
for single-process local operation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextvars import ContextVar

from .ch_client import ChClient
from .table_worker import AsyncTableWorker


_lifecycle_var: ContextVar[LifecycleHandler | None] = ContextVar('lifecycle', default=None)


def get_data_lifecycle() -> LifecycleHandler | None:
    """Return the active LifecycleHandler, or None if no lifecycle is set."""
    return _lifecycle_var.get()


class LifecycleHandler(ABC):
    """Abstract interface for Object table lifecycle management.

    Supports async context manager usage::

        async with handler:
            ...  # handler.start() called on enter, handler.stop() on exit
    """

    @abstractmethod
    async def start(self) -> None:
        """Initialize the handler."""

    @abstractmethod
    async def stop(self) -> None:
        """Shutdown and cleanup."""

    @abstractmethod
    def incref(self, table_name: str) -> None:
        """Increment reference count for a table."""

    @abstractmethod
    def decref(self, table_name: str) -> None:
        """Decrement reference count for a table."""

    async def flush(self) -> None:
        """Wait until all pending table drops have completed.

        Blocks until every incref/decref already in the queue has been
        processed.  Useful for releasing memory between benchmark
        iterations or before measuring the next operation.
        """

    def pin(self, table_name: str) -> None:
        """Mark table as result that survives stop(). Default: no-op."""

    async def claim(self, table_name: str, job_id: int) -> None:
        """Release a job-scoped pinned ref (ownership transfer to consumer).

        Only meaningful for distributed lifecycle handlers that track refs
        in an external store (e.g. PostgreSQL). Local handler raises.
        """
        raise NotImplementedError("claim() requires a distributed lifecycle handler")

    def oplog_record(self, result_table: str, operation: str,
                     kwargs: dict[str, str] | None = None,
                     sql: str | None = None) -> None:
        """Record an oplog entry. No-op in local mode."""

    def oplog_record_sample(self, result_table: str, operation: str,
                            kwargs: dict[str, str] | None = None,
                            sql: str | None = None) -> None:
        """Record an oplog entry with lineage sampling. No-op in local mode."""

    def oplog_record_table(self, table_name: str) -> None:
        """Record a table registry entry. No-op in local mode."""

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
        return False


class LocalLifecycleHandler(LifecycleHandler):
    """Local lifecycle via async AsyncTableWorker task.

    Wraps AsyncTableWorker — asyncio Task with queue-based refcounting and
    automatic DROP TABLE when refcount reaches 0. Runs entirely in the main
    event loop; no background thread or sync client needed.

    Args:
        ch_client: Async ClickHouse client to use for DROP TABLE operations.
    """

    def __init__(self, ch_client: ChClient):
        self._worker = AsyncTableWorker(ch_client)

    async def start(self) -> None:
        await self._worker.start()

    async def stop(self) -> None:
        await self._worker.stop()

    def incref(self, table_name: str) -> None:
        self._worker.incref(table_name)

    def decref(self, table_name: str) -> None:
        self._worker.decref(table_name)

    async def flush(self) -> None:
        await self._worker.flush()

    async def claim(self, table_name: str, job_id: int) -> None:
        pass  # No distributed refs to release in local mode
