"""
aaiclick.data.lifecycle - Abstract lifecycle handler and local implementation.

This module defines the LifecycleHandler interface for Object table lifecycle
management (incref/decref) and the LocalLifecycleHandler that wraps TableWorker
for single-process local operation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from .models import ClickHouseCreds
from .table_worker import TableWorker


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

    def pin(self, table_name: str) -> None:
        """Mark table as result that survives stop(). Default: no-op."""

    async def claim(self, table_name: str, job_id: int) -> None:
        """Release a job-scoped pinned ref (ownership transfer to consumer).

        Only meaningful for distributed lifecycle handlers that track refs
        in an external store (e.g. PostgreSQL). Default: no-op.
        """

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
        return False


class LocalLifecycleHandler(LifecycleHandler):
    """Local lifecycle via background TableWorker thread.

    Wraps the existing TableWorker — background thread with queue-based
    refcounting and automatic DROP TABLE when refcount reaches 0.
    This is the default when no lifecycle handler is injected.
    """

    def __init__(self, creds: ClickHouseCreds):
        self._worker = TableWorker(creds)

    async def start(self) -> None:
        self._worker.start()

    async def stop(self) -> None:
        self._worker.stop()

    def incref(self, table_name: str) -> None:
        self._worker.incref(table_name)

    def decref(self, table_name: str) -> None:
        self._worker.decref(table_name)
