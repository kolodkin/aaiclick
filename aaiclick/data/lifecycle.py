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
    """Abstract interface for Object table lifecycle management."""

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
