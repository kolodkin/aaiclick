"""
aaiclick.data.lifecycle - Abstract lifecycle handler and local implementation.

This module defines the LifecycleHandler interface for Object table lifecycle
management (incref/decref) and the LocalLifecycleHandler that wraps AsyncTableWorker
for single-process local operation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from contextvars import ContextVar
from typing import NamedTuple

from .ch_client import ChClient
from .table_worker import AsyncTableWorker

_lifecycle_var: ContextVar[LifecycleHandler | None] = ContextVar("lifecycle", default=None)


class TrackedTable(NamedTuple):
    """Per-table state recorded by :class:`LocalLifecycleHandler`.

    ``preserved`` tables survive past ``task_scope`` exit (BackgroundWorker
    drops them at job completion). ``pinned`` tables survive because a
    downstream consumer task still needs them.
    """

    name: str
    preserved: bool
    pinned: bool


def get_data_lifecycle() -> LifecycleHandler | None:
    """Return the active LifecycleHandler, or None if no lifecycle is set."""
    return _lifecycle_var.get()


def register_table(table_name: str, schema_doc: str | None = None) -> None:
    """Register a newly created table via the active lifecycle handler.

    Writes ``schema_doc`` (a Pydantic-serialised ``SchemaView`` JSON) into
    SQL ``table_registry`` so ``open_object()`` can rehydrate the table's
    schema later. Distinct from oplog: ``operation_log`` records what
    operations ran; ``table_registry`` records what tables exist.
    """
    lc = _lifecycle_var.get()
    if lc is not None:
        lc.register_table(table_name, schema_doc=schema_doc)


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

    def unpin(self, table_name: str) -> None:
        """Remove this task's pin for a table. Default: no-op."""

    def oplog_record(
        self, result_table: str, operation: str, kwargs: dict[str, str] | None = None, sql: str | None = None
    ) -> None:
        """Record an oplog entry. No-op in local mode."""

    def oplog_record_sample(
        self, result_table: str, operation: str, kwargs: dict[str, str] | None = None, sql: str | None = None
    ) -> None:
        """Record an oplog entry with lineage sampling. No-op in local mode."""

    def register_table(self, table_name: str, schema_doc: str | None = None) -> None:
        """Insert a row into ``table_registry`` (SQL) for the new table.

        ``schema_doc`` is the Pydantic-serialised ``SchemaView`` JSON read
        back by ``_get_table_schema``. No-op in local mode — only the orch
        lifecycle handler writes to ``table_registry``.
        """

    def current_job_id(self) -> int | None:
        """Return the job ID owning this handler, or ``None`` outside orch.

        Used by ``create_object_from_value(scope="job")`` to build the
        ``j_<job_id>_<name>`` table name.
        """
        return None

    def track_table(self, table_name: str, *, preserved: bool = False) -> None:
        """Record that this handler's lifetime owns ``table_name``. Default no-op."""

    def mark_pinned(self, table_name: str) -> None:
        """Flag a tracked table as pinned (consumer-bound). Default no-op."""

    def iter_tracked_tables(self) -> Iterable[TrackedTable]:
        """Yield :class:`TrackedTable` entries. Default empty."""
        return iter(())

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
        self._tracked: dict[str, TrackedTable] = {}

    async def start(self) -> None:
        await self._worker.start()

    async def stop(self) -> None:
        await self._worker.stop()

    def incref(self, table_name: str) -> None:
        self._worker.incref(table_name)
        self.track_table(table_name)

    def decref(self, table_name: str) -> None:
        self._worker.decref(table_name)

    async def flush(self) -> None:
        await self._worker.flush()

    async def claim(self, table_name: str, job_id: int) -> None:
        pass  # No distributed refs to release in local mode

    def track_table(self, table_name: str, *, preserved: bool = False) -> None:
        existing = self._tracked.get(table_name)
        if existing is None:
            self._tracked[table_name] = TrackedTable(table_name, preserved, False)
        elif preserved and not existing.preserved:
            self._tracked[table_name] = existing._replace(preserved=True)

    def mark_pinned(self, table_name: str) -> None:
        existing = self._tracked.get(table_name)
        if existing is not None and not existing.pinned:
            self._tracked[table_name] = existing._replace(pinned=True)

    def iter_tracked_tables(self) -> Iterable[TrackedTable]:
        return iter(list(self._tracked.values()))
