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

_lifecycle_var: ContextVar[LifecycleHandler | None] = ContextVar("lifecycle", default=None)


def get_data_lifecycle() -> LifecycleHandler | None:
    """Return the active LifecycleHandler, or None if no lifecycle is set."""
    return _lifecycle_var.get()


def register_table(table_name: str, schema_doc: str | None = None) -> None:
    """Register a newly created table via the active lifecycle handler.

    Writes ``schema_doc`` (a Pydantic-serialised ``Schema`` JSON) into
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

        ``schema_doc`` is the Pydantic-serialised ``Schema`` JSON read
        back by ``_get_table_schema``. No-op in local mode — only the orch
        lifecycle handler writes to ``table_registry``.
        """

    def current_job_id(self) -> int | None:
        """Return the job ID owning this handler, or ``None`` outside orch.

        Used by ``create_object_from_value(scope="job")`` to build the
        ``j_<job_id>_<name>`` table name.
        """
        return None

    async def resolve_global_table(self, name: str) -> str | None:
        """Return the CH table registered under ``name`` for the active tenant, or ``None``.

        ``scope="global"`` tables are named ``p_<snowflake>``, so the only
        name → table mapping is ``table_registry.name``. Orch-only: the
        local handler has no registry.
        """
        raise NotImplementedError

    async def claim_global_table(self, name: str, table_name: str, schema_doc: str) -> str:
        """Register ``table_name`` under ``name`` for the active tenant; return the owning table.

        That is ``table_name`` when the claim wins, or the already-registered
        table when the name is taken (an append, or a concurrent creator that
        got there first). This *is* the registry write for a global table —
        callers do not also ``register_table``. Orch-only.
        """
        raise NotImplementedError

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
