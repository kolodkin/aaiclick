"""
aaiclick.data.lifecycle - Abstract lifecycle handler and local implementation.

This module defines the LifecycleHandler interface for Object table lifecycle
management (incref/decref) and the LocalLifecycleHandler that wraps AsyncTableWorker
for single-process local operation.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from contextvars import ContextVar
from datetime import datetime

from .ch_client import ChClient
from .table_worker import AsyncTableWorker

logger = logging.getLogger(__name__)

_lifecycle_var: ContextVar[LifecycleHandler | None] = ContextVar("lifecycle", default=None)


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
        back by ``_get_table_schema``. This is **not** an oplog write —
        the oplog (``operation_log`` in ClickHouse) tracks per-operation
        audit; ``table_registry`` is the SQL keyed-lookup that
        ``open_object()`` uses to rehydrate a table's schema.
        """

    def current_job_id(self) -> int | None:
        """Return the job ID owning this handler, or ``None`` outside orch.

        Used by ``create_object_from_value(scope="job")`` to build the
        ``j_<job_id>_<name>`` table name.
        """
        return None

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

    When an SQL engine is bound to the active ``data_context()``, also
    writes ``table_registry.schema_doc`` rows so persistent objects are
    re-openable across context exits via ``open_object()``.

    Args:
        ch_client: Async ClickHouse client to use for DROP TABLE operations.
    """

    def __init__(self, ch_client: ChClient):
        self._worker = AsyncTableWorker(ch_client)
        self._registry_tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        await self._worker.start()

    async def stop(self) -> None:
        await self._await_registry_tasks()
        await self._worker.stop()

    def incref(self, table_name: str) -> None:
        self._worker.incref(table_name)

    def decref(self, table_name: str) -> None:
        self._worker.decref(table_name)

    async def flush(self) -> None:
        await self._worker.flush()
        await self._await_registry_tasks()

    def register_table(self, table_name: str, schema_doc: str | None = None) -> None:
        if schema_doc is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._registry_tasks.append(loop.create_task(self._write_registry_row(table_name, schema_doc)))

    async def _await_registry_tasks(self) -> None:
        if not self._registry_tasks:
            return
        pending, self._registry_tasks = self._registry_tasks, []
        await asyncio.gather(*pending, return_exceptions=True)

    async def _write_registry_row(self, table_name: str, schema_doc: str) -> None:
        from sqlalchemy import text

        from aaiclick.orchestration.sql_context import get_sql_session

        try:
            async with get_sql_session() as session:
                await session.execute(
                    text(
                        "INSERT INTO table_registry "
                        "(table_name, created_at, schema_doc) "
                        "VALUES (:table_name, :created_at, :schema_doc) "
                        "ON CONFLICT (table_name) DO NOTHING"
                    ),
                    {
                        "table_name": table_name,
                        "created_at": datetime.utcnow(),
                        "schema_doc": schema_doc,
                    },
                )
                await session.commit()
        except RuntimeError:
            return
        except Exception:
            logger.error("Failed to write table registry for %s", table_name, exc_info=True)

    async def claim(self, table_name: str, job_id: int) -> None:
        pass  # No distributed refs to release in local mode
