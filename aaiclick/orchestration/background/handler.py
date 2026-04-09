"""Background handler protocol and factory dispatch.

Concrete implementations live in sqlite_handler.py and pg_handler.py.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from aaiclick.backend import is_sqlite


def extract_last_run_ids(rows: list) -> list[int]:
    """Extract the last run_id from each task's run_ids JSON array.

    Used by get_dead_worker_run_ids implementations to parse the
    tasks.run_ids JSON column (list of ints) and return the last
    element of each (the in-progress run that was interrupted).
    """
    result: list[int] = []
    for (run_ids_json,) in rows:
        ids = run_ids_json if isinstance(run_ids_json, list) else json.loads(run_ids_json or "[]")
        if ids:
            result.append(ids[-1])
    return result


class BackgroundHandler(ABC):
    """Abstract base for backend-specific background cleanup SQL."""

    @staticmethod
    @abstractmethod
    async def mark_dead_workers(
        session: AsyncSession, dead_worker_ids: list[int], now: datetime,
    ) -> None:
        """Mark dead workers as STOPPED and their tasks as PENDING_CLEANUP."""
        ...

    @staticmethod
    @abstractmethod
    async def get_dead_worker_run_ids(
        session: AsyncSession, dead_worker_ids: list[int],
    ) -> list[int]:
        """Return last run_id of each RUNNING/CLAIMED task on dead workers."""
        ...

    @staticmethod
    async def clean_task_run(session: AsyncSession, run_id: str) -> None:
        """Delete all table_run_refs rows for a given run_id (crash recovery)."""
        await session.execute(
            text("DELETE FROM table_run_refs WHERE run_id = :run_id"),
            {"run_id": run_id},
        )

    @staticmethod
    async def clean_task_pins(session: AsyncSession, task_id: int) -> None:
        """Delete all table_pin_refs rows for a given task_id.

        Cleans pin refs that upstream producers created for this task as
        a downstream consumer.  Called during PENDING_CLEANUP processing
        so stale pins don't block table cleanup.
        """
        await session.execute(
            text("DELETE FROM table_pin_refs WHERE task_id = :task_id"),
            {"task_id": task_id},
        )

    @staticmethod
    @abstractmethod
    async def clean_task_runs(session: AsyncSession, run_ids: list[str]) -> None:
        """Batch-delete table_run_refs rows for multiple run_ids."""
        ...

    @staticmethod
    @abstractmethod
    async def get_pending_cleanup_tasks(
        session: AsyncSession,
    ) -> list[tuple[int, int, int, str, list, int]]:
        """Return tasks in PENDING_CLEANUP status.

        Returns list of (task_id, job_id, worker_id, error, run_ids, attempt, max_retries)
        tuples for processing by the background worker.
        """
        ...

    @staticmethod
    @abstractmethod
    async def transition_pending_cleanup(
        session: AsyncSession,
        task_id: int,
        *,
        has_retries: bool,
        attempt: int,
        retry_after: datetime,
    ) -> None:
        """Transition a PENDING_CLEANUP task to PENDING or FAILED."""
        ...


def create_background_handler() -> BackgroundHandler:
    """Create the appropriate handler based on AAICLICK_SQL_URL."""
    if is_sqlite():
        from .sqlite_handler import SqliteBackgroundHandler

        return SqliteBackgroundHandler()

    from .pg_handler import PgBackgroundHandler

    return PgBackgroundHandler()
