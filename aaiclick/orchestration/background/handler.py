"""Background handler protocol and factory dispatch.

Concrete implementations live in sqlite_handler.py and pg_handler.py.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import NamedTuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from aaiclick.backend import is_sqlite


def in_clause(ids: list, prefix: str) -> tuple[str, dict]:
    """Build a parameterized IN clause compatible with both SQLite and PostgreSQL.

    Returns (placeholder_string, params_dict) e.g. (":p0, :p1", {"p0": 1, "p1": 2}).
    """
    params = {f"{prefix}{i}": v for i, v in enumerate(ids)}
    placeholders = ", ".join(f":{k}" for k in params)
    return placeholders, params


class PendingCleanupTask(NamedTuple):
    """Row returned by get_pending_cleanup_tasks."""

    task_id: int
    job_id: int
    worker_id: int
    error: str
    run_ids: list
    attempt: int
    max_retries: int


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
    ) -> list[PendingCleanupTask]:
        """Return tasks in PENDING_CLEANUP status."""
        ...

    @staticmethod
    async def transition_pending_cleanup(
        session: AsyncSession,
        task_id: int,
        *,
        has_retries: bool,
        attempt: int,
        retry_after: datetime,
    ) -> None:
        """Transition a PENDING_CLEANUP task to PENDING or FAILED."""
        if has_retries:
            await session.execute(
                text(
                    "UPDATE tasks SET status = 'PENDING', "
                    "attempt = :attempt, retry_after = :retry_after, "
                    "worker_id = NULL, claimed_at = NULL, "
                    "started_at = NULL, completed_at = NULL "
                    "WHERE id = :task_id"
                ),
                {"task_id": task_id, "attempt": attempt, "retry_after": retry_after},
            )
        else:
            await session.execute(
                text(
                    "UPDATE tasks SET status = 'FAILED', "
                    "completed_at = :now "
                    "WHERE id = :task_id"
                ),
                {"task_id": task_id, "now": datetime.utcnow()},
            )


def create_background_handler() -> BackgroundHandler:
    """Create the appropriate handler based on AAICLICK_SQL_URL."""
    if is_sqlite():
        from .sqlite_handler import SqliteBackgroundHandler

        return SqliteBackgroundHandler()

    from .pg_handler import PgBackgroundHandler

    return PgBackgroundHandler()
