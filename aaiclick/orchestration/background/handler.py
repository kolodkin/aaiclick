"""Background handler protocol and factory dispatch.

Concrete implementations live in sqlite_handler.py and pg_handler.py.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from aaiclick.backend import is_sqlite


class BackgroundHandler(ABC):
    """Abstract base for backend-specific background cleanup SQL."""

    @staticmethod
    @abstractmethod
    async def clear_job_pins(session: AsyncSession, job_ids: list[int]) -> None:
        """Clear job_id on pin refs for completed job IDs."""
        ...

    @staticmethod
    @abstractmethod
    async def mark_dead_workers(
        session: AsyncSession, dead_worker_ids: list[int], now: datetime,
    ) -> None:
        """Mark dead workers as STOPPED and their tasks as FAILED."""
        ...

    @staticmethod
    @abstractmethod
    async def get_dead_worker_run_ids(
        session: AsyncSession, dead_worker_ids: list[int],
    ) -> list[int]:
        """Return last run_id of each RUNNING/CLAIMED task on dead workers."""
        ...

    @staticmethod
    @abstractmethod
    async def clean_task_run(session: AsyncSession, run_id: str) -> None:
        """Remove run_id from all run_ids arrays in table_context_refs."""
        ...


def create_background_handler() -> BackgroundHandler:
    """Create the appropriate handler based on AAICLICK_SQL_URL."""
    if is_sqlite():
        from .sqlite_handler import SqliteBackgroundHandler

        return SqliteBackgroundHandler()

    from .pg_handler import PgBackgroundHandler

    return PgBackgroundHandler()
