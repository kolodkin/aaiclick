"""DB handler protocol, shared SQL, and factory dispatch.

Concrete implementations live in pg_handler.py and sqlite_handler.py.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from ..backend import is_sqlite
from .models import Task

# Shared dependency check SQL — identical for both backends
DEPENDENCY_WHERE = """
    AND NOT EXISTS (
        SELECT 1 FROM dependencies d
        JOIN tasks prev ON d.previous_id = prev.id
        WHERE d.next_id = t.id
        AND d.next_type = 'task'
        AND d.previous_type = 'task'
        AND prev.status != :completed_status
    )
    AND NOT EXISTS (
        SELECT 1 FROM dependencies d
        JOIN tasks prev ON prev.group_id = d.previous_id
        WHERE d.next_id = t.id
        AND d.next_type = 'task'
        AND d.previous_type = 'group'
        AND prev.status != :completed_status
    )
    AND NOT EXISTS (
        SELECT 1 FROM dependencies d
        JOIN tasks prev ON d.previous_id = prev.id
        WHERE d.next_id = t.group_id
        AND d.next_type = 'group'
        AND d.previous_type = 'task'
        AND prev.status != :completed_status
        AND t.group_id IS NOT NULL
    )
    AND NOT EXISTS (
        SELECT 1 FROM dependencies d
        JOIN tasks prev ON prev.group_id = d.previous_id
        WHERE d.next_id = t.group_id
        AND d.next_type = 'group'
        AND d.previous_type = 'group'
        AND prev.status != :completed_status
        AND t.group_id IS NOT NULL
    )
"""


class DbHandler(ABC):
    """Abstract base class for backend-specific SQL operations."""

    @staticmethod
    @abstractmethod
    async def claim_next_task(session: AsyncSession, worker_id: int, now: datetime) -> Task | None: ...

    @staticmethod
    @abstractmethod
    def lock_query(query: Select) -> Select: ...


def create_db_handler() -> PgDbHandler | SqliteDbHandler:  # noqa: F821
    """Create the appropriate DB handler based on AAICLICK_SQL_URL."""
    if is_sqlite():
        from .sqlite_handler import SqliteDbHandler

        return SqliteDbHandler()

    from .pg_handler import PgDbHandler

    return PgDbHandler()
