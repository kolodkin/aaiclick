"""DB handler protocol, shared SQL, and factory dispatch.

Concrete implementations live in pg_handler.py and sqlite_handler.py.

Module-level ContextVars:
- _sql_engine_var: active AsyncEngine, set by orch_context()
- _db_handler_var: active DbHandler, set by orch_context()

Access via get_sql_session() and get_db_handler().
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime
from typing import AsyncIterator, Optional, Union

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.sql import Select

from ..backend import is_sqlite
from .models import Task

_sql_engine_var: ContextVar[AsyncEngine | None] = ContextVar('sql_engine', default=None)
_db_handler_var: ContextVar[DbHandler | None] = ContextVar('db_handler', default=None)


def get_db_handler() -> DbHandler:
    """Return the DbHandler for the active orchestration context."""
    handler = _db_handler_var.get()
    if handler is None:
        raise RuntimeError(
            "No active orch_context — use 'async with orch_context()'"
        )
    return handler


@asynccontextmanager
async def get_sql_session() -> AsyncIterator[AsyncSession]:
    """Yield an AsyncSession from the active orchestration context engine."""
    engine = _sql_engine_var.get()
    if engine is None:
        raise RuntimeError(
            "No active orch_context — use 'async with orch_context()'"
        )
    async with AsyncSession(engine, expire_on_commit=False) as session:
        yield session

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
    async def claim_next_task(
        session: AsyncSession, worker_id: int, now: datetime
    ) -> Optional[Task]: ...

    @staticmethod
    @abstractmethod
    def lock_query(query: Select) -> Select: ...


def create_db_handler() -> Union["PgDbHandler", "SqliteDbHandler"]:
    """Create the appropriate DB handler based on AAICLICK_SQL_URL."""
    if is_sqlite():
        from .sqlite_handler import SqliteDbHandler

        return SqliteDbHandler()

    from .pg_handler import PgDbHandler

    return PgDbHandler()
