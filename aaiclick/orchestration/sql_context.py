"""SQL connection context for orchestration.

Provides the SQL engine ContextVar and session accessor for the orchestration layer,
analogous to ch_client.py for ClickHouse.

The engine is set by orch_context() and accessed via get_sql_session().
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

_sql_engine_var: ContextVar[AsyncEngine | None] = ContextVar("sql_engine", default=None)


def _enable_sqlite_foreign_keys(dbapi_connection, _record) -> None:
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def create_sql_engine(url: str) -> AsyncEngine:
    """Async engine for ``url``; on SQLite, turns on ``PRAGMA foreign_keys`` per connection."""
    engine = create_async_engine(url, echo=False)
    if engine.dialect.name == "sqlite":
        event.listen(engine.sync_engine, "connect", _enable_sqlite_foreign_keys)
    return engine


@asynccontextmanager
async def get_sql_session() -> AsyncIterator[AsyncSession]:
    """Yield an AsyncSession from the active orchestration context engine."""
    engine = _sql_engine_var.get()
    if engine is None:
        raise RuntimeError("No active orch_context — use 'async with orch_context()'")
    async with AsyncSession(engine, expire_on_commit=False) as session:
        yield session
