"""SQL connection context for orchestration.

Provides the SQL engine ContextVar and session accessor for the orchestration layer,
analogous to ch_client.py for ClickHouse.

The engine is set by orch_context() and accessed via get_sql_session().
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

_sql_engine_var: ContextVar[AsyncEngine | None] = ContextVar('sql_engine', default=None)


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
