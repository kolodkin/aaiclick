"""Orchestration context manager for database connections."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine


# Global list holding the engine (avoids global keyword)
_engine: list[Optional[AsyncEngine]] = [None]


def get_orch_context_engine() -> AsyncEngine:
    """Get or create the async SQLAlchemy engine for orchestration.

    Returns:
        AsyncEngine for orchestration database

    Example:
        engine = get_orch_context_engine()
    """
    if _engine[0] is None:
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        user = os.getenv("POSTGRES_USER", "aaiclick")
        password = os.getenv("POSTGRES_PASSWORD", "secret")
        database = os.getenv("POSTGRES_DB", "aaiclick")

        database_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"
        _engine[0] = create_async_engine(database_url, echo=False)

    return _engine[0]


@asynccontextmanager
async def get_orch_context_session() -> AsyncIterator[AsyncSession]:
    """Context manager for orchestration database sessions.

    Yields:
        AsyncSession configured with expire_on_commit=False

    Example:
        async with get_orch_context_session() as session:
            session.add(job)
            session.add(task)
            await session.commit()
    """
    engine = get_orch_context_engine()
    async with AsyncSession(engine, expire_on_commit=False) as session:
        yield session


async def reset_orch_context_engine():
    """Reset the orchestration async SQLAlchemy engine.

    Disposes the existing engine and sets it to None, forcing
    a new engine to be created on next get_orch_context_engine() call.

    Used primarily for test cleanup to ensure test isolation.
    """
    if _engine[0] is not None:
        await _engine[0].dispose()
        _engine[0] = None
