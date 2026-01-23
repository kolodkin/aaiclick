"""Orchestration context manager for database connections."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncIterator, Optional

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine


# Global ContextVar to hold the current OrchContext instance
_current_orch_context: ContextVar['OrchContext'] = ContextVar('current_orch_context')

# Global async engine (SQLAlchemy manages connection pooling internally)
_engine: list[Optional[AsyncEngine]] = [None]


def get_orch_context() -> 'OrchContext':
    """
    Get the current OrchContext instance from ContextVar.

    Returns:
        OrchContext: The active OrchContext instance

    Raises:
        RuntimeError: If no active context (must be called within 'async with OrchContext()')

    Example:
        async with OrchContext():
            ctx = get_orch_context()
            # Use ctx...
    """
    try:
        return _current_orch_context.get()
    except LookupError:
        raise RuntimeError("No active OrchContext - must be called within 'async with OrchContext()'")


def _get_engine() -> AsyncEngine:
    """Get or create the global async SQLAlchemy engine (private).

    Engine is initialized on first call using environment variables:
    - POSTGRES_HOST (default: "localhost")
    - POSTGRES_PORT (default: 5432)
    - POSTGRES_USER (default: "aaiclick")
    - POSTGRES_PASSWORD (default: "secret")
    - POSTGRES_DB (default: "aaiclick")

    SQLAlchemy manages connection pooling internally via asyncpg.

    Returns:
        AsyncEngine for orchestration database
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


async def _reset_engine():
    """Reset the global async SQLAlchemy engine (private).

    Disposes the existing engine and sets it to None, forcing
    a new engine to be created on next call.

    Required for test isolation because asyncpg connections in the pool
    retain protocol state (prepared statements, transactions, Futures)
    beyond what SQLAlchemy's pool_reset_on_return can clean. This ensures
    each test gets fresh connections with clean asyncpg protocol state.
    """
    if _engine[0] is not None:
        await _engine[0].dispose()
        _engine[0] = None


class OrchContext:
    """
    OrchContext manager for orchestration database access.

    This context manager:
    - Provides access to the global SQLAlchemy AsyncEngine
    - Sets itself in ContextVar for global access via get_orch_context()
    - SQLAlchemy manages connection pooling internally

    Example:
        >>> async with OrchContext() as ctx:
        ...     job = await create_job("my_job", "mymodule.task1")
        ...     # Use job...
    """

    def __init__(self):
        """Initialize OrchContext."""
        self._token = None

    @property
    def engine(self) -> AsyncEngine:
        """
        Get the global async SQLAlchemy engine.

        Returns:
            AsyncEngine: The SQLAlchemy engine (global, shared across contexts)

        Example:
            async with OrchContext() as ctx:
                engine = ctx.engine
        """
        return _get_engine()

    async def __aenter__(self):
        """Enter the context, initializing the engine and setting ContextVar."""
        _get_engine()  # Ensure engine is initialized
        self._token = _current_orch_context.set(self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the context, resetting ContextVar."""
        _current_orch_context.reset(self._token)
        return False

    @asynccontextmanager
    async def get_session(self) -> AsyncIterator[AsyncSession]:
        """
        Context manager for orchestration database sessions.

        Yields:
            AsyncSession configured with expire_on_commit=False

        Example:
            async with ctx.get_session() as session:
                session.add(job)
                session.add(task)
                await session.commit()
        """
        async with AsyncSession(self.engine, expire_on_commit=False) as session:
            yield session


@asynccontextmanager
async def get_orch_context_session() -> AsyncIterator[AsyncSession]:
    """
    Convenience function to get a session from the current OrchContext.

    Yields:
        AsyncSession configured with expire_on_commit=False

    Raises:
        RuntimeError: If no active OrchContext

    Example:
        async with OrchContext():
            async with get_orch_context_session() as session:
                session.add(job)
                session.add(task)
                await session.commit()
    """
    ctx = get_orch_context()
    async with ctx.get_session() as session:
        yield session
