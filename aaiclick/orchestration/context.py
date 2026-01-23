"""Orchestration context manager for database connections."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncIterator, Optional

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine


# Global ContextVar to hold the current OrchContext instance
_current_orch_context: ContextVar['OrchContext'] = ContextVar('current_orch_context')


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
            engine = ctx.engine
    """
    try:
        return _current_orch_context.get()
    except LookupError:
        raise RuntimeError("No active OrchContext - must be called within 'async with OrchContext()'")


class OrchContext:
    """
    OrchContext manager for orchestration database connections.

    This context manager:
    - Manages a SQLAlchemy AsyncEngine instance (automatically initialized on enter)
    - Sets itself in ContextVar for global access via get_orch_context()
    - Automatically disposes the engine on exit

    Example:
        >>> async with OrchContext() as ctx:
        ...     job = await create_job("my_job", "mymodule.task1")
        ...     # Use job...
        ... # Engine is automatically disposed here
    """

    def __init__(self):
        """Initialize OrchContext."""
        self._engine: Optional[AsyncEngine] = None
        self._token = None

    @property
    def engine(self) -> AsyncEngine:
        """
        Get the async SQLAlchemy engine for this context.

        Returns:
            AsyncEngine: The SQLAlchemy engine (initialized in __aenter__)

        Raises:
            RuntimeError: If accessed outside of context manager
        """
        if self._engine is None:
            raise RuntimeError(
                "OrchContext engine not initialized. Use 'async with OrchContext()' to enter context."
            )
        return self._engine

    async def __aenter__(self):
        """Enter the context, initializing the engine and setting ContextVar."""
        if self._engine is None:
            host = os.getenv("POSTGRES_HOST", "localhost")
            port = os.getenv("POSTGRES_PORT", "5432")
            user = os.getenv("POSTGRES_USER", "aaiclick")
            password = os.getenv("POSTGRES_PASSWORD", "secret")
            database = os.getenv("POSTGRES_DB", "aaiclick")

            database_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"
            self._engine = create_async_engine(database_url, echo=False)

        self._token = _current_orch_context.set(self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context, disposing engine and resetting ContextVar.
        """
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None

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
