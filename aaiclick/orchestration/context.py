"""Orchestration context manager for database connections."""

from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from ..snowflake_id import get_snowflake_id
from .env import get_pg_url
from .models import Group, Task, TasksType


# Global ContextVar to hold the current OrchContext instance
_current_orch_context: ContextVar[OrchContext] = ContextVar('current_orch_context')


def get_orch_context() -> OrchContext:
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


class OrchContext:
    """
    OrchContext manager for orchestration database access.

    Each context creates its own SQLAlchemy AsyncEngine on enter and disposes
    it on exit, ensuring proper async lifecycle and test isolation.

    Example:
        >>> async with OrchContext() as ctx:
        ...     job = await create_job("my_job", "mymodule.task1")
        ...     # Use job...
    """

    def __init__(self):
        """Initialize OrchContext."""
        self._token = None
        self._engine: AsyncEngine | None = None

    @property
    def engine(self) -> AsyncEngine:
        """
        Get the async SQLAlchemy engine for this context.

        Returns:
            AsyncEngine: The SQLAlchemy engine for this context

        Raises:
            RuntimeError: If accessed outside of context manager

        Example:
            async with OrchContext() as ctx:
                engine = ctx.engine
        """
        if self._engine is None:
            raise RuntimeError("Engine not initialized - must be called within context")
        return self._engine

    async def __aenter__(self):
        """
        Enter the context, creating engine and setting ContextVar.

        Creates a new SQLAlchemy AsyncEngine using environment variables
        configured in orchestration.env.get_pg_url().
        """
        self._engine = create_async_engine(get_pg_url(), echo=False)

        self._token = _current_orch_context.set(self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context, disposing engine and resetting ContextVar.

        Properly disposes the async engine, closing all connections.
        """
        _current_orch_context.reset(self._token)

        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None

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

    async def apply(
        self,
        items: TasksType,
        job_id: int,
    ) -> TasksType:
        """
        Commit tasks, groups, and their dependencies to the database.

        Sets job_id on all items, generates snowflake IDs for Groups
        if not already set, and commits to PostgreSQL. Also commits
        any dependencies created by >> and << operators.

        Args:
            items: Single Task/Group or list of Task/Group objects
            job_id: Job ID to assign to all items

        Returns:
            Same items with database IDs populated

        Example:
            async with OrchContext() as ctx:
                task1 = create_task("mymodule.func1", {"x": 1})
                task2 = create_task("mymodule.func2", {"x": 2})
                task1 >> task2  # task2 depends on task1
                await ctx.apply([task1, task2], job_id=job.id)
        """
        # Normalize to list
        items_list = items if isinstance(items, list) else [items]

        async with self.get_session() as session:
            for item in items_list:
                # Set job_id on all items
                item.job_id = job_id

                # Generate snowflake ID for Groups if not set
                if isinstance(item, Group) and item.id is None:
                    item.id = get_snowflake_id()

                session.add(item)

            await session.commit()

        # Return in same format as input
        if isinstance(items, list):
            return items_list
        return items_list[0]


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
