"""
aaiclick.orch_context - OrchContext manager for orchestration operations.

This module provides a context manager for orchestration backend operations,
managing PostgreSQL connections for job and task management.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Optional

from aaiclick.orchestration.database import get_postgres_pool

# Global ContextVar to hold the current OrchContext instance
_current_orch_context: ContextVar[Optional[OrchContext]] = ContextVar("orch_context", default=None)


def get_orch_context() -> OrchContext:
    """
    Get the current OrchContext instance from ContextVar.

    Returns:
        OrchContext: The active OrchContext instance

    Raises:
        RuntimeError: If no active context (must be called within 'async with OrchContext(job_id)')
    """
    ctx = _current_orch_context.get()
    if ctx is None:
        raise RuntimeError("No active OrchContext - must be called within 'async with OrchContext(job_id)'")
    return ctx


class OrchContext:
    """
    OrchContext manager for orchestration operations.

    This context manager:
    - Manages PostgreSQL connection pool access for orchestration
    - Provides methods for job and task operations
    - Uses global asyncpg.Pool (shared across all OrchContext instances)
    - Each operation acquires its own connection/session from pool

    Args:
        job_id: Job ID for this context (required)

    Example:
        >>> async with OrchContext(job_id=123) as ctx:
        ...     # Orchestration operations here
        ...     await ctx.apply(task)
    """

    def __init__(self, job_id: int):
        """
        Initialize an OrchContext.

        Args:
            job_id: Job ID for this context (required)
        """
        self.job_id = job_id
        self._pool = None
        self._token = None

    async def __aenter__(self):
        """Enter the context, initializing pool reference and setting ContextVar."""
        # Get reference to global pool
        self._pool = await get_postgres_pool()
        # Set this context as current
        self._token = _current_orch_context.set(self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the context, resetting ContextVar."""
        # Reset the ContextVar
        if self._token is not None:
            _current_orch_context.reset(self._token)
        return False

    async def apply(self, *args, **kwargs):
        """
        Apply (commit) tasks or groups to the database.

        To be implemented in Phase 4.

        Args:
            *args: Tasks, Groups, or lists to commit
            **kwargs: Additional options
        """
        raise NotImplementedError("OrchContext.apply() will be implemented in Phase 4")
