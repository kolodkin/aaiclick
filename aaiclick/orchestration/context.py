"""Function-based orchestration context for database connections."""

from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from ..snowflake_id import get_snowflake_id
from .env import get_pg_url
from .models import Group, Task, TasksType


@dataclass
class OrchCtxState:
    """State bundle for a named orchestration context."""

    engine: AsyncEngine


# ContextVar holding dict[name -> OrchCtxState]
_orch_contexts: ContextVar[dict[str, OrchCtxState]] = ContextVar('orch_contexts')


def _get_orch_state(ctx: str = "default") -> OrchCtxState:
    """Get state bundle for a named orchestration context.

    Raises:
        RuntimeError: If no active context with that name.
    """
    try:
        contexts = _orch_contexts.get()
    except LookupError:
        raise RuntimeError(
            f"No active orch context '{ctx}' - use 'async with orch_context()'"
        )
    if ctx not in contexts:
        raise RuntimeError(
            f"No active orch context '{ctx}' - use 'async with orch_context()'"
        )
    return contexts[ctx]


@asynccontextmanager
async def orch_context(ctx: str = "default") -> AsyncIterator[None]:
    """Async context manager for orchestration database access.

    Creates an SQLAlchemy AsyncEngine on enter and disposes it on exit.

    Args:
        ctx: Named context key (default "default").
    """
    engine = create_async_engine(get_pg_url(), echo=False)

    state = OrchCtxState(engine=engine)

    # Copy-on-write
    try:
        existing = _orch_contexts.get()
    except LookupError:
        existing = {}
    contexts = dict(existing)
    contexts[ctx] = state
    token = _orch_contexts.set(contexts)

    try:
        yield
    finally:
        _orch_contexts.reset(token)
        await engine.dispose()


@asynccontextmanager
async def get_orch_session(ctx: str = "default") -> AsyncIterator[AsyncSession]:
    """Get a database session from the active orchestration context.

    Yields:
        AsyncSession configured with expire_on_commit=False
    """
    state = _get_orch_state(ctx)
    async with AsyncSession(state.engine, expire_on_commit=False) as session:
        yield session


async def apply(
    items: TasksType,
    job_id: int,
    ctx: str = "default",
) -> TasksType:
    """Commit tasks, groups, and their dependencies to the database.

    Sets job_id on all items, generates snowflake IDs for Groups
    if not already set, and commits to PostgreSQL.

    Args:
        items: Single Task/Group or list of Task/Group objects
        job_id: Job ID to assign to all items
        ctx: Named context key (default "default")

    Returns:
        Same items with database IDs populated
    """
    items_list = items if isinstance(items, list) else [items]

    async with get_orch_session(ctx) as session:
        for item in items_list:
            item.job_id = job_id

            if isinstance(item, Group) and item.id is None:
                item.id = get_snowflake_id()

            session.add(item)

        await session.commit()

    if isinstance(items, list):
        return items_list
    return items_list[0]
