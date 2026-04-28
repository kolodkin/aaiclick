"""Orchestration context manager and per-task scope."""

from __future__ import annotations

import logging
import weakref
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from aaiclick.backend import is_postgres
from aaiclick.data.data_context.ch_client import _ch_client_var, create_ch_client, get_ch_client
from aaiclick.data.data_context.data_context import _engine_var, _objects_var
from aaiclick.data.data_context.lifecycle import _lifecycle_var
from aaiclick.data.models import ENGINE_DEFAULT
from aaiclick.oplog.models import init_oplog_tables

from ..snowflake import get_snowflake_id
from .env import get_db_url
from .execution.db_handler import _db_handler_var, create_db_handler, get_db_handler  # noqa: F401
from .lifecycle.db_lifecycle import release_task_name_locks_for_task
from .lifecycle.task_lifecycle import TaskLifecycleHandler
from .models import Group, Job, Task, TasksType
from .oplog_backfill import migrate_table_registry_to_sql
from .sql_context import _sql_engine_var, get_sql_session
from .task_registry import _task_registry_var, get_task_registry

logger = logging.getLogger(__name__)


@asynccontextmanager
async def orch_context(with_ch: bool = True) -> AsyncIterator[None]:
    """Async context manager for all orchestration operations.

    Creates shared resources on enter:
    - SQLAlchemy AsyncEngine for orchestration SQL (set in _sql_engine_var)
    - DbHandler for database operations (set in _db_handler_var)
    - ChClient for ClickHouse operations (set in _ch_client_var, if with_ch=True)

    Sets ContextVars for the duration:
    - _sql_engine_var: SQL engine (accessed via get_sql_session())
    - _db_handler_var: DB handler (accessed via get_db_handler())
    - _ch_client_var: shared ClickHouse client (accessed via get_ch_client())
    - _engine_var: ENGINE_DEFAULT for data operations

    Args:
        with_ch: Whether to open a ClickHouse client. Set to False for read-only
            SQL-only operations (e.g. job status queries) to avoid acquiring the
            chdb file lock, which would conflict with a running worker process.

    Per-task state (lifecycle handler, objects, oplog) is managed by task_scope().
    """
    if is_postgres():
        try:
            import asyncpg  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "PostgreSQL requires the aaiclick[distributed] extra. Install with: pip install aaiclick[distributed]"
            ) from e
    engine = create_async_engine(get_db_url(), echo=False)
    handler = create_db_handler()

    sql_token = _sql_engine_var.set(engine)
    db_token = _db_handler_var.set(handler)
    eng_token = _engine_var.set(ENGINE_DEFAULT)
    registry_token = _task_registry_var.set({})

    ch_token = None
    if with_ch:
        # Reuse an outer context's ch_client so nested ``orch_context()`` calls
        # (e.g. ``ajob_test``) don't tear down the shared chdb Session — chdb's
        # Session cannot be safely closed and reopened in-process (see
        # ``docs/technical_debt.md``).
        existing = _ch_client_var.get()
        ch_client = existing if existing is not None else await create_ch_client()
        ch_token = _ch_client_var.set(ch_client)

    try:
        yield
    finally:
        _sql_engine_var.reset(sql_token)
        _db_handler_var.reset(db_token)
        _engine_var.reset(eng_token)
        if ch_token is not None:
            _ch_client_var.reset(ch_token)
            # chdb's Session is a true per-process singleton: closing it mid-process
            # leaves dangling references in concurrent tasks (e.g. the lifespan worker
            # vs request handlers under uvicorn) and re-opening is not safely supported
            # — see `pin_chdb_session` in aaiclick/testing.py for the test-side mirror
            # of the same constraint. Process exit cleans up the OS resources.
        _task_registry_var.reset(registry_token)
        await engine.dispose()


@asynccontextmanager
async def task_scope(
    task_id: int,
    job_id: int,
    run_id: int,
) -> AsyncIterator[None]:
    """Per-task context nested inside orch_context.

    Uses :class:`TaskLifecycleHandler` to own the SQL-side writes
    (``table_registry``, ``table_pin_refs``, ``operation_log``) and to
    track every CH table the task touches with its
    ``(owned, preserved, pinned)`` flags. On exit, drops every owned
    table that is neither pinned (a downstream consumer needs it) nor
    preserved (the job declared it long-lived) — on the failure path
    only ``t_*`` scratch tables are dropped, leaving every ``j_<id>_*``
    for the BackgroundWorker.

    Locks held in ``task_name_locks`` by this task are released regardless
    of outcome.

    Args:
        task_id: ID of the current task.
        job_id: ID of the job.
        run_id: Per-attempt snowflake ID for oplog isolation across retries.
    """
    ch_client = get_ch_client()
    async with get_sql_session() as session:
        job_row = (await session.execute(select(Job.preserve).where(Job.id == job_id))).first()
        preserve = job_row[0] if job_row is not None else None
    lifecycle = TaskLifecycleHandler(
        task_id=task_id,
        job_id=job_id,
        run_id=run_id,
        ch_client=ch_client,
        preserve=preserve,
    )
    await lifecycle.start()

    objects: dict[int, weakref.ref] = {}
    await init_oplog_tables(ch_client)
    await migrate_table_registry_to_sql(ch_client)

    lc_token = _lifecycle_var.set(lifecycle)
    obj_token = _objects_var.set(objects)
    registry_token = _task_registry_var.set({})

    success = False
    try:
        yield
        success = True
    finally:
        _task_registry_var.reset(registry_token)

        for obj_ref in objects.values():
            obj = obj_ref()
            if obj is not None:
                obj._stale = True
                obj._registered = False
        objects.clear()
        _objects_var.reset(obj_token)

        await lifecycle.flush()
        for tt in list(lifecycle.iter_tracked_tables()):
            if not tt.owned or tt.pinned:
                continue
            if tt.name.startswith("p_"):
                continue
            if success and tt.preserved:
                continue
            if not success and not tt.name.startswith("t_"):
                continue
            try:
                await ch_client.command(f"DROP TABLE IF EXISTS {tt.name}")
            except Exception:
                logger.warning("Failed to drop %s on task exit", tt.name, exc_info=True)

        await lifecycle.stop()
        _lifecycle_var.reset(lc_token)

        async with get_sql_session() as session:
            await release_task_name_locks_for_task(session, task_id=task_id)
            await session.commit()


def _collect_from_registry(items: list[Task | Group]) -> list[Task | Group]:
    """Collect all reachable Task/Group objects via dependency IDs and the task registry.

    Walks the dependency graph starting from ``items``, looking up each
    upstream ID in the active registry (ContextVar). Registry entries are
    in-memory objects not yet persisted; missing entries are already in the
    DB so traversal stops there naturally.

    Returns objects in dependency-first order so SQLAlchemy inserts them
    without FK violations. If no registry is active, returns ``items`` as-is.
    """
    registry = get_task_registry()
    if registry is None:
        return items

    visited: dict[int, Task | Group] = {}
    result: list[Task | Group] = []

    def visit(node: Task | Group) -> None:
        if id(node) in visited:
            return
        visited[id(node)] = node
        for dep in node.previous_dependencies:
            upstream = registry.get(dep.previous_id)
            if upstream is not None:
                visit(upstream)
        result.append(node)

    for item in items:
        visit(item)

    return result


async def commit_tasks(
    items: TasksType,
    job_id: int,
) -> TasksType:
    """Commit tasks, groups, and their dependencies to the database.

    Sets job_id on all items, generates snowflake IDs for Groups
    if not already set, and commits to the SQL database.

    All tasks and groups created via create_task() or Group() are tracked in
    the active task registry (ContextVar set by orch_context / task_scope).
    commit_tasks uses the registry to resolve upstream IDs from dependency
    records, so callers only need to pass terminal (leaf) tasks — all upstream
    tasks are discovered automatically.

    Args:
        items: Single Task/Group or list of Task/Group objects
        job_id: Job ID to assign to all items

    Returns:
        Same items with database IDs populated
    """
    items_list = items if isinstance(items, list) else [items]
    all_items = _collect_from_registry(items_list)

    async with get_sql_session() as session:
        for item in all_items:
            item.job_id = job_id

            if isinstance(item, Group) and item.id is None:
                item.id = get_snowflake_id()

            session.add(item)

        await session.commit()

    # Remove committed items from the registry so subsequent commit_tasks calls
    # don't re-visit them (which would trigger a detached lazy-load error).
    registry = get_task_registry()
    if registry is not None:
        for item in all_items:
            registry.pop(item.id, None)

    if isinstance(items, list):
        return items_list
    return items_list[0]
