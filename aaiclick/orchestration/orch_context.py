"""Orchestration context manager and per-task scope."""

from __future__ import annotations

import asyncio
import queue
import weakref
from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from aaiclick.backend import is_postgres
from aaiclick.data.ch_client import create_ch_client, get_ch_client, _ch_client_var
from aaiclick.data.data_context import _engine_var, _objects_var
from aaiclick.data.lifecycle import LifecycleHandler, _lifecycle_var
from aaiclick.data.models import ENGINE_DEFAULT
from aaiclick.oplog.collector import OplogCollector, _oplog_collector
from aaiclick.oplog.models import init_oplog_tables
from ..snowflake_id import get_snowflake_id
from .db_handler import create_db_handler, get_db_handler, _db_handler_var
from .sql_context import get_sql_session, _sql_engine_var
from .db_lifecycle import DBLifecycleMessage, DBLifecycleOp
from .env import get_db_url
from .models import Group, Task, TasksType, _task_registry


class OrchLifecycleHandler(LifecycleHandler):
    """Distributed lifecycle handler using shared resources from orch_context.

    Uses get_sql_session() for DB ops and get_ch_client() for CH ops —
    no private engine or client needed.
    When DECREF causes total refcount across all contexts to reach 0,
    it creates a sample copy and drops the CH table.

    Args:
        task_id: Task ID used as context_id for grouping this handler's refs.
        job_id: Job ID used as context_id for pin operations.
    """

    def __init__(
        self,
        task_id: int,
        job_id: int,
    ):
        self._task_id = task_id
        self._job_id = job_id
        self._queue: queue.Queue[DBLifecycleMessage] = queue.Queue()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._process_loop())

    async def stop(self) -> None:
        """Drain queue then unconditionally delete all refs for this task_id.

        Pin refs use job_id as context_id, so they survive this cleanup.
        Only execution-time refs (incref/decref) are removed.
        """
        self._queue.put(DBLifecycleMessage(DBLifecycleOp.SHUTDOWN, ""))
        if self._task:
            await self._task
        async with get_sql_session() as session:
            await session.execute(
                text("DELETE FROM table_context_refs WHERE context_id = :ctx"),
                {"ctx": self._task_id},
            )
            await session.commit()

    def incref(self, table_name: str) -> None:
        self._queue.put(DBLifecycleMessage(DBLifecycleOp.INCREF, table_name))

    def decref(self, table_name: str) -> None:
        self._queue.put(DBLifecycleMessage(DBLifecycleOp.DECREF, table_name))

    def pin(self, table_name: str) -> None:
        """Mark table as result — inserts a job-scoped ref that survives stop()."""
        self._queue.put(DBLifecycleMessage(DBLifecycleOp.PIN, table_name))

    async def claim(self, table_name: str, job_id: int) -> None:
        """Release a job-scoped pinned ref (ownership transfer to consumer)."""
        async with get_sql_session() as session:
            await session.execute(
                text(
                    "DELETE FROM table_context_refs "
                    "WHERE table_name = :table AND context_id = :ctx"
                ),
                {"table": table_name, "ctx": job_id},
            )
            await session.commit()

    async def _create_sample_and_drop(self, table_name: str) -> None:
        """Create a sample copy of the table, then drop the original."""
        try:
            await get_ch_client().command(
                f"CREATE TABLE IF NOT EXISTS {table_name}_sample "
                f"ENGINE = MergeTree() ORDER BY tuple() "
                f"AS SELECT * FROM {table_name} LIMIT 1000"
            )
        except Exception:
            pass  # Best effort
        try:
            await get_ch_client().command(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass  # Best effort

    async def _process_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            msg = await loop.run_in_executor(None, self._queue.get)
            if msg.op == DBLifecycleOp.SHUTDOWN:
                break
            total = 0
            async with get_sql_session() as session:
                if msg.op == DBLifecycleOp.INCREF:
                    await session.execute(
                        text(
                            "INSERT INTO table_context_refs (table_name, context_id, refcount) "
                            "VALUES (:table_name, :context_id, 1) "
                            "ON CONFLICT (table_name, context_id) "
                            "DO UPDATE SET refcount = table_context_refs.refcount + 1"
                        ),
                        {"table_name": msg.table_name, "context_id": self._task_id},
                    )
                elif msg.op == DBLifecycleOp.DECREF:
                    await session.execute(
                        text(
                            "UPDATE table_context_refs "
                            "SET refcount = refcount - 1 "
                            "WHERE table_name = :table_name AND context_id = :context_id"
                        ),
                        {"table_name": msg.table_name, "context_id": self._task_id},
                    )
                    result = await session.execute(
                        text(
                            "SELECT COALESCE(SUM(refcount), 0) "
                            "FROM table_context_refs "
                            "WHERE table_name = :table_name"
                        ),
                        {"table_name": msg.table_name},
                    )
                    total = result.scalar_one() or 0
                elif msg.op == DBLifecycleOp.PIN:
                    await session.execute(
                        text(
                            "INSERT INTO table_context_refs (table_name, context_id, refcount) "
                            "VALUES (:table_name, :context_id, 1) "
                            "ON CONFLICT (table_name, context_id) "
                            "DO UPDATE SET refcount = table_context_refs.refcount + 1"
                        ),
                        {"table_name": msg.table_name, "context_id": self._job_id},
                    )
                await session.commit()
            if (
                msg.op == DBLifecycleOp.DECREF
                and total <= 0
                and not msg.table_name.startswith("p_")
            ):
                await self._create_sample_and_drop(msg.table_name)


@asynccontextmanager
async def orch_context() -> AsyncIterator[None]:
    """Async context manager for all orchestration operations.

    Creates shared resources on enter:
    - SQLAlchemy AsyncEngine for orchestration SQL (set in _sql_engine_var)
    - DbHandler for database operations (set in _db_handler_var)
    - ChClient for ClickHouse operations (set in _ch_client_var)

    Sets ContextVars for the duration:
    - _sql_engine_var: SQL engine (accessed via get_sql_session())
    - _db_handler_var: DB handler (accessed via get_db_handler())
    - _ch_client_var: shared ClickHouse client (accessed via get_ch_client())
    - _engine_var: ENGINE_DEFAULT for data operations

    Per-task state (lifecycle handler, objects, oplog) is managed by task_scope().
    """
    if is_postgres():
        try:
            import asyncpg  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "PostgreSQL requires the aaiclick[postgres] extra. "
                "Install with: pip install aaiclick[postgres]"
            ) from e
    engine = create_async_engine(get_db_url(), echo=False)
    handler = create_db_handler()
    ch_client = await create_ch_client()

    sql_token = _sql_engine_var.set(engine)
    db_token = _db_handler_var.set(handler)
    ch_token = _ch_client_var.set(ch_client)
    eng_token = _engine_var.set(ENGINE_DEFAULT)

    try:
        yield
    finally:
        _sql_engine_var.reset(sql_token)
        _db_handler_var.reset(db_token)
        _engine_var.reset(eng_token)
        _ch_client_var.reset(ch_token)
        await engine.dispose()


@asynccontextmanager
async def task_scope(task_id: int, job_id: int) -> AsyncIterator[None]:
    """Per-task context nested inside orch_context.

    Creates isolated per-task state:
    - Fresh objects registry for stale-marking on exit
    - OrchLifecycleHandler using task_id as context_id for distributed refcounting
    - OplogCollector for operation lineage (always active in orch mode)

    Oplog is flushed on clean exit; discarded on exception.
    All tracked objects are stale-marked on exit.

    Args:
        task_id: ID of the current task (used as context_id for lifecycle refs and oplog).
        job_id: ID of the job (for pin/claim lifecycle ownership).
    """
    lifecycle = OrchLifecycleHandler(
        task_id=task_id,
        job_id=job_id,
    )
    await lifecycle.start()

    objects: Dict[int, weakref.ref] = {}
    collector = OplogCollector(task_id=task_id, job_id=job_id)
    await init_oplog_tables(get_ch_client())

    lc_token = _lifecycle_var.set(lifecycle)
    obj_token = _objects_var.set(objects)
    oplog_token = _oplog_collector.set(collector)

    failed = False
    try:
        yield
    except Exception:
        failed = True
        raise
    finally:
        if not failed:
            await collector.flush()
        _oplog_collector.reset(oplog_token)

        # Stale-mark all tracked objects
        for obj_ref in objects.values():
            obj = obj_ref()
            if obj is not None:
                obj._stale = True
        objects.clear()
        _objects_var.reset(obj_token)

        # Stop lifecycle (drains queue, deletes context refs)
        await lifecycle.stop()
        _lifecycle_var.reset(lc_token)


def _collect_from_registry(items: list) -> list:
    """Collect all reachable Task/Group objects via dependency IDs and _task_registry.

    Walks the dependency graph starting from ``items``, looking up each
    upstream ID in _task_registry. Registry entries are in-memory objects
    that haven't been persisted yet; missing entries are already in the DB,
    so traversal stops there naturally.

    Returns objects in dependency-first order so SQLAlchemy inserts them
    without FK violations.
    """
    visited: dict = {}
    result: list = []

    def visit(node: object) -> None:
        if id(node) in visited:
            return
        visited[id(node)] = node
        for dep in node.previous_dependencies:
            upstream = _task_registry.get(dep.previous_id)
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
    _task_registry. commit_tasks uses the registry to resolve upstream IDs
    from dependency records, so callers only need to pass terminal (leaf)
    tasks — all upstream tasks are discovered automatically.

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
    for item in all_items:
        _task_registry.pop(item.id, None)

    if isinstance(items, list):
        return items_list
    return items_list[0]
