"""Orchestration context manager and per-task scope."""

from __future__ import annotations

import asyncio
import logging
import queue
import weakref
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncIterator, Dict

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from aaiclick.backend import is_postgres
from aaiclick.data.data_context.ch_client import create_ch_client, get_ch_client, _ch_client_var
from aaiclick.data.data_context.data_context import _engine_var, _objects_var
from aaiclick.data.data_context.lifecycle import LifecycleHandler, _lifecycle_var
from aaiclick.data.models import ENGINE_DEFAULT
from aaiclick.oplog.cleanup import lineage_aware_drop
from aaiclick.oplog.models import OPERATION_LOG_EXPECTED_COLUMNS, TABLE_REGISTRY_EXPECTED_COLUMNS, init_oplog_tables
from aaiclick.oplog.sampling import sample_lineage
from ..snowflake_id import get_snowflake_id
from .execution.db_handler import create_db_handler, get_db_handler, _db_handler_var
from .sql_context import get_sql_session, _sql_engine_var
from .lifecycle.db_lifecycle import DBLifecycleMessage, DBLifecycleOp, OplogPayload, OplogTablePayload
from .env import get_db_url
from .task_registry import _task_registry_var, get_task_registry
from .models import Group, Task, TasksType

logger = logging.getLogger(__name__)

_OPLOG_COLS = ["result_table", "operation", "kwargs", "kwargs_aai_ids",
               "result_aai_ids", "sql_template", "task_id", "job_id", "created_at"]
_OPLOG_TYPE_NAMES = [OPERATION_LOG_EXPECTED_COLUMNS[c] for c in _OPLOG_COLS]

_REG_COLS = ["table_name", "job_id", "task_id", "created_at"]
_REG_TYPE_NAMES = [TABLE_REGISTRY_EXPECTED_COLUMNS[c] for c in _REG_COLS]



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
        self._queue.put(DBLifecycleMessage(DBLifecycleOp.SHUTDOWN))
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

    # -- Oplog methods (enqueue to same FIFO as incref/decref) --

    def oplog_record(self, result_table: str, operation: str,
                     kwargs: dict[str, str] | None = None, sql: str | None = None) -> None:
        self._queue.put(DBLifecycleMessage(
            DBLifecycleOp.OPLOG_RECORD,
            oplog=OplogPayload(result_table, operation, kwargs or {}, sql,
                               self._task_id, self._job_id),
        ))

    def oplog_record_sample(self, result_table: str, operation: str,
                            kwargs: dict[str, str] | None = None, sql: str | None = None) -> None:
        self._queue.put(DBLifecycleMessage(
            DBLifecycleOp.OPLOG_SAMPLE,
            oplog=OplogPayload(result_table, operation, kwargs or {}, sql,
                               self._task_id, self._job_id),
        ))

    def oplog_record_table(self, table_name: str) -> None:
        self._queue.put(DBLifecycleMessage(
            DBLifecycleOp.OPLOG_TABLE,
            oplog_table=OplogTablePayload(table_name, self._task_id, self._job_id),
        ))

    # -- Internal --

    async def _create_sample_and_drop(self, table_name: str) -> None:
        """Replace table with lineage-referenced rows, then drop original."""
        await lineage_aware_drop(get_ch_client(), table_name)

    async def _write_oplog_row(self, p: OplogPayload,
                               kwargs_aai_ids: dict[str, list[int]] | None = None,
                               result_aai_ids: list[int] | None = None) -> None:
        """Insert a single oplog row to ClickHouse. Best effort."""
        now = datetime.now(timezone.utc)
        try:
            await get_ch_client().insert(
                "operation_log",
                [[p.result_table, p.operation, p.kwargs,
                  kwargs_aai_ids or {}, result_aai_ids or [],
                  p.sql, p.task_id, p.job_id, now]],
                column_names=_OPLOG_COLS,
                column_type_names=_OPLOG_TYPE_NAMES,
            )
        except Exception:
            logger.error("Failed to write oplog for %s", p.result_table, exc_info=True)

    async def _write_table_registry_row(self, p: OplogTablePayload) -> None:
        """Insert a single table_registry row to ClickHouse. Best effort."""
        now = datetime.now(timezone.utc)
        try:
            await get_ch_client().insert(
                "table_registry",
                [[p.table_name, p.job_id, p.task_id, now]],
                column_names=_REG_COLS,
                column_type_names=_REG_TYPE_NAMES,
            )
        except Exception:
            logger.error("Failed to write table registry for %s", p.table_name, exc_info=True)

    async def _process_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            msg = await loop.run_in_executor(None, self._queue.get)
            if msg.op == DBLifecycleOp.SHUTDOWN:
                break

            # -- Table lifecycle --
            if msg.op in (DBLifecycleOp.INCREF, DBLifecycleOp.DECREF, DBLifecycleOp.PIN):
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

            # -- Oplog (immediate write, no buffer) --
            elif msg.op == DBLifecycleOp.OPLOG_RECORD:
                await self._write_oplog_row(msg.oplog)
            elif msg.op == DBLifecycleOp.OPLOG_SAMPLE:
                kwargs_aai_ids, result_aai_ids = {}, []
                if msg.oplog.kwargs:
                    try:
                        kwargs_aai_ids, result_aai_ids = await sample_lineage(
                            get_ch_client(), msg.oplog.result_table, msg.oplog.kwargs,
                        )
                    except Exception:
                        logger.error("Failed to sample lineage for %s",
                                     msg.oplog.result_table, exc_info=True)
                await self._write_oplog_row(msg.oplog, kwargs_aai_ids, result_aai_ids)
            elif msg.op == DBLifecycleOp.OPLOG_TABLE:
                await self._write_table_registry_row(msg.oplog_table)



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
                "PostgreSQL requires the aaiclick[distributed] extra. "
                "Install with: pip install aaiclick[distributed]"
            ) from e
    engine = create_async_engine(get_db_url(), echo=False)
    handler = create_db_handler()

    sql_token = _sql_engine_var.set(engine)
    db_token = _db_handler_var.set(handler)
    eng_token = _engine_var.set(ENGINE_DEFAULT)
    registry_token = _task_registry_var.set({})

    ch_token = None
    if with_ch:
        ch_client = await create_ch_client()
        ch_token = _ch_client_var.set(ch_client)

    try:
        yield
    finally:
        _sql_engine_var.reset(sql_token)
        _db_handler_var.reset(db_token)
        _engine_var.reset(eng_token)
        if ch_token is not None:
            _ch_client_var.reset(ch_token)
        _task_registry_var.reset(registry_token)
        await engine.dispose()


@asynccontextmanager
async def task_scope(task_id: int, job_id: int) -> AsyncIterator[None]:
    """Per-task context nested inside orch_context.

    Creates isolated per-task state:
    - Fresh objects registry for stale-marking on exit
    - OrchLifecycleHandler using task_id as context_id for distributed refcounting
    - Oplog recording via OrchLifecycleHandler queue (always active in orch mode)

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
    await init_oplog_tables(get_ch_client())

    lc_token = _lifecycle_var.set(lifecycle)
    obj_token = _objects_var.set(objects)
    registry_token = _task_registry_var.set({})

    try:
        yield
    finally:
        _task_registry_var.reset(registry_token)

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

    visited: dict = {}
    result: list = []

    def visit(node: object) -> None:
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
