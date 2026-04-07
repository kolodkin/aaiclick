"""Orchestration context manager and per-task scope."""

from __future__ import annotations

import asyncio
import logging
import weakref
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncIterator, Dict

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from aaiclick.backend import is_postgres
from aaiclick.data.data_context.ch_client import create_ch_client, get_ch_client, _ch_client_var
from aaiclick.data.data_context.data_context import _engine_var, _objects_var, decref
from aaiclick.data.data_context.lifecycle import LifecycleHandler, _lifecycle_var
from aaiclick.data.models import ENGINE_DEFAULT
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
               "result_aai_ids", "sql_template", "task_id", "job_id", "run_id", "created_at"]
_OPLOG_TYPE_NAMES = [OPERATION_LOG_EXPECTED_COLUMNS[c] for c in _OPLOG_COLS]

_REG_COLS = ["table_name", "job_id", "task_id", "run_id", "created_at"]
_REG_TYPE_NAMES = [TABLE_REGISTRY_EXPECTED_COLUMNS[c] for c in _REG_COLS]



class OrchLifecycleHandler(LifecycleHandler):
    """Distributed lifecycle handler using shared resources from orch_context.

    Uses get_sql_session() for DB ops and get_ch_client() for CH ops —
    no private engine or client needed.

    Refcount updates are written to SQL immediately.  Actual table drops
    are **never** triggered inline — the background worker is the sole
    cleanup authority (``HAVING SUM(refcount) <= 0 → DROP``).

    Args:
        task_id: Task ID used as context_id for grouping this handler's refs.
        job_id: Job ID used as context_id for pin operations.
        run_id: Per-attempt snowflake ID for oplog isolation across retries.
    """

    def __init__(
        self,
        task_id: int,
        job_id: int,
        run_id: int,
    ):
        self._task_id = task_id
        self._job_id = job_id
        self._run_id = run_id
        self._queue: asyncio.Queue[DBLifecycleMessage] = asyncio.Queue()
        self._task: asyncio.Task | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pinned: set[str] = set()

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.create_task(self._process_loop())

    def _enqueue(self, msg: DBLifecycleMessage) -> None:
        """Thread-safe enqueue via call_soon_threadsafe."""
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, msg)

    async def stop(self) -> None:
        """Drain the lifecycle queue.

        Non-pinned objects are deterministically decreffed at task_scope exit
        before this method is called, so their task-scoped refs are already at
        zero.  Pinned tables keep their job-scoped ref; the background worker
        handles actual table drops once all refs reach zero.
        """
        self._enqueue(DBLifecycleMessage(DBLifecycleOp.SHUTDOWN))
        if self._task:
            await self._task

    def incref(self, table_name: str) -> None:
        self._enqueue(DBLifecycleMessage(DBLifecycleOp.INCREF, table_name))

    def decref(self, table_name: str) -> None:
        self._enqueue(DBLifecycleMessage(DBLifecycleOp.DECREF, table_name))

    @property
    def pinned_tables(self) -> set[str]:
        """Tables pinned by this task (should not be decreffed at exit)."""
        return self._pinned

    def pin(self, table_name: str) -> None:
        """Mark table as result — inserts a job-scoped ref that survives stop()."""
        self._pinned.add(table_name)
        self._enqueue(DBLifecycleMessage(DBLifecycleOp.PIN, table_name))

    async def claim(self, table_name: str, job_id: int) -> None:
        """Release a pinned ref (ownership transfer to consumer).

        Clears the job_id marker on the ref row so it becomes a plain task ref.
        The consumer's incref already created its own row.
        """
        async with get_sql_session() as session:
            await session.execute(
                text(
                    "UPDATE table_context_refs "
                    "SET job_id = NULL "
                    "WHERE table_name = :table AND job_id = :job_id"
                ),
                {"table": table_name, "job_id": job_id},
            )
            await session.commit()

    # -- Oplog methods (enqueue to same FIFO as incref/decref) --

    def oplog_record(self, result_table: str, operation: str,
                     kwargs: dict[str, str] | None = None, sql: str | None = None) -> None:
        self._enqueue(DBLifecycleMessage(
            DBLifecycleOp.OPLOG_RECORD,
            oplog=OplogPayload(result_table, operation, kwargs or {}, sql,
                               self._task_id, self._job_id, self._run_id),
        ))

    def oplog_record_sample(self, result_table: str, operation: str,
                            kwargs: dict[str, str] | None = None, sql: str | None = None) -> None:
        self._enqueue(DBLifecycleMessage(
            DBLifecycleOp.OPLOG_SAMPLE,
            oplog=OplogPayload(result_table, operation, kwargs or {}, sql,
                               self._task_id, self._job_id, self._run_id),
        ))

    def oplog_record_table(self, table_name: str) -> None:
        self._enqueue(DBLifecycleMessage(
            DBLifecycleOp.OPLOG_TABLE,
            oplog_table=OplogTablePayload(table_name, self._task_id, self._job_id, self._run_id),
        ))

    # -- Internal --

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
                  p.sql, p.task_id, p.job_id, p.run_id, now]],
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
                [[p.table_name, p.job_id, p.task_id, p.run_id, now]],
                column_names=_REG_COLS,
                column_type_names=_REG_TYPE_NAMES,
            )
        except Exception:
            logger.error("Failed to write table registry for %s", p.table_name, exc_info=True)

    async def _process_loop(self) -> None:
        while True:
            msg = await self._queue.get()
            if msg.op == DBLifecycleOp.SHUTDOWN:
                break

            # -- Table lifecycle (refcount only, no inline drops) --
            if msg.op in (DBLifecycleOp.INCREF, DBLifecycleOp.DECREF, DBLifecycleOp.PIN):
                async with get_sql_session() as session:
                    if msg.op in (DBLifecycleOp.INCREF, DBLifecycleOp.PIN):
                        job_id = self._job_id if msg.op == DBLifecycleOp.PIN else None
                        await session.execute(
                            text(
                                "INSERT INTO table_context_refs (table_name, context_id, refcount, job_id) "
                                "VALUES (:table_name, :context_id, 1, :job_id) "
                                "ON CONFLICT (table_name, context_id) "
                                "DO UPDATE SET refcount = table_context_refs.refcount + 1, "
                                "job_id = COALESCE(:job_id, table_context_refs.job_id)"
                            ),
                            {"table_name": msg.table_name, "context_id": self._task_id,
                             "job_id": job_id},
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
                    await session.commit()

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
async def task_scope(task_id: int, job_id: int, run_id: int) -> AsyncIterator[None]:
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
        run_id: Per-attempt snowflake ID for oplog isolation across retries.
    """
    lifecycle = OrchLifecycleHandler(
        task_id=task_id,
        job_id=job_id,
        run_id=run_id,
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

        # Decref non-pinned objects; skip pinned (their job-scoped ref
        # keeps them alive for downstream consumers).  Stale-mark all so
        # __del__ is a no-op.
        pinned = lifecycle.pinned_tables
        for obj_ref in objects.values():
            obj = obj_ref()
            if obj is not None:
                obj._stale = True
                if obj._registered and not obj.persistent and obj.table not in pinned:
                    decref(obj.table)
                obj._registered = False
        objects.clear()
        _objects_var.reset(obj_token)

        # Drain remaining lifecycle messages (decrefs enqueued above)
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
