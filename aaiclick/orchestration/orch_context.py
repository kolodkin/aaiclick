"""Orchestration context manager and per-task scope."""

from __future__ import annotations

import asyncio
import logging
import weakref
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from aaiclick.backend import is_postgres
from aaiclick.data.data_context.ch_client import _ch_client_var, create_ch_client, get_ch_client
from aaiclick.data.data_context.data_context import _engine_var, _objects_var, decref
from aaiclick.data.data_context.lifecycle import LifecycleHandler, _lifecycle_var
from aaiclick.data.models import ENGINE_DEFAULT
from aaiclick.locks import lookup_advisory_id
from aaiclick.oplog.models import OPERATION_LOG_EXPECTED_COLUMNS, init_oplog_tables

from ..snowflake import get_snowflake_id
from .env import get_db_url
from .execution.db_handler import _db_handler_var, create_db_handler, get_db_handler  # noqa: F401
from .lifecycle.db_lifecycle import DBLifecycleMessage, DBLifecycleOp, OplogPayload, OplogTablePayload
from .models import Group, Task, TasksType
from .oplog_backfill import migrate_table_registry_to_sql
from .sql_context import _sql_engine_var, get_sql_session
from .task_registry import _task_registry_var, get_task_registry

logger = logging.getLogger(__name__)

_OPLOG_COLS = [
    "result_table",
    "operation",
    "kwargs",
    "sql_template",
    "task_id",
    "job_id",
    "run_id",
    "created_at",
]
_OPLOG_TYPE_NAMES = [OPERATION_LOG_EXPECTED_COLUMNS[c] for c in _OPLOG_COLS]


class OrchLifecycleHandler(LifecycleHandler):
    """Distributed lifecycle handler using shared resources from orch_context.

    Uses get_sql_session() for DB ops and get_ch_client() for CH ops —
    no private engine or client needed.

    incref inserts a row into table_run_refs; decref deletes it.
    Actual table drops are **never** triggered inline — the background
    worker is the sole cleanup authority (no run refs + no pin → DROP).

    If a task crashes mid-execution, ``clean_task_run(run_id)`` deletes
    all table_run_refs rows for that run_id, making affected tables
    eligible for cleanup.

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

    async def flush(self) -> None:
        """Block until every message already enqueued has been processed.

        Used by tests and by any caller that needs to read back state the
        handler writes (e.g. ``table_registry.schema_doc`` after a
        ``create_object`` call).
        """
        event = asyncio.Event()
        self._enqueue(DBLifecycleMessage(DBLifecycleOp.FLUSH, flush_event=event))
        await event.wait()

    def incref(self, table_name: str) -> None:
        self._enqueue(DBLifecycleMessage(DBLifecycleOp.INCREF, table_name))

    def decref(self, table_name: str) -> None:
        self._enqueue(DBLifecycleMessage(DBLifecycleOp.DECREF, table_name))

    def pin(self, table_name: str) -> None:
        """Pin a table for all downstream consumer tasks.

        The PIN handler fans out: queries the dependencies table to find
        all tasks that depend on the current task, then inserts one
        pin_ref(table_name, consumer_task_id) per consumer.

        Only at runtime (when the producer returns an Object) do we know
        both the table_name and the upstream task_id. The downstream
        consumer task_ids are discovered via the dependencies table —
        this is the only point where table→task mapping exists.
        """
        self._enqueue(
            DBLifecycleMessage(
                DBLifecycleOp.PIN,
                table_name,
                pin_task_id=self._task_id,
            )
        )

    def unpin(self, table_name: str) -> None:
        """Remove this task's pin ref for a table.

        Enqueued through the FIFO queue so it executes AFTER any preceding
        INCREF, ensuring the run_ref is committed before the pin is released.
        """
        self._enqueue(
            DBLifecycleMessage(
                DBLifecycleOp.UNPIN,
                table_name,
                pin_task_id=self._task_id,
            )
        )

    # -- Oplog methods (enqueue to same FIFO as incref/decref) --

    def _make_payload(
        self,
        result_table: str,
        operation: str,
        kwargs: dict[str, str] | None,
        sql: str | None,
    ) -> OplogPayload:
        return OplogPayload(
            result_table=result_table,
            operation=operation,
            kwargs=kwargs or {},
            sql=sql,
            task_id=self._task_id,
            job_id=self._job_id,
            run_id=self._run_id,
        )

    def oplog_record(
        self, result_table: str, operation: str, kwargs: dict[str, str] | None = None, sql: str | None = None
    ) -> None:
        self._enqueue(
            DBLifecycleMessage(
                DBLifecycleOp.OPLOG_RECORD,
                oplog=self._make_payload(result_table, operation, kwargs, sql),
            )
        )

    def oplog_record_sample(
        self, result_table: str, operation: str, kwargs: dict[str, str] | None = None, sql: str | None = None
    ) -> None:
        self.oplog_record(result_table, operation, kwargs, sql)

    def register_table(self, table_name: str, schema_doc: str | None = None) -> None:
        self._enqueue(
            DBLifecycleMessage(
                DBLifecycleOp.OPLOG_TABLE,
                oplog_table=OplogTablePayload(
                    table_name,
                    self._task_id,
                    self._job_id,
                    self._run_id,
                    schema_doc=schema_doc,
                ),
            )
        )

    def current_job_id(self) -> int | None:
        return self._job_id

    # -- Internal --

    async def _write_oplog_row(self, p: OplogPayload) -> None:
        """Insert a single oplog row to ClickHouse. Best effort."""
        # Strip tzinfo: TableRegistry.created_at and operation_log timestamps are
        # mapped to naive SQL/CH columns; asyncpg rejects aware datetimes there.
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        try:
            await get_ch_client().insert(
                "operation_log",
                [
                    [
                        p.result_table,
                        p.operation,
                        p.kwargs,
                        p.sql,
                        p.task_id,
                        p.job_id,
                        p.run_id,
                        now,
                    ]
                ],
                column_names=_OPLOG_COLS,
                column_type_names=_OPLOG_TYPE_NAMES,
            )
        except Exception:
            logger.error("Failed to write oplog for %s", p.result_table, exc_info=True)

    async def _write_table_registry_row(self, p: OplogTablePayload) -> None:
        """Insert a single table_registry row to SQL. Best effort.

        Idempotent via ON CONFLICT DO NOTHING — a re-register of the same
        table_name keeps the original owner (first-writer-wins).
        """
        # Strip tzinfo: TableRegistry.created_at and operation_log timestamps are
        # mapped to naive SQL/CH columns; asyncpg rejects aware datetimes there.
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        try:
            async with get_sql_session() as session:
                await session.execute(
                    text(
                        "INSERT INTO table_registry "
                        "(table_name, job_id, task_id, run_id, created_at, schema_doc) "
                        "VALUES (:table_name, :job_id, :task_id, :run_id, :created_at, :schema_doc) "
                        "ON CONFLICT (table_name) DO NOTHING"
                    ),
                    {
                        "table_name": p.table_name,
                        "job_id": p.job_id,
                        "task_id": p.task_id,
                        "run_id": p.run_id,
                        "created_at": now,
                        "schema_doc": p.schema_doc,
                    },
                )
                await session.commit()
        except Exception:
            logger.error("Failed to write table registry for %s", p.table_name, exc_info=True)

    async def _process_loop(self) -> None:
        run_id = str(self._run_id)
        while True:
            msg = await self._queue.get()
            if msg.op == DBLifecycleOp.SHUTDOWN:
                break

            # -- Table lifecycle (junction table, no inline drops) --
            if msg.op in (DBLifecycleOp.INCREF, DBLifecycleOp.DECREF, DBLifecycleOp.PIN, DBLifecycleOp.UNPIN):
                async with get_sql_session() as session:
                    if msg.op == DBLifecycleOp.INCREF:
                        # lookup_advisory_id holds the hashtext xact lock in
                        # distributed mode, so concurrent INCREFs on the same
                        # table_name serialize and agree on one advisory_id.
                        existing = await lookup_advisory_id(session, msg.table_name)
                        advisory_id = existing if existing is not None else get_snowflake_id()

                        # Register table in context refs (idempotent)
                        await session.execute(
                            text(
                                "INSERT INTO table_context_refs "
                                "(table_name, context_id, advisory_id) "
                                "VALUES (:table_name, :context_id, :advisory_id) "
                                "ON CONFLICT (table_name, context_id) DO NOTHING"
                            ),
                            {
                                "table_name": msg.table_name,
                                "context_id": self._task_id,
                                "advisory_id": advisory_id,
                            },
                        )
                        # Add run ref
                        await session.execute(
                            text(
                                "INSERT INTO table_run_refs (table_name, run_id) "
                                "VALUES (:table_name, :run_id) "
                                "ON CONFLICT (table_name, run_id) DO NOTHING"
                            ),
                            {"table_name": msg.table_name, "run_id": run_id},
                        )
                    elif msg.op == DBLifecycleOp.DECREF:
                        await session.execute(
                            text("DELETE FROM table_run_refs WHERE table_name = :table_name AND run_id = :run_id"),
                            {"table_name": msg.table_name, "run_id": run_id},
                        )
                    elif msg.op == DBLifecycleOp.PIN:
                        # Fan out: one pin_ref per downstream consumer task.
                        result = await session.execute(
                            text(
                                "SELECT next_id FROM dependencies "
                                "WHERE previous_id = :task_id "
                                "AND previous_type = 'task' AND next_type = 'task'"
                            ),
                            {"task_id": msg.pin_task_id},
                        )
                        consumer_ids = [row[0] for row in result.fetchall()]
                        for cid in consumer_ids:
                            await session.execute(
                                text(
                                    "INSERT INTO table_pin_refs (table_name, task_id) "
                                    "VALUES (:table_name, :task_id) "
                                    "ON CONFLICT (table_name, task_id) DO NOTHING"
                                ),
                                {"table_name": msg.table_name, "task_id": cid},
                            )
                    elif msg.op == DBLifecycleOp.UNPIN:
                        await session.execute(
                            text("DELETE FROM table_pin_refs WHERE table_name = :table_name AND task_id = :task_id"),
                            {"table_name": msg.table_name, "task_id": msg.pin_task_id},
                        )
                    await session.commit()

            # -- Oplog (immediate write, no buffer) --
            elif msg.op == DBLifecycleOp.OPLOG_RECORD:
                assert msg.oplog is not None
                await self._write_oplog_row(msg.oplog)
            elif msg.op == DBLifecycleOp.OPLOG_TABLE:
                assert msg.oplog_table is not None
                await self._write_table_registry_row(msg.oplog_table)

            # -- Flush barrier (signalled once all prior messages have been processed) --
            elif msg.op == DBLifecycleOp.FLUSH:
                assert msg.flush_event is not None
                msg.flush_event.set()


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
        # chdb's Session is a true per-process singleton (see
        # ``docs/technical_debt.md``): we open it once and reuse it for the
        # lifetime of the process. Reusing an outer context's client when
        # nested keeps that invariant for callers that enter ``orch_context``
        # multiple times (e.g. ``ajob_test``).
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
        _task_registry_var.reset(registry_token)
        await engine.dispose()


@asynccontextmanager
async def task_scope(
    task_id: int,
    job_id: int,
    run_id: int,
) -> AsyncIterator[None]:
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

    objects: dict[int, weakref.ref] = {}
    await init_oplog_tables(get_ch_client())
    await migrate_table_registry_to_sql(get_ch_client())

    lc_token = _lifecycle_var.set(lifecycle)
    obj_token = _objects_var.set(objects)
    registry_token = _task_registry_var.set({})

    try:
        yield
    finally:
        _task_registry_var.reset(registry_token)

        # Decref all live objects (deletes this run_id from table_run_refs).
        # Pinned tables are protected by their job_id marker in
        # table_context_refs — the background worker skips tables with
        # non-NULL job_id even when no run refs remain.
        for obj_ref in objects.values():
            obj = obj_ref()
            if obj is not None:
                obj._stale = True
                if obj._registered and obj._owns_lifecycle_ref and not obj.persistent:
                    decref(obj.table)
                obj._registered = False
        objects.clear()
        _objects_var.reset(obj_token)

        # Drain remaining lifecycle messages (decrefs enqueued above)
        await lifecycle.stop()
        _lifecycle_var.reset(lc_token)


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
