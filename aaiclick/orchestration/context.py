"""Function-based orchestration context for database connections."""

from __future__ import annotations

import asyncio
import queue
import weakref
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import AsyncIterator, Dict

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from aaiclick.data.ch_client import ChClient, create_ch_client, _ch_client_var
from aaiclick.data.data_context import _engine_var, _objects_var
from aaiclick.data.lifecycle import LifecycleHandler, _lifecycle_var
from aaiclick.data.models import ENGINE_DEFAULT
from aaiclick.oplog.collector import OplogCollector, _oplog_collector
from aaiclick.oplog.models import init_oplog_tables
from ..snowflake_id import get_snowflake_id
from .db_handler import DbHandler, create_db_handler
from .db_lifecycle import PgLifecycleMessage, PgLifecycleOp
from .env import get_db_url
from .models import Group, Task, TasksType


@dataclass
class OrchCtxState:
    """State bundle for a named orchestration context."""

    engine: AsyncEngine
    db_handler: DbHandler
    ch_client: ChClient
    pg_engine: AsyncEngine


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


class _OrchLifecycleView(LifecycleHandler):
    """Distributed lifecycle handler using shared resources from orch_context.

    Unlike PgLifecycleHandler, this class does NOT create its own engine.
    It uses the shared pg_engine from orch_context for all database ops.
    When DECREF causes total refcount across all contexts to reach 0,
    it creates a sample copy and drops the CH table.

    Args:
        pg_engine: Shared SQLAlchemy engine from orch_context.
        ch_client: Shared ClickHouse client from orch_context.
        context_id: Snowflake ID for grouping this handler's refs.
        job_id: Job ID used as context_id for pin operations.
    """

    def __init__(
        self,
        pg_engine: AsyncEngine,
        ch_client: ChClient,
        context_id: int,
        job_id: int,
    ):
        self._engine = pg_engine
        self._ch_client = ch_client
        self._context_id = context_id
        self._job_id = job_id
        self._queue: queue.Queue[PgLifecycleMessage] = queue.Queue()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._process_loop())

    async def stop(self) -> None:
        """Drain queue then unconditionally delete all refs for this context_id.

        Pin refs use job_id as context_id, so they survive this cleanup.
        Only execution-time refs (incref/decref) are removed.
        """
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.SHUTDOWN, ""))
        if self._task:
            await self._task
        async with AsyncSession(self._engine) as session:
            await session.execute(
                text("DELETE FROM table_context_refs WHERE context_id = :ctx"),
                {"ctx": self._context_id},
            )
            await session.commit()

    def incref(self, table_name: str) -> None:
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.INCREF, table_name))

    def decref(self, table_name: str) -> None:
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.DECREF, table_name))

    def pin(self, table_name: str) -> None:
        """Mark table as result — inserts a job-scoped ref that survives stop()."""
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.PIN, table_name))

    async def claim(self, table_name: str, job_id: int) -> None:
        """Release a job-scoped pinned ref (ownership transfer to consumer)."""
        async with AsyncSession(self._engine) as session:
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
            await self._ch_client.command(
                f"CREATE TABLE IF NOT EXISTS {table_name}_sample "
                f"ENGINE = MergeTree() ORDER BY tuple() "
                f"AS SELECT * FROM {table_name} LIMIT 1000"
            )
        except Exception:
            pass  # Best effort
        try:
            await self._ch_client.command(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass  # Best effort

    async def _process_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            msg = await loop.run_in_executor(None, self._queue.get)
            if msg.op == PgLifecycleOp.SHUTDOWN:
                break
            total = 0
            async with AsyncSession(self._engine) as session:
                if msg.op == PgLifecycleOp.INCREF:
                    await session.execute(
                        text(
                            "INSERT INTO table_context_refs (table_name, context_id, refcount) "
                            "VALUES (:table_name, :context_id, 1) "
                            "ON CONFLICT (table_name, context_id) "
                            "DO UPDATE SET refcount = table_context_refs.refcount + 1"
                        ),
                        {"table_name": msg.table_name, "context_id": self._context_id},
                    )
                elif msg.op == PgLifecycleOp.DECREF:
                    await session.execute(
                        text(
                            "UPDATE table_context_refs "
                            "SET refcount = refcount - 1 "
                            "WHERE table_name = :table_name AND context_id = :context_id"
                        ),
                        {"table_name": msg.table_name, "context_id": self._context_id},
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
                elif msg.op == PgLifecycleOp.PIN:
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
                msg.op == PgLifecycleOp.DECREF
                and total <= 0
                and not msg.table_name.startswith("p_")
            ):
                await self._create_sample_and_drop(msg.table_name)


@asynccontextmanager
async def orch_context(ctx: str = "default") -> AsyncIterator[None]:
    """Async context manager for all orchestration operations.

    Creates shared resources on enter:
    - SQLAlchemy AsyncEngine for orchestration SQL
    - DbHandler for database operations
    - ChClient for ClickHouse operations (set in _ch_client_var)
    - AsyncEngine (pg_engine) for distributed lifecycle

    Sets ContextVars for the duration:
    - _ch_client_var: shared ClickHouse client
    - _engine_var: ENGINE_DEFAULT for data operations
    - _orch_contexts: named state bundle

    Per-task state (lifecycle view, objects, oplog) is managed by task_scope().

    Args:
        ctx: Named context key (default "default").
    """
    engine = create_async_engine(get_db_url(), echo=False)
    handler = create_db_handler()
    ch_client = await create_ch_client()
    pg_engine = create_async_engine(get_db_url(), echo=False)

    state = OrchCtxState(
        engine=engine,
        db_handler=handler,
        ch_client=ch_client,
        pg_engine=pg_engine,
    )

    # Copy-on-write
    try:
        existing = _orch_contexts.get()
    except LookupError:
        existing = {}
    contexts = dict(existing)
    contexts[ctx] = state
    token = _orch_contexts.set(contexts)

    ch_token = _ch_client_var.set(ch_client)
    eng_token = _engine_var.set(ENGINE_DEFAULT)

    try:
        yield
    finally:
        _orch_contexts.reset(token)
        _engine_var.reset(eng_token)
        _ch_client_var.reset(ch_token)
        await engine.dispose()
        await pg_engine.dispose()


@asynccontextmanager
async def task_scope(task_id: int, job_id: int) -> AsyncIterator[None]:
    """Per-task context nested inside orch_context.

    Creates isolated per-task state:
    - Fresh objects registry for stale-marking on exit
    - _OrchLifecycleView with new snowflake context_id for distributed refcounting
    - OplogCollector for operation lineage (always active in orch mode)

    Oplog is flushed on clean exit; discarded on exception.
    All tracked objects are stale-marked on exit.

    Args:
        task_id: ID of the current task (for oplog).
        job_id: ID of the job (for pin/claim lifecycle ownership).
    """
    state = _get_orch_state()
    context_id = get_snowflake_id()

    lifecycle = _OrchLifecycleView(
        pg_engine=state.pg_engine,
        ch_client=state.ch_client,
        context_id=context_id,
        job_id=job_id,
    )
    await lifecycle.start()

    objects: Dict[int, weakref.ref] = {}
    collector = OplogCollector(task_id=task_id, job_id=job_id)
    await init_oplog_tables(state.ch_client)

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


def get_db_handler(ctx: str = "default") -> DbHandler:
    """Get the database handler from the active orchestration context."""
    return _get_orch_state(ctx).db_handler


@asynccontextmanager
async def get_orch_session(ctx: str = "default") -> AsyncIterator[AsyncSession]:
    """Get a database session from the active orchestration context.

    Yields:
        AsyncSession configured with expire_on_commit=False
    """
    state = _get_orch_state(ctx)
    async with AsyncSession(state.engine, expire_on_commit=False) as session:
        yield session


async def commit_tasks(
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
