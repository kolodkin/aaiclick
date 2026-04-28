"""TaskLifecycleHandler — orch-aware LifecycleHandler for task_scope.

Replaces the deleted-in-Phase-6 ``OrchLifecycleHandler``. Differences from
``LocalLifecycleHandler``:

- No ``AsyncTableWorker`` / refcount machinery. Within a task_scope, tables
  live until ``__aexit__`` and the cleanup decision is purely flag-based:
  drop a tracked table when ``owned=True``, ``pinned=False``, and
  ``preserved=False`` (success) or it is a ``t_*`` scratch table (failure).
- Owns the SQL-side writes the prior orch handler did: ``register_table``
  inserts a ``table_registry`` row including ``schema_doc`` (read by
  ``_get_table_schema``); ``pin`` fans out one ``table_pin_refs`` row per
  downstream consumer task; ``oplog_record`` writes to ``operation_log``.
- ``current_job_id`` exposes the job id for ``create_object_from_value(scope="job")``.

A single asyncio.Queue + processor task serialises SQL writes so sync
callers (``Object.__del__``, ``register_table`` from within a sync helper)
can enqueue without awaiting.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from datetime import datetime, timezone

from sqlalchemy import text

from aaiclick.data.data_context.ch_client import ChClient, get_ch_client
from aaiclick.data.data_context.lifecycle import LifecycleHandler, TrackedTable
from aaiclick.oplog.models import OPERATION_LOG_EXPECTED_COLUMNS

from ..sql_context import get_sql_session
from .db_lifecycle import (
    DBLifecycleMessage,
    DBLifecycleOp,
    OplogPayload,
    OplogTablePayload,
)

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


class TaskLifecycleHandler(LifecycleHandler):
    def __init__(
        self,
        *,
        task_id: int,
        job_id: int,
        run_id: int,
        ch_client: ChClient,
    ):
        self._task_id = task_id
        self._job_id = job_id
        self._run_id = run_id
        self._ch_client = ch_client
        self._tracked: dict[str, TrackedTable] = {}
        self._queue: asyncio.Queue[DBLifecycleMessage] = asyncio.Queue()
        self._process_task: asyncio.Task | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._process_task = asyncio.create_task(self._process_loop())

    async def stop(self) -> None:
        self._enqueue(DBLifecycleMessage(DBLifecycleOp.SHUTDOWN))
        if self._process_task is not None:
            await self._process_task

    async def flush(self) -> None:
        event = asyncio.Event()
        self._enqueue(DBLifecycleMessage(DBLifecycleOp.FLUSH, flush_event=event))
        await event.wait()

    def current_job_id(self) -> int | None:
        return self._job_id

    def incref(self, table_name: str) -> None:
        """Cross-task input read. Track but don't claim ownership."""
        self.track_table(table_name, owned=False)

    def decref(self, table_name: str) -> None:
        """No-op: cleanup is decided by flags at task_scope exit, not refcounts."""
        return

    def track_table(self, table_name: str, *, preserved: bool = False, owned: bool = False) -> None:
        existing = self._tracked.get(table_name)
        if existing is None:
            self._tracked[table_name] = TrackedTable(table_name, preserved, False, owned)
            return
        upgraded = existing
        if preserved and not existing.preserved:
            upgraded = upgraded._replace(preserved=True)
        if owned and not existing.owned:
            upgraded = upgraded._replace(owned=True)
        if upgraded is not existing:
            self._tracked[table_name] = upgraded

    def mark_pinned(self, table_name: str) -> None:
        existing = self._tracked.get(table_name)
        if existing is not None and not existing.pinned:
            self._tracked[table_name] = existing._replace(pinned=True)

    def iter_tracked_tables(self) -> Iterable[TrackedTable]:
        return iter(list(self._tracked.values()))

    def register_table(self, table_name: str, schema_doc: str | None = None) -> None:
        self.track_table(table_name, owned=True)
        self._enqueue(
            DBLifecycleMessage(
                DBLifecycleOp.OPLOG_TABLE,
                oplog_table=OplogTablePayload(
                    table_name=table_name,
                    task_id=self._task_id,
                    job_id=self._job_id,
                    run_id=self._run_id,
                    schema_doc=schema_doc,
                ),
            )
        )

    def pin(self, table_name: str) -> None:
        self.mark_pinned(table_name)
        self._enqueue(
            DBLifecycleMessage(
                DBLifecycleOp.PIN,
                table_name=table_name,
                pin_task_id=self._task_id,
            )
        )

    def unpin(self, table_name: str) -> None:
        self._enqueue(
            DBLifecycleMessage(
                DBLifecycleOp.UNPIN,
                table_name=table_name,
                pin_task_id=self._task_id,
            )
        )

    def oplog_record(
        self,
        result_table: str,
        operation: str,
        kwargs: dict[str, str] | None = None,
        sql: str | None = None,
    ) -> None:
        self._enqueue(
            DBLifecycleMessage(
                DBLifecycleOp.OPLOG_RECORD,
                oplog=OplogPayload(
                    result_table=result_table,
                    operation=operation,
                    kwargs=kwargs or {},
                    sql=sql,
                    task_id=self._task_id,
                    job_id=self._job_id,
                    run_id=self._run_id,
                ),
            )
        )

    def oplog_record_sample(
        self,
        result_table: str,
        operation: str,
        kwargs: dict[str, str] | None = None,
        sql: str | None = None,
    ) -> None:
        self.oplog_record(result_table, operation, kwargs, sql)

    def _enqueue(self, msg: DBLifecycleMessage) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, msg)

    async def _write_oplog_row(self, p: OplogPayload) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        try:
            await self._ch_client.insert(
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
        while True:
            msg = await self._queue.get()
            if msg.op == DBLifecycleOp.SHUTDOWN:
                break
            if msg.op == DBLifecycleOp.PIN:
                async with get_sql_session() as session:
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
                    await session.commit()
            elif msg.op == DBLifecycleOp.UNPIN:
                async with get_sql_session() as session:
                    await session.execute(
                        text(
                            "DELETE FROM table_pin_refs "
                            "WHERE table_name = :table_name AND task_id = :task_id"
                        ),
                        {"table_name": msg.table_name, "task_id": msg.pin_task_id},
                    )
                    await session.commit()
            elif msg.op == DBLifecycleOp.OPLOG_RECORD:
                assert msg.oplog is not None
                await self._write_oplog_row(msg.oplog)
            elif msg.op == DBLifecycleOp.OPLOG_TABLE:
                assert msg.oplog_table is not None
                await self._write_table_registry_row(msg.oplog_table)
            elif msg.op == DBLifecycleOp.FLUSH:
                assert msg.flush_event is not None
                msg.flush_event.set()
