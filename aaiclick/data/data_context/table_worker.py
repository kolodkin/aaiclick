"""
aaiclick.data.table_worker - Async worker for table lifecycle and oplog management.

This module provides an async task that manages table reference counting,
automatic cleanup when tables are no longer referenced, and operation log
buffering with lineage sampling.

The single FIFO queue guarantees that OPLOG_SAMPLE messages are processed
before subsequent DECREF messages — source tables are still alive when
the sampling query runs.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto

from aaiclick.oplog.models import OPERATION_LOG_EXPECTED_COLUMNS, TABLE_REGISTRY_EXPECTED_COLUMNS

from .ch_client import ChClient

_OPLOG_COLS = ["result_table", "operation", "kwargs", "kwargs_aai_ids",
               "result_aai_ids", "sql_template", "task_id", "job_id", "created_at"]
_OPLOG_TYPE_NAMES = [OPERATION_LOG_EXPECTED_COLUMNS[c] for c in _OPLOG_COLS]

_REG_COLS = ["table_name", "job_id", "task_id", "created_at"]
_REG_TYPE_NAMES = [TABLE_REGISTRY_EXPECTED_COLUMNS[c] for c in _REG_COLS]


def _sample_size() -> int:
    return int(os.environ.get("AAICLICK_OPLOG_SAMPLE_SIZE", "10"))


class TableOp(Enum):
    """Operations for the table worker."""

    INCREF = auto()
    DECREF = auto()
    OPLOG_RECORD = auto()
    OPLOG_SAMPLE = auto()
    OPLOG_TABLE = auto()
    OPLOG_FLUSH = auto()
    FLUSH = auto()
    SHUTDOWN = auto()


@dataclass
class OplogSampleContext:
    """Context for a lineage sampling request."""

    result_table: str
    operation: str
    kwargs: dict[str, str] = field(default_factory=dict)
    sql: str | None = None
    task_id: int | None = None
    job_id: int | None = None


@dataclass
class OplogRecordContext:
    """Context for a plain oplog record (no sampling needed)."""

    result_table: str
    operation: str
    kwargs: dict[str, str] = field(default_factory=dict)
    sql: str | None = None
    task_id: int | None = None
    job_id: int | None = None


@dataclass
class OplogTableContext:
    """Context for a table registry record."""

    table_name: str
    task_id: int | None = None
    job_id: int | None = None


@dataclass
class OplogBufferEntry:
    """Buffered oplog entry ready for flush."""

    result_table: str
    operation: str
    kwargs: dict[str, str]
    kwargs_aai_ids: dict[str, list[int]]
    result_aai_ids: list[int]
    sql: str | None
    task_id: int | None
    job_id: int | None


@dataclass
class TableMessage:
    """Message passed to worker via queue."""

    op: TableOp
    table_name: str = ""
    event: asyncio.Event | None = None
    oplog_sample: OplogSampleContext | None = None
    oplog_record: OplogRecordContext | None = None
    oplog_table: OplogTableContext | None = None


class AsyncTableWorker:
    """Async worker that manages table lifecycle and oplog via a single FIFO queue.

    Runs as an asyncio Task in the main event loop. Uses the shared async
    ChClient for DROP TABLE and lineage sampling queries.

    The single queue guarantees ordering: OPLOG_SAMPLE messages are always
    processed before DECREF messages for the same tables, ensuring source
    tables are alive when the sampling query runs.

    incref/decref/oplog_enqueue are sync (safe to call from __del__) and
    schedule work onto the event loop via call_soon_threadsafe.
    """

    def __init__(self, ch_client: ChClient):
        self._ch_client = ch_client
        self._queue: asyncio.Queue[TableMessage] = asyncio.Queue()
        self._task: asyncio.Task | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._refcounts: dict[str, int] = {}
        self._oplog_buffer: list[OplogBufferEntry] = []
        self._table_buffer: list[OplogTableContext] = []

    async def start(self) -> None:
        """Start the worker task in the running event loop."""
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Send SHUTDOWN and wait for the worker task to finish.

        SHUTDOWN is scheduled via call_soon_threadsafe to guarantee ordering
        after any pending incref/decref messages that were also scheduled via
        call_soon_threadsafe.
        """
        if self._loop is not None:
            self._loop.call_soon_threadsafe(
                self._queue.put_nowait, TableMessage(TableOp.SHUTDOWN)
            )
        if self._task is not None:
            await self._task

    def incref(self, table_name: str) -> None:
        """Increment reference count for table. Non-blocking, safe from any thread."""
        if self._loop is not None:
            self._loop.call_soon_threadsafe(
                self._queue.put_nowait, TableMessage(TableOp.INCREF, table_name)
            )

    def decref(self, table_name: str) -> None:
        """Decrement reference count for table. Non-blocking, safe from any thread."""
        if self._loop is not None:
            self._loop.call_soon_threadsafe(
                self._queue.put_nowait, TableMessage(TableOp.DECREF, table_name)
            )

    def enqueue_oplog_sample(self, ctx: OplogSampleContext) -> None:
        """Enqueue a lineage sampling request. Non-blocking."""
        if self._loop is not None:
            self._loop.call_soon_threadsafe(
                self._queue.put_nowait,
                TableMessage(TableOp.OPLOG_SAMPLE, oplog_sample=ctx),
            )

    def enqueue_oplog_record(self, ctx: OplogRecordContext) -> None:
        """Enqueue a plain oplog record (no sampling). Non-blocking."""
        if self._loop is not None:
            self._loop.call_soon_threadsafe(
                self._queue.put_nowait,
                TableMessage(TableOp.OPLOG_RECORD, oplog_record=ctx),
            )

    def enqueue_oplog_table(self, ctx: OplogTableContext) -> None:
        """Enqueue a table registry record. Non-blocking."""
        if self._loop is not None:
            self._loop.call_soon_threadsafe(
                self._queue.put_nowait,
                TableMessage(TableOp.OPLOG_TABLE, oplog_table=ctx),
            )

    async def flush(self) -> None:
        """Wait until all pending messages have been processed.

        Enqueues a FLUSH sentinel and awaits its event. Since the worker
        processes the queue in order, the event fires only after every
        message that was already in the queue has been handled — meaning
        all pending DROP TABLEs and oplog samples have completed.
        """
        if self._loop is None:
            return
        event = asyncio.Event()
        self._loop.call_soon_threadsafe(
            self._queue.put_nowait, TableMessage(TableOp.FLUSH, event=event)
        )
        await event.wait()

    async def flush_oplog(self) -> None:
        """Flush buffered oplog entries to ClickHouse.

        Enqueues an OPLOG_FLUSH sentinel and awaits its event. Processes
        all pending messages first (including any OPLOG_SAMPLE), then
        batch-inserts the buffer.
        """
        if self._loop is None:
            return
        event = asyncio.Event()
        self._loop.call_soon_threadsafe(
            self._queue.put_nowait, TableMessage(TableOp.OPLOG_FLUSH, event=event)
        )
        await event.wait()

    def discard_oplog(self) -> None:
        """Discard buffered oplog entries without flushing (on error)."""
        self._oplog_buffer.clear()
        self._table_buffer.clear()

    async def _run(self) -> None:
        """Worker loop — runs as an asyncio Task."""
        while True:
            msg = await self._queue.get()

            if msg.op == TableOp.SHUTDOWN:
                await self._cleanup_all()
                break

            elif msg.op == TableOp.FLUSH:
                if msg.event is not None:
                    msg.event.set()
                continue

            elif msg.op == TableOp.OPLOG_FLUSH:
                await self._flush_oplog_buffer()
                if msg.event is not None:
                    msg.event.set()
                continue

            elif msg.op == TableOp.INCREF:
                self._refcounts[msg.table_name] = (
                    self._refcounts.get(msg.table_name, 0) + 1
                )

            elif msg.op == TableOp.DECREF:
                if msg.table_name in self._refcounts:
                    self._refcounts[msg.table_name] -= 1
                    if self._refcounts[msg.table_name] <= 0:
                        if not msg.table_name.startswith("p_"):
                            await self._drop_table(msg.table_name)
                        del self._refcounts[msg.table_name]

            elif msg.op == TableOp.OPLOG_SAMPLE:
                await self._process_oplog_sample(msg.oplog_sample)

            elif msg.op == TableOp.OPLOG_RECORD:
                self._process_oplog_record(msg.oplog_record)

            elif msg.op == TableOp.OPLOG_TABLE:
                self._table_buffer.append(msg.oplog_table)

    async def _process_oplog_sample(self, ctx: OplogSampleContext) -> None:
        """Run lineage sampling query and buffer the result."""
        n = _sample_size()
        kwargs_aai_ids: dict[str, list[int]] = {}
        result_aai_ids: list[int] = []

        try:
            source_tables = list(ctx.kwargs.values())
            source_roles = list(ctx.kwargs.keys())

            if len(source_tables) == 0:
                pass
            elif len(source_tables) == 1:
                kwargs_aai_ids, result_aai_ids = await self._sample_unary(
                    ctx.result_table, source_roles[0], source_tables[0], n,
                )
            elif len(source_tables) == 2:
                kwargs_aai_ids, result_aai_ids = await self._sample_binary(
                    ctx.result_table,
                    source_roles[0], source_tables[0],
                    source_roles[1], source_tables[1],
                    n,
                )
            else:
                kwargs_aai_ids, result_aai_ids = await self._sample_nary(
                    ctx.result_table, source_roles, source_tables, n,
                )
        except Exception:
            pass  # Best effort — don't block operations on lineage failure

        self._oplog_buffer.append(OplogBufferEntry(
            result_table=ctx.result_table,
            operation=ctx.operation,
            kwargs=ctx.kwargs,
            kwargs_aai_ids=kwargs_aai_ids,
            result_aai_ids=result_aai_ids,
            sql=ctx.sql,
            task_id=ctx.task_id,
            job_id=ctx.job_id,
        ))

    def _process_oplog_record(self, ctx: OplogRecordContext) -> None:
        """Buffer a plain oplog record (no lineage sampling)."""
        self._oplog_buffer.append(OplogBufferEntry(
            result_table=ctx.result_table,
            operation=ctx.operation,
            kwargs=ctx.kwargs,
            kwargs_aai_ids={},
            result_aai_ids=[],
            sql=ctx.sql,
            task_id=ctx.task_id,
            job_id=ctx.job_id,
        ))

    async def _sample_unary(
        self,
        result_table: str,
        role: str,
        source_table: str,
        n: int,
    ) -> tuple[dict[str, list[int]], list[int]]:
        """Sample lineage for unary ops (aggregation, copy, etc.)."""
        source_ids = await self._pick_aai_ids(source_table, n)
        if not source_ids:
            return {}, []

        # Find corresponding result aai_ids by position
        ids_list = ", ".join(str(i) for i in source_ids)
        result = await self._ch_client.query(f"""
            SELECT s.aai_id, r.aai_id
            FROM (
                SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn
                FROM {source_table}
            ) s
            INNER JOIN (
                SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn
                FROM {result_table}
            ) r ON s.rn = r.rn
            WHERE s.aai_id IN ({ids_list})
        """)

        src_ids = [row[0] for row in result.result_rows]
        res_ids = [row[1] for row in result.result_rows]
        return {role: src_ids}, res_ids

    async def _sample_binary(
        self,
        result_table: str,
        role_a: str, table_a: str,
        role_b: str, table_b: str,
        n: int,
    ) -> tuple[dict[str, list[int]], list[int]]:
        """Sample lineage for binary ops (add, sub, etc.)."""
        # Pick aai_ids from the left source preferring lineage-connected ones
        a_ids = await self._pick_aai_ids(table_a, n)
        if not a_ids:
            return {}, []

        ids_list = ", ".join(str(i) for i in a_ids)
        result = await self._ch_client.query(f"""
            SELECT a.aai_id, b.aai_id, r.aai_id
            FROM (
                SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn
                FROM {table_a}
            ) a
            INNER JOIN (
                SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn
                FROM {table_b}
            ) b ON a.rn = b.rn
            INNER JOIN (
                SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn
                FROM {result_table}
            ) r ON a.rn = r.rn
            WHERE a.aai_id IN ({ids_list})
        """)

        a_sampled = [row[0] for row in result.result_rows]
        b_sampled = [row[1] for row in result.result_rows]
        r_sampled = [row[2] for row in result.result_rows]
        return {role_a: a_sampled, role_b: b_sampled}, r_sampled

    async def _sample_nary(
        self,
        result_table: str,
        roles: list[str],
        source_tables: list[str],
        n: int,
    ) -> tuple[dict[str, list[int]], list[int]]:
        """Sample lineage for n-ary ops (concat with 3+ sources)."""
        # For concat, rows are appended sequentially — sample from result
        result = await self._ch_client.query(f"""
            SELECT aai_id FROM {result_table}
            ORDER BY rand() LIMIT {n}
        """)
        result_ids = [row[0] for row in result.result_rows]
        if not result_ids:
            return {}, []

        # For each source, sample independently
        kwargs_aai_ids: dict[str, list[int]] = {}
        for role, source_table in zip(roles, source_tables):
            src_ids = await self._pick_aai_ids(source_table, n)
            if src_ids:
                kwargs_aai_ids[role] = src_ids

        return kwargs_aai_ids, result_ids

    async def _pick_aai_ids(self, table: str, n: int) -> list[int]:
        """Pick N aai_ids from a table, preferring those already in oplog lineage."""
        # First: try aai_ids already in oplog (connected lineage chains)
        try:
            result = await self._ch_client.query(f"""
                SELECT aai_id FROM {table}
                WHERE aai_id IN (
                    SELECT arrayJoin(result_aai_ids)
                    FROM operation_log
                    WHERE result_table = '{table}'
                )
                LIMIT {n}
            """)
            known_ids = [row[0] for row in result.result_rows]
        except Exception:
            known_ids = []

        if len(known_ids) >= n:
            return known_ids[:n]

        # Fill remaining with random
        remaining = n - len(known_ids)
        exclude = ", ".join(str(i) for i in known_ids) if known_ids else "0"
        try:
            result = await self._ch_client.query(f"""
                SELECT aai_id FROM {table}
                WHERE aai_id NOT IN ({exclude})
                ORDER BY rand() LIMIT {remaining}
            """)
            random_ids = [row[0] for row in result.result_rows]
        except Exception:
            random_ids = []

        return known_ids + random_ids

    async def _flush_oplog_buffer(self) -> None:
        """Batch-insert buffered oplog entries to ClickHouse."""
        now = datetime.now(timezone.utc)

        if self._oplog_buffer:
            rows = [
                [
                    e.result_table,
                    e.operation,
                    e.kwargs,
                    e.kwargs_aai_ids,
                    e.result_aai_ids,
                    e.sql,
                    e.task_id,
                    e.job_id,
                    now,
                ]
                for e in self._oplog_buffer
            ]
            try:
                await self._ch_client.insert(
                    "operation_log",
                    rows,
                    column_names=_OPLOG_COLS,
                    column_type_names=_OPLOG_TYPE_NAMES,
                )
            except Exception:
                pass  # Best effort
            self._oplog_buffer.clear()

        if self._table_buffer:
            table_rows = [
                [ctx.table_name, ctx.job_id, ctx.task_id, now]
                for ctx in self._table_buffer
            ]
            try:
                await self._ch_client.insert(
                    "table_registry",
                    table_rows,
                    column_names=_REG_COLS,
                    column_type_names=_REG_TYPE_NAMES,
                )
            except Exception:
                pass  # Best effort
            self._table_buffer.clear()

    async def _drop_table(self, table_name: str) -> None:
        """Drop single table. Best effort."""
        try:
            await self._ch_client.command(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass  # Best effort - table may already be gone

    async def _cleanup_all(self) -> None:
        """Drop all remaining tables on shutdown. Skips persistent (p_) tables."""
        for table_name in list(self._refcounts.keys()):
            if not table_name.startswith("p_"):
                await self._drop_table(table_name)
        self._refcounts.clear()
