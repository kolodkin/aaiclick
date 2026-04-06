"""
aaiclick.oplog.collector - OplogCollector for buffering and flushing operation events.

Buffers operation events in memory during a task_scope session. On flush,
runs lineage sampling queries for operations that requested it, then
batch-inserts all entries to ClickHouse.

Only active in orchestration mode (task_scope sets the ContextVar).
In local data_context mode, the ContextVar is None and all calls are no-ops.
"""

from __future__ import annotations

import os
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone

from aaiclick.oplog.models import OPERATION_LOG_EXPECTED_COLUMNS, TABLE_REGISTRY_EXPECTED_COLUMNS

_OPLOG_COLS = ["result_table", "operation", "kwargs", "kwargs_aai_ids",
               "result_aai_ids", "sql_template", "task_id", "job_id", "created_at"]
_OPLOG_TYPE_NAMES = [OPERATION_LOG_EXPECTED_COLUMNS[c] for c in _OPLOG_COLS]

_REG_COLS = ["table_name", "job_id", "task_id", "created_at"]
_REG_TYPE_NAMES = [TABLE_REGISTRY_EXPECTED_COLUMNS[c] for c in _REG_COLS]


def _sample_size() -> int:
    return int(os.environ.get("AAICLICK_OPLOG_SAMPLE_SIZE", "10"))


@dataclass
class OperationEvent:
    result_table: str
    operation: str
    kwargs: dict[str, str] = field(default_factory=dict)
    kwargs_aai_ids: dict[str, list[int]] = field(default_factory=dict)
    result_aai_ids: list[int] = field(default_factory=list)
    sql: str | None = None
    needs_sampling: bool = False


class OplogCollector:
    """Collects operation events during a task_scope session.

    Buffer-based: events are held in memory and batch-inserted into
    ClickHouse on successful context exit via flush(). On failure,
    the buffer is discarded to avoid partial oplog entries.

    Source tables are guaranteed alive at flush time because flush runs
    inside task_scope's finally block, before BackgroundWorker polls
    for unreferenced tables.
    """

    def __init__(
        self,
        task_id: int | None = None,
        job_id: int | None = None,
    ) -> None:
        self._buffer: list[OperationEvent] = []
        self._table_buffer: list[str] = []
        self.task_id = task_id
        self.job_id = job_id

    def record(
        self,
        result_table: str,
        operation: str,
        kwargs: dict[str, str] | None = None,
        sql: str | None = None,
    ) -> None:
        """Buffer an operation event (synchronous, zero I/O overhead)."""
        self._buffer.append(
            OperationEvent(
                result_table=result_table,
                operation=operation,
                kwargs=kwargs or {},
                sql=sql,
            )
        )

    def record_sample(
        self,
        result_table: str,
        operation: str,
        kwargs: dict[str, str] | None = None,
        sql: str | None = None,
    ) -> None:
        """Buffer an operation event that needs lineage sampling at flush time."""
        self._buffer.append(
            OperationEvent(
                result_table=result_table,
                operation=operation,
                kwargs=kwargs or {},
                sql=sql,
                needs_sampling=True,
            )
        )

    def record_table(self, table_name: str) -> None:
        """Register a newly created table in the table_registry buffer."""
        self._table_buffer.append(table_name)

    async def flush(self) -> None:
        """Run lineage sampling, then batch-insert all entries to ClickHouse."""
        if not self._buffer and not self._table_buffer:
            return

        from aaiclick.data.data_context import get_ch_client
        ch_client = get_ch_client()
        now = datetime.now(timezone.utc)

        n = _sample_size()
        for ev in self._buffer:
            if ev.needs_sampling and ev.kwargs:
                try:
                    ev.kwargs_aai_ids, ev.result_aai_ids = await _sample_lineage(
                        ch_client, ev.result_table, ev.kwargs, n,
                    )
                except Exception:
                    pass  # Best effort

        if self._buffer:
            rows = [
                [
                    ev.result_table, ev.operation, ev.kwargs,
                    ev.kwargs_aai_ids, ev.result_aai_ids,
                    ev.sql, self.task_id, self.job_id, now,
                ]
                for ev in self._buffer
            ]
            await ch_client.insert(
                "operation_log", rows,
                column_names=_OPLOG_COLS, column_type_names=_OPLOG_TYPE_NAMES,
            )

        if self._table_buffer:
            table_rows = [
                [tbl, self.job_id, self.task_id, now]
                for tbl in self._table_buffer
            ]
            await ch_client.insert(
                "table_registry", table_rows,
                column_names=_REG_COLS, column_type_names=_REG_TYPE_NAMES,
            )

    def discard(self) -> None:
        """Discard buffered entries without flushing (on error)."""
        self._buffer.clear()
        self._table_buffer.clear()


# -- Lineage sampling --

async def _sample_lineage(ch_client, result_table, kwargs, n):
    sources = list(kwargs.values())
    roles = list(kwargs.keys())
    if len(sources) == 1:
        return await _sample_unary(ch_client, result_table, roles[0], sources[0], n)
    elif len(sources) == 2:
        return await _sample_binary(
            ch_client, result_table, roles[0], sources[0], roles[1], sources[1], n,
        )
    else:
        return await _sample_nary(ch_client, result_table, roles, sources, n)


async def _sample_unary(ch_client, result_table, role, source_table, n):
    source_ids = await _pick_aai_ids(ch_client, source_table, n)
    if not source_ids:
        return {}, []
    ids_list = ", ".join(str(i) for i in source_ids)
    result = await ch_client.query(f"""
        SELECT s.aai_id, r.aai_id
        FROM (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn FROM {source_table}) s
        INNER JOIN (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn FROM {result_table}) r
        ON s.rn = r.rn WHERE s.aai_id IN ({ids_list})
    """)
    return {role: [r[0] for r in result.result_rows]}, [r[1] for r in result.result_rows]


async def _sample_binary(ch_client, result_table, role_a, table_a, role_b, table_b, n):
    a_ids = await _pick_aai_ids(ch_client, table_a, n)
    if not a_ids:
        return {}, []
    ids_list = ", ".join(str(i) for i in a_ids)
    result = await ch_client.query(f"""
        SELECT a.aai_id, b.aai_id, r.aai_id
        FROM (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn FROM {table_a}) a
        INNER JOIN (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn FROM {table_b}) b ON a.rn = b.rn
        INNER JOIN (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn FROM {result_table}) r ON a.rn = r.rn
        WHERE a.aai_id IN ({ids_list})
    """)
    rows = result.result_rows
    return {role_a: [r[0] for r in rows], role_b: [r[1] for r in rows]}, [r[2] for r in rows]


async def _sample_nary(ch_client, result_table, roles, source_tables, n):
    result = await ch_client.query(
        f"SELECT aai_id FROM {result_table} ORDER BY rand() LIMIT {n}"
    )
    result_ids = [r[0] for r in result.result_rows]
    if not result_ids:
        return {}, []
    kwargs_aai_ids = {}
    for role, src in zip(roles, source_tables):
        ids = await _pick_aai_ids(ch_client, src, n)
        if ids:
            kwargs_aai_ids[role] = ids
    return kwargs_aai_ids, result_ids


async def _pick_aai_ids(ch_client, table, n):
    try:
        result = await ch_client.query(f"""
            SELECT aai_id FROM {table}
            WHERE aai_id IN (
                SELECT arrayJoin(result_aai_ids) FROM operation_log
                WHERE result_table = '{table}'
            ) LIMIT {n}
        """)
        known = [r[0] for r in result.result_rows]
    except Exception:
        known = []
    if len(known) >= n:
        return known[:n]
    remaining = n - len(known)
    exclude = ", ".join(str(i) for i in known) if known else "0"
    try:
        result = await ch_client.query(f"""
            SELECT aai_id FROM {table}
            WHERE aai_id NOT IN ({exclude})
            ORDER BY rand() LIMIT {remaining}
        """)
        return known + [r[0] for r in result.result_rows]
    except Exception:
        return known


_oplog_collector: ContextVar[OplogCollector | None] = ContextVar(
    "oplog_collector", default=None
)


def get_oplog_collector() -> OplogCollector | None:
    """Return the active OplogCollector, or None if oplog is disabled."""
    return _oplog_collector.get()


def oplog_record(
    result_table: str,
    operation: str,
    kwargs: dict[str, str] | None = None,
    sql: str | None = None,
) -> None:
    """Record an operation if an OplogCollector is active in the current context."""
    collector = _oplog_collector.get()
    if collector is not None:
        collector.record(result_table, operation, kwargs=kwargs, sql=sql)


def oplog_record_sample(
    result_table: str,
    operation: str,
    kwargs: dict[str, str] | None = None,
    sql: str | None = None,
) -> None:
    """Record an operation with lineage sampling if an OplogCollector is active."""
    collector = _oplog_collector.get()
    if collector is not None:
        collector.record_sample(result_table, operation, kwargs=kwargs, sql=sql)


def oplog_record_table(table_name: str) -> None:
    """Register a newly created table if an OplogCollector is active in the current context."""
    collector = _oplog_collector.get()
    if collector is not None:
        collector.record_table(table_name)
