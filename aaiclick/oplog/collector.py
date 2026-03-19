"""
aaiclick.oplog.collector - OplogCollector for buffering and flushing operation events.
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone

from aaiclick.data.data_context import get_ch_client


_oplog_collector: ContextVar[OplogCollector | None] = ContextVar(
    "oplog_collector", default=None
)


def get_oplog_collector() -> OplogCollector | None:
    """Return the active OplogCollector, or None if oplog is disabled."""
    return _oplog_collector.get()


@dataclass
class OperationEvent:
    result_table: str
    operation: str
    args: list[str] = field(default_factory=list)
    kwargs: dict[str, str] = field(default_factory=dict)
    sql: str | None = None


class OplogCollector:
    """Collects operation events during a data_context session.

    Buffer-based: events are held in memory and batch-inserted into
    ClickHouse on successful context exit. On failure, the buffer is
    discarded to avoid partial oplog entries.
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
        args: list[str] | None = None,
        kwargs: dict[str, str] | None = None,
        sql: str | None = None,
    ) -> None:
        """Buffer an operation event (synchronous, zero I/O overhead)."""
        self._buffer.append(
            OperationEvent(
                result_table=result_table,
                operation=operation,
                args=args or [],
                kwargs=kwargs or {},
                sql=sql,
            )
        )

    def record_table(self, table_name: str) -> None:
        """Register a newly created table in the table_registry buffer."""
        self._table_buffer.append(table_name)

    async def flush(self) -> None:
        """Batch-insert buffered events into ClickHouse operation_log and table_registry."""
        if not self._buffer and not self._table_buffer:
            return

        ch_client = get_ch_client()
        now = datetime.now(timezone.utc)

        if self._buffer:
            rows = [
                [
                    ev.result_table,
                    ev.operation,
                    ev.args,
                    ev.kwargs,
                    ev.sql,
                    self.task_id,
                    self.job_id,
                    now,
                ]
                for ev in self._buffer
            ]
            await ch_client.insert(
                "operation_log",
                rows,
                column_names=[
                    "result_table", "operation", "args", "kwargs",
                    "sql_template", "task_id", "job_id", "created_at",
                ],
            )

        if self._table_buffer:
            table_rows = [
                [tbl, self.job_id, self.task_id, now]
                for tbl in self._table_buffer
            ]
            await ch_client.insert(
                "table_registry",
                table_rows,
                column_names=["table_name", "job_id", "task_id", "created_at"],
            )
