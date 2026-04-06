"""
aaiclick.oplog.collector - Oplog recording via the lifecycle handler.

The public API (oplog_record, oplog_record_sample, oplog_record_table)
delegates to the active LifecycleHandler which enqueues messages on the
same FIFO queue as incref/decref. This guarantees lineage sampling runs
while source tables are still alive.

In local data_context mode (no orchestration), the lifecycle handler's
oplog methods are no-ops — lineage is only captured in orchestration mode.
"""

from __future__ import annotations

from contextvars import ContextVar


class OplogCollector:
    """Per-task oplog context that delegates to the lifecycle handler.

    Holds task_id/job_id and routes events to the active LifecycleHandler.
    Used by task_scope() to inject orchestration metadata into oplog entries.
    """

    def __init__(
        self,
        task_id: int | None = None,
        job_id: int | None = None,
    ) -> None:
        self.task_id = task_id
        self.job_id = job_id

    def _lifecycle(self):
        from aaiclick.data.data_context.lifecycle import get_data_lifecycle
        return get_data_lifecycle()

    def record(
        self,
        result_table: str,
        operation: str,
        kwargs: dict[str, str] | None = None,
        sql: str | None = None,
    ) -> None:
        """Route an operation record to the lifecycle handler's queue."""
        lc = self._lifecycle()
        if lc is not None:
            lc.oplog_record(
                result_table, operation, kwargs=kwargs, sql=sql,
                task_id=self.task_id, job_id=self.job_id,
            )

    def record_sample(
        self,
        result_table: str,
        operation: str,
        kwargs: dict[str, str] | None = None,
        sql: str | None = None,
    ) -> None:
        """Route a lineage sampling request to the lifecycle handler's queue."""
        lc = self._lifecycle()
        if lc is not None:
            lc.oplog_record_sample(
                result_table, operation, kwargs=kwargs, sql=sql,
                task_id=self.task_id, job_id=self.job_id,
            )

    def record_table(self, table_name: str) -> None:
        """Route a table registry record to the lifecycle handler's queue."""
        lc = self._lifecycle()
        if lc is not None:
            lc.oplog_record_table(
                table_name, task_id=self.task_id, job_id=self.job_id,
            )


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
