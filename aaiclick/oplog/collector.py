"""
aaiclick.oplog.collector - Oplog recording via the unified table worker.

The public API (oplog_record, oplog_record_table) routes operation events
through the LifecycleHandler's worker queue, ensuring lineage sampling
runs before DECREF drops source tables.

OplogCollector is retained for orchestration mode (task_scope) where
task_id/job_id context is needed per-task.
"""

from __future__ import annotations

from contextvars import ContextVar


def _get_lifecycle():
    """Lazy import to break circular dependency."""
    from aaiclick.data.data_context.lifecycle import get_data_lifecycle
    return get_data_lifecycle()


def _make_record_ctx(result_table, operation, kwargs, sql, task_id, job_id):
    """Lazy import to break circular dependency."""
    from aaiclick.data.data_context.table_worker import OplogRecordContext
    return OplogRecordContext(
        result_table=result_table, operation=operation,
        kwargs=kwargs, sql=sql, task_id=task_id, job_id=job_id,
    )


def _make_sample_ctx(result_table, operation, kwargs, sql, task_id, job_id):
    """Lazy import to break circular dependency."""
    from aaiclick.data.data_context.table_worker import OplogSampleContext
    return OplogSampleContext(
        result_table=result_table, operation=operation,
        kwargs=kwargs, sql=sql, task_id=task_id, job_id=job_id,
    )


def _make_table_ctx(table_name, task_id, job_id):
    """Lazy import to break circular dependency."""
    from aaiclick.data.data_context.table_worker import OplogTableContext
    return OplogTableContext(
        table_name=table_name, task_id=task_id, job_id=job_id,
    )


class OplogCollector:
    """Per-task oplog context that routes events to the lifecycle worker.

    Holds task_id/job_id and delegates to the LifecycleHandler. Used by
    task_scope() to inject orchestration metadata into oplog entries.
    """

    def __init__(
        self,
        task_id: int | None = None,
        job_id: int | None = None,
    ) -> None:
        self.task_id = task_id
        self.job_id = job_id

    def record(
        self,
        result_table: str,
        operation: str,
        kwargs: dict[str, str] | None = None,
        sql: str | None = None,
    ) -> None:
        """Route an operation record to the lifecycle worker."""
        lifecycle = _get_lifecycle()
        if lifecycle is None:
            return
        lifecycle.enqueue_oplog_record(_make_record_ctx(
            result_table, operation, kwargs or {}, sql,
            self.task_id, self.job_id,
        ))

    def record_sample(
        self,
        result_table: str,
        operation: str,
        kwargs: dict[str, str] | None = None,
        sql: str | None = None,
    ) -> None:
        """Route a lineage sampling request to the lifecycle worker."""
        lifecycle = _get_lifecycle()
        if lifecycle is None:
            return
        lifecycle.enqueue_oplog_sample(_make_sample_ctx(
            result_table, operation, kwargs or {}, sql,
            self.task_id, self.job_id,
        ))

    def record_table(self, table_name: str) -> None:
        """Route a table registry record to the lifecycle worker."""
        lifecycle = _get_lifecycle()
        if lifecycle is None:
            return
        lifecycle.enqueue_oplog_table(_make_table_ctx(
            table_name, self.task_id, self.job_id,
        ))

    async def flush(self) -> None:
        """Flush buffered oplog entries via the lifecycle worker."""
        lifecycle = _get_lifecycle()
        if lifecycle is not None:
            await lifecycle.flush_oplog()

    def discard(self) -> None:
        """Discard buffered oplog entries without flushing."""
        lifecycle = _get_lifecycle()
        if lifecycle is not None:
            lifecycle.discard_oplog()


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
