"""
aaiclick.oplog.oplog_api - Oplog recording via the lifecycle handler.

Module-level functions delegate to the active LifecycleHandler.
In local mode (data_context), the handler's oplog methods are no-ops.
In orchestration mode (task_scope), OrchLifecycleHandler enqueues
messages on the same FIFO queue as incref/decref.
"""

from __future__ import annotations


def _get_lifecycle():
    """Lazy import to avoid circular dependency."""
    from aaiclick.data.data_context.lifecycle import get_data_lifecycle

    return get_data_lifecycle()


def oplog_record(
    result_table: str,
    operation: str,
    kwargs: dict[str, str] | None = None,
    sql: str | None = None,
) -> None:
    """Record an operation via the active lifecycle handler."""
    lc = _get_lifecycle()
    if lc is not None:
        lc.oplog_record(result_table, operation, kwargs=kwargs, sql=sql)


def oplog_record_sample(
    result_table: str,
    operation: str,
    kwargs: dict[str, str] | None = None,
    sql: str | None = None,
) -> None:
    """Record an operation with lineage sampling via the active lifecycle handler."""
    lc = _get_lifecycle()
    if lc is not None:
        lc.oplog_record_sample(result_table, operation, kwargs=kwargs, sql=sql)


def oplog_record_table(table_name: str, schema_doc: str | None = None) -> None:
    """Register a newly created table via the active lifecycle handler.

    ``schema_doc`` is a Pydantic-serialised ``SchemaView`` JSON document; it lands
    in ``table_registry.schema_doc`` and is read back by ``_get_table_schema``.
    """
    lc = _get_lifecycle()
    if lc is not None:
        lc.oplog_record_table(table_name, schema_doc=schema_doc)
