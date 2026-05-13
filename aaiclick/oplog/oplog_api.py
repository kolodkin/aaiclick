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


def _check_kwargs(kwargs: dict[str, str] | None) -> None:
    """Reject non-string values — the CH ``operation_log.kwargs`` column is
    ``Map(String, String)`` and silently drops the row on type mismatch.
    Encode lists/objects to a string at the callsite (e.g. ``",".join(...)``).
    """
    if kwargs is None:
        return
    for k, v in kwargs.items():
        if not isinstance(v, str):
            raise TypeError(
                f"oplog kwarg {k!r} must be str, got {type(v).__name__}; "
                f"encode lists/objects at the callsite (e.g. ','.join(...))"
            )


def oplog_record(
    result_table: str,
    operation: str,
    kwargs: dict[str, str] | None = None,
    sql: str | None = None,
) -> None:
    """Record an operation via the active lifecycle handler.

    ``kwargs`` values must be strings. See ``_check_kwargs``.
    """
    _check_kwargs(kwargs)
    lc = _get_lifecycle()
    if lc is not None:
        lc.oplog_record(result_table, operation, kwargs=kwargs, sql=sql)


def oplog_record_sample(
    result_table: str,
    operation: str,
    kwargs: dict[str, str] | None = None,
    sql: str | None = None,
) -> None:
    """Record an operation with lineage sampling via the active lifecycle handler.

    ``kwargs`` values must be strings. See ``_check_kwargs``.
    """
    _check_kwargs(kwargs)
    lc = _get_lifecycle()
    if lc is not None:
        lc.oplog_record_sample(result_table, operation, kwargs=kwargs, sql=sql)
