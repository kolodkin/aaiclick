"""
aaiclick.oplog.models - startup entry point for the internal ClickHouse
schema (``operation_log``, ``task_logs``), driven by the migration runner
in ``aaiclick.oplog.migrate``. DDL lives in ``aaiclick/oplog/migrations/``.
"""

from __future__ import annotations

from aaiclick.backend import is_local
from aaiclick.data.data_context import ChClient

from .migrate import ch_pending, ch_upgrade

OPERATION_LOG_COLUMN_TYPES: dict[str, str] = {
    "id": "UInt64",
    "result_table": "String",
    "operation": "String",
    "kwargs": "Map(String, String)",
    "sql_template": "Nullable(String)",
    "task_id": "Nullable(UInt64)",
    "job_id": "Nullable(UInt64)",
    "run_id": "Nullable(UInt64)",
    "created_at": "DateTime64(3)",
}
# Insert type names for the oplog flush (orch_context._OPLOG_TYPE_NAMES) —
# must stay in sync with the operation_log DDL in the migration scripts.

TASK_LOGS_COLUMN_TYPES: dict[str, str] = {
    "task_id": "UInt64",
    "job_id": "UInt64",
    "run_id": "UInt64",
    "seq": "UInt64",
    "stream": "String",
    "level": "String",
    "line": "String",
    "created_at": "DateTime64(3)",
}
# Insert column names/types for the task-log flush (orchestration.logging) —
# must stay in sync with the task_logs DDL in the migration scripts.


async def init_oplog_tables(ch_client: ChClient) -> None:
    """Bring the internal CH schema up to date, or fail asking for a migrate.

    Local mode (chdb + SQLite) applies pending migrations directly —
    single-process, zero-ops, mirrors SQLite's create-on-setup. Distributed
    mode never writes: the operator runs ``aaiclick migrate upgrade``.
    """
    if is_local():
        await ch_upgrade(ch_client)
        return

    pending = await ch_pending(ch_client)
    if pending:
        raise RuntimeError(
            f"ClickHouse schema is behind (pending: {', '.join(pending)}). Run: aaiclick migrate upgrade"
        )
