"""
aaiclick.oplog.models - ClickHouse DDL and schema validation for the
orchestration-owned CH tables created on task-scope entry: the ``operation_log``
provenance table and the ``task_logs`` captured-output stream.
"""

from __future__ import annotations

from aaiclick.data.data_context import ChClient

OPERATION_LOG_DDL = """
CREATE TABLE IF NOT EXISTS operation_log (
    id              UInt64 DEFAULT generateSnowflakeID(),
    result_table    String,
    operation       String,
    kwargs          Map(String, String),
    sql_template    Nullable(String),
    task_id         Nullable(UInt64),
    job_id          Nullable(UInt64),
    run_id          Nullable(UInt64),
    created_at      DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (result_table, created_at)
"""
# result_table leads the sort key so every oplog consumer
# (backward_oplog, ...) gets skip-index-friendly lookups;
# created_at breaks ties within a table so "most recent row"
# stays a tail scan.

OPERATION_LOG_EXPECTED_COLUMNS: dict[str, str] = {
    "id": "UInt64",  # DEFAULT generateSnowflakeID() — type check only
    "result_table": "String",
    "operation": "String",
    "kwargs": "Map(String, String)",
    "sql_template": "Nullable(String)",
    "task_id": "Nullable(UInt64)",
    "job_id": "Nullable(UInt64)",
    "run_id": "Nullable(UInt64)",
    "created_at": "DateTime64(3)",
}

TASK_LOGS_DDL = """
CREATE TABLE IF NOT EXISTS task_logs (
    task_id     UInt64,
    job_id      UInt64,
    run_id      UInt64,
    seq         UInt64,
    stream      String,
    line        String,
    created_at  DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (task_id, run_id, seq)
"""
# Captured task stdout/stderr, one row per line. Every runner (subprocess,
# docker, kubernetes) streams here from inside the task process, so logs are
# reachable cross-host through a single read path. (task_id, run_id) leads the
# sort key — the read path always scans one task attempt — and seq preserves
# the emission order within that attempt. ``stream`` tags the source
# (``stdout`` / ``stderr``) so the UI can distinguish them.

TASK_LOGS_EXPECTED_COLUMNS: dict[str, str] = {
    "task_id": "UInt64",
    "job_id": "UInt64",
    "run_id": "UInt64",
    "seq": "UInt64",
    "stream": "String",
    "line": "String",
    "created_at": "DateTime64(3)",
}


async def _validate_schema(
    ch_client: ChClient,
    table: str,
    expected: dict[str, str],
) -> None:
    """Check all expected columns exist with correct types; raise on mismatch."""
    result = await ch_client.query(f"SELECT name, type FROM system.columns WHERE table = '{table}'")
    actual = {row[0]: row[1] for row in result.result_rows}
    for col, expected_type in expected.items():
        if col not in actual:
            raise RuntimeError(
                f"ClickHouse table '{table}' is missing column '{col}'. Drop the table and let aaiclick recreate it."
            )
        if actual[col] != expected_type:
            raise RuntimeError(
                f"ClickHouse table '{table}' column '{col}' has type "
                f"'{actual[col]}', expected '{expected_type}'. "
                f"Drop the table and let aaiclick recreate it."
            )


async def init_oplog_tables(ch_client: ChClient) -> None:
    """Create oplog tables if they don't exist; validate schema if they do."""
    await ch_client.command(OPERATION_LOG_DDL)
    await _validate_schema(ch_client, "operation_log", OPERATION_LOG_EXPECTED_COLUMNS)
    await ch_client.command(TASK_LOGS_DDL)
    await _validate_schema(ch_client, "task_logs", TASK_LOGS_EXPECTED_COLUMNS)
