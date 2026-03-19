"""
aaiclick.oplog.models - ClickHouse DDL and schema validation for oplog tables.
"""

from __future__ import annotations

import os

from aaiclick.data.ch_client import ChClient


OPERATION_LOG_DDL = """
CREATE TABLE IF NOT EXISTS operation_log (
    id           UInt64 DEFAULT generateSnowflakeID(),
    result_table String,
    operation    String,
    args         Array(String),
    kwargs       Map(String, String),
    sql_template Nullable(String),
    task_id      Nullable(UInt64),
    job_id       Nullable(UInt64),
    created_at   DateTime64(3)
) ENGINE = MergeTree()
ORDER BY created_at
TTL created_at + INTERVAL {ttl_days} DAY DELETE
"""

TABLE_REGISTRY_DDL = """
CREATE TABLE IF NOT EXISTS table_registry (
    table_name   String,
    job_id       Nullable(UInt64),
    task_id      Nullable(UInt64),
    created_at   DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (created_at,)
"""

OPERATION_LOG_EXPECTED_COLUMNS: dict[str, str] = {
    "id": "UInt64",  # DEFAULT generateSnowflakeID() — type check only
    "result_table": "String",
    "operation": "String",
    "args": "Array(String)",
    "kwargs": "Map(String, String)",
    "sql_template": "Nullable(String)",
    "task_id": "Nullable(UInt64)",
    "job_id": "Nullable(UInt64)",
    "created_at": "DateTime64(3)",
}

TABLE_REGISTRY_EXPECTED_COLUMNS: dict[str, str] = {
    "table_name": "String",
    "job_id": "Nullable(UInt64)",
    "task_id": "Nullable(UInt64)",
    "created_at": "DateTime64(3)",
}


def _ttl_days() -> int:
    return int(os.environ.get("AAICLICK_OPLOG_TTL_DAYS", "90"))


async def _validate_schema(
    ch_client: ChClient,
    table: str,
    expected: dict[str, str],
) -> None:
    """Check all expected columns exist with correct types; raise on mismatch."""
    result = await ch_client.query(
        f"SELECT name, type FROM system.columns WHERE table = '{table}'"
    )
    actual = {row[0]: row[1] for row in result.result_rows}
    for col, expected_type in expected.items():
        if col not in actual:
            raise RuntimeError(
                f"Oplog table '{table}' is missing column '{col}'. "
                f"Drop the table and let aaiclick recreate it."
            )
        if actual[col] != expected_type:
            raise RuntimeError(
                f"Oplog table '{table}' column '{col}' has type "
                f"'{actual[col]}', expected '{expected_type}'. "
                f"Drop the table and let aaiclick recreate it."
            )


async def init_oplog_tables(ch_client: ChClient) -> None:
    """Create oplog tables if they don't exist; validate schema if they do."""
    await ch_client.command(OPERATION_LOG_DDL.format(ttl_days=_ttl_days()))
    await ch_client.command(TABLE_REGISTRY_DDL)
    await _validate_schema(ch_client, "operation_log", OPERATION_LOG_EXPECTED_COLUMNS)
    await _validate_schema(ch_client, "table_registry", TABLE_REGISTRY_EXPECTED_COLUMNS)
