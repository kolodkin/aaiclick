"""
aaiclick.oplog.models - ClickHouse DDL and schema validation for oplog tables.
"""

from __future__ import annotations

from aaiclick.data.data_context import ChClient


OPERATION_LOG_DDL = """
CREATE TABLE IF NOT EXISTS operation_log (
    id              UInt64 DEFAULT generateSnowflakeID(),
    result_table    String,
    operation       String,
    kwargs          Map(String, String),
    kwargs_aai_ids  Map(String, Array(UInt64)),
    result_aai_ids  Array(UInt64),
    sql_template    Nullable(String),
    task_id         Nullable(UInt64),
    job_id          Nullable(UInt64),
    run_id          Nullable(UInt64),
    created_at      DateTime64(3)
) ENGINE = MergeTree()
ORDER BY created_at
"""

TABLE_REGISTRY_DDL = """
CREATE TABLE IF NOT EXISTS table_registry (
    table_name   String,
    job_id       Nullable(UInt64),
    task_id      Nullable(UInt64),
    run_id       Nullable(UInt64),
    created_at   DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (created_at,)
"""

OPERATION_LOG_EXPECTED_COLUMNS: dict[str, str] = {
    "id": "UInt64",  # DEFAULT generateSnowflakeID() — type check only
    "result_table": "String",
    "operation": "String",
    "kwargs": "Map(String, String)",
    "kwargs_aai_ids": "Map(String, Array(UInt64))",
    "result_aai_ids": "Array(UInt64)",
    "sql_template": "Nullable(String)",
    "task_id": "Nullable(UInt64)",
    "job_id": "Nullable(UInt64)",
    "run_id": "Nullable(UInt64)",
    "created_at": "DateTime64(3)",
}

TABLE_REGISTRY_EXPECTED_COLUMNS: dict[str, str] = {
    "table_name": "String",
    "job_id": "Nullable(UInt64)",
    "task_id": "Nullable(UInt64)",
    "run_id": "Nullable(UInt64)",
    "created_at": "DateTime64(3)",
}


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
    await ch_client.command(OPERATION_LOG_DDL)
    await ch_client.command(TABLE_REGISTRY_DDL)
    await _validate_schema(ch_client, "operation_log", OPERATION_LOG_EXPECTED_COLUMNS)
    await _validate_schema(ch_client, "table_registry", TABLE_REGISTRY_EXPECTED_COLUMNS)
