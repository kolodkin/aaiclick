-- Baseline: aaiclick's internal ClickHouse tables. They live in the internal
-- database ({internal_db}, substituted by the runner), never in the data
-- database that holds user objects.

CREATE TABLE IF NOT EXISTS {internal_db}.operation_log (
    id              UInt64 DEFAULT generateSnowflakeID(),
    result_table    String,
    operation       String,
    kwargs          Map(String, String),
    sql_template    Nullable(String),
    task_id         Nullable(UInt64),
    job_id          Nullable(UInt64),
    run_id          Nullable(UInt64),
    created_at      DateTime64(3, 'UTC')
) ENGINE = MergeTree()
ORDER BY (result_table, created_at);

CREATE TABLE IF NOT EXISTS {internal_db}.task_logs (
    task_id     UInt64,
    job_id      UInt64,
    run_id      UInt64,
    seq         UInt64,
    stream      String,
    line        String,
    created_at  DateTime64(3, 'UTC')
) ENGINE = MergeTree()
ORDER BY (task_id, run_id, seq);
