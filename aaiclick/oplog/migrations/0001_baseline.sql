-- Baseline: the internal ClickHouse tables as they exist before the
-- migration framework. IF NOT EXISTS makes this safe on installs that
-- already have them; fresh installs get the full schema.

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
ORDER BY (result_table, created_at);

CREATE TABLE IF NOT EXISTS task_logs (
    task_id     UInt64,
    job_id      UInt64,
    run_id      UInt64,
    seq         UInt64,
    stream      String,
    level       String,
    line        String,
    created_at  DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (task_id, run_id, seq);
