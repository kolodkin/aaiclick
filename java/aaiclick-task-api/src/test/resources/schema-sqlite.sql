-- Local mirror of the SQLModel tables the shim touches (aaiclick/orchestration/
-- models.py — Task, RemoteTaskResult). CI regenerates the real schema from the
-- Python metadata and points the suite at it via AAICLICK_TEST_SQLITE_DB, which
-- guards this file against cross-language drift.

CREATE TABLE tasks (
    id BIGINT NOT NULL PRIMARY KEY,
    job_id BIGINT,
    group_id BIGINT,
    entrypoint VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    kwargs JSON,
    entry_type VARCHAR,
    command JSON,
    command_env JSON,
    image_source JSON,
    is_image_build BOOLEAN NOT NULL DEFAULT 0,
    status VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    claimed_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    execution_worker_id BIGINT,
    result JSON,
    error VARCHAR,
    max_retries INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    retry_after TIMESTAMP,
    run_ids JSON NOT NULL DEFAULT '[]',
    run_statuses JSON NOT NULL DEFAULT '[]',
    run_epoch BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE remote_task_results (
    task_id BIGINT NOT NULL,
    run_epoch BIGINT NOT NULL,
    success BOOLEAN NOT NULL,
    result_ref JSON,
    error VARCHAR,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (task_id, run_epoch)
);
