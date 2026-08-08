CREATE TABLE execution_workers (
    id BIGINT PRIMARY KEY,
    hostname VARCHAR NOT NULL,
    pid INTEGER NOT NULL,
    status VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP NOT NULL,
    last_heartbeat TIMESTAMP NOT NULL,
    tasks_completed INTEGER NOT NULL DEFAULT 0,
    tasks_failed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE jobs (
    id BIGINT PRIMARY KEY,
    name VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    run_type VARCHAR NOT NULL DEFAULT 'MANUAL',
    preservation_mode VARCHAR NOT NULL DEFAULT 'NONE',
    runner_mode VARCHAR NOT NULL DEFAULT 'subprocess',
    runner JSON,
    created_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error VARCHAR
);

CREATE TABLE groups (
    id BIGINT PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id),
    parent_group_id BIGINT,
    name VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE tasks (
    id BIGINT PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id),
    group_id BIGINT REFERENCES groups(id),
    entrypoint VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    kwargs JSON NOT NULL DEFAULT '{}',
    entry_type VARCHAR,
    command JSON,
    command_env JSON,
    image_source JSON,
    is_image_build BOOLEAN NOT NULL DEFAULT FALSE,
    status VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    claimed_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    execution_worker_id BIGINT REFERENCES execution_workers(id),
    result JSON,
    error VARCHAR,
    max_retries INTEGER NOT NULL DEFAULT 0,
    attempt INTEGER NOT NULL DEFAULT 0,
    retry_after TIMESTAMP,
    run_ids JSON NOT NULL DEFAULT '[]',
    run_statuses JSON NOT NULL DEFAULT '[]',
    run_epoch BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE dependencies (
    previous_id BIGINT NOT NULL,
    previous_type VARCHAR NOT NULL,
    next_id BIGINT NOT NULL,
    next_type VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (previous_id, previous_type, next_id, next_type)
);
