-- Shared PostgreSQL claim query — the single source of truth for both the
-- Python worker (pg_handler.py) and the Java worker (TaskRepo.java, which
-- embeds this file as a build-time resource). SQLite has its own
-- Python-only implementation in sqlite_handler.py.
--
-- Named parameters only; every predicate difference between workers is a
-- bound VALUE, never a structural edit:
--   :execution_worker_id  claiming worker id
--   :now                  claim timestamp (repeated)
--   :entry_types          entry types this worker can execute
--                         (Python: all; Java: shell only)
--   :allow_image_tasks    false pins the worker to host-subprocess tasks
--                         (image_source both SQL NULL and JSON null mean
--                         "no image" — SQLAlchemy writes the latter)
WITH claimed_task AS (
    UPDATE tasks
    SET
        status = 'RUNNING',
        execution_worker_id = :execution_worker_id,
        claimed_at = :now
    WHERE id = (
        SELECT t.id FROM tasks t
        JOIN jobs j ON t.job_id = j.id
        WHERE t.status = 'PENDING'
        AND (t.retry_after IS NULL OR t.retry_after <= :now)
        AND j.status NOT IN ('CANCELLED', 'FAILED')
        AND COALESCE(t.entry_type, 'module') = ANY(:entry_types)
        AND (:allow_image_tasks OR t.image_source IS NULL OR t.image_source::text = 'null')
        AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON d.previous_id = prev.id
            WHERE d.next_id = t.id
            AND d.next_type = 'task'
            AND d.previous_type = 'task'
            AND prev.status != 'COMPLETED'
        )
        AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON prev.group_id = d.previous_id
            WHERE d.next_id = t.id
            AND d.next_type = 'task'
            AND d.previous_type = 'group'
            AND prev.status != 'COMPLETED'
        )
        AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON d.previous_id = prev.id
            WHERE d.next_id = t.group_id
            AND d.next_type = 'group'
            AND d.previous_type = 'task'
            AND prev.status != 'COMPLETED'
            AND t.group_id IS NOT NULL
        )
        AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON prev.group_id = d.previous_id
            WHERE d.next_id = t.group_id
            AND d.next_type = 'group'
            AND d.previous_type = 'group'
            AND prev.status != 'COMPLETED'
            AND t.group_id IS NOT NULL
        )
        ORDER BY j.started_at ASC NULLS LAST, t.id ASC
        LIMIT 1
        FOR UPDATE OF t SKIP LOCKED
    )
    -- RETURNING * (not a hand-maintained column list): every tasks column
    -- must reach Python's Task(**row) or it silently falls back to a model
    -- default. Dropping entry_type/command made shell tasks run as module
    -- tasks; dropping run_epoch broke the fencing guard. Java reads only
    -- the columns it needs, by name, from the same result set.
    RETURNING *
),
updated_job AS (
    UPDATE jobs
    SET
        started_at = COALESCE(started_at, :now),
        status = CASE
            WHEN started_at IS NULL THEN 'RUNNING'
            ELSE status
        END
    WHERE id = (SELECT job_id FROM claimed_task)
    RETURNING id
)
SELECT * FROM claimed_task
