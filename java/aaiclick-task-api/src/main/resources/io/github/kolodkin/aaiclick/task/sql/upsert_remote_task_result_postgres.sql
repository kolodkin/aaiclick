-- Postgres variant: result_ref is a json column, so the text parameter needs
-- an explicit cast (JDBC sends strings as varchar).
INSERT INTO remote_task_results (task_id, run_epoch, success, result_ref, error, created_at)
VALUES (:task_id, :run_epoch, :success, CAST(:result_ref AS json), :error, CURRENT_TIMESTAMP)
ON CONFLICT (task_id, run_epoch) DO UPDATE
SET success = EXCLUDED.success,
    result_ref = EXCLUDED.result_ref,
    error = EXCLUDED.error
