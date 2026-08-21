-- SQLite variant: JSON columns have TEXT affinity, no cast needed.
INSERT INTO remote_task_results (task_id, run_epoch, success, result_ref, error, created_at)
VALUES (:task_id, :run_epoch, :success, :result_ref, :error, CURRENT_TIMESTAMP)
ON CONFLICT (task_id, run_epoch) DO UPDATE
SET success = EXCLUDED.success,
    result_ref = EXCLUDED.result_ref,
    error = EXCLUDED.error
