-- Shared job-completion rollup — executed by the Python worker/background
-- worker (background/handler.py) and the Java worker (TaskRepo.java). This
-- file owns the status-set knowledge: which task states count as
-- non-terminal, failed-like, and cascade-triggering. Portable SQL — the
-- Python side also runs it on SQLite in local mode.
--
--   job_id  the job to aggregate
--
-- Workers read total/non_terminal/failed for the rollup-only recipe;
-- cascade_trigger is consumed only by the full try_complete_job on the
-- failure-processing path (UPSTREAM_FAILED sweep stays Python-only).
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN status IN ('PENDING', 'CLAIMED', 'RUNNING', 'PENDING_CLEANUP')
        THEN 1 ELSE 0 END) AS non_terminal,
    SUM(CASE WHEN status IN ('FAILED', 'UPSTREAM_FAILED') THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN status IN ('FAILED', 'CANCELLED', 'UPSTREAM_FAILED')
        THEN 1 ELSE 0 END) AS cascade_trigger
FROM tasks WHERE job_id = :job_id
