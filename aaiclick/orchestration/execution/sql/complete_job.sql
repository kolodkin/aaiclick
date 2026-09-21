-- Shared terminal job update, paired with job_rollup.sql. Runs only after
-- the rollup reported zero non-terminal tasks, and only on a job that is not
-- already terminal: the cancelled-cleanup pass settling a cancelled job's
-- last task must not turn CANCELLED into COMPLETED or FAILED.
-- Portable SQL (PG + SQLite).
--
--   status  'COMPLETED' or 'FAILED'
--   error   failure message, or NULL to leave the column untouched
--   now     completion timestamp
--   job_id  the job to complete
UPDATE jobs
SET status = :status,
    completed_at = :now,
    error = COALESCE(:error, error)
WHERE id = :job_id
  AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
