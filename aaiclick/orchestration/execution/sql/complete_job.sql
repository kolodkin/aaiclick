-- Shared terminal job update, paired with job_rollup.sql. Runs only after
-- the rollup reported zero non-terminal tasks. Portable SQL (PG + SQLite).
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
