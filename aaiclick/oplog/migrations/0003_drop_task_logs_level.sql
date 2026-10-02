-- Task logs carry raw stdout / stderr only; the UI colors lines by keyword,
-- so the per-line severity column is gone.

ALTER TABLE task_logs DROP COLUMN IF EXISTS level;
