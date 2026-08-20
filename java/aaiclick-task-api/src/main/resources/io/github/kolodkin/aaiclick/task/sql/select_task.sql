SELECT id, job_id, entrypoint, kwargs
FROM tasks
WHERE id = :task_id
