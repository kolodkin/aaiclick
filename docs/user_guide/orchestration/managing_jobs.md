Managing Jobs
---

# Managing Jobs

## Inspect jobs

<!-- req: inspect-jobs -->
=== "Python"

    ```python
    from aaiclick.orchestration import get_job, list_jobs, orch_context

    async with orch_context():
        jobs = await list_jobs(status="RUNNING", name_like="%crawl%", limit=20)
        job = await get_job(job_id)
    ```

=== "CLI"

    ```bash
    python -m aaiclick job list [--status RUNNING] [--like "%crawl%"] [--limit 20 --offset 40]
    python -m aaiclick job get <id>
    python -m aaiclick job stats <id>
    python -m aaiclick task list [--job <ref>] [--like "%load%"] [--status FAILED] [--limit 20 --offset 40]
    ```

=== "REST"

    ```bash
    curl "http://127.0.0.1:5255/api/v0/jobs?status=RUNNING&limit=20"
    curl http://127.0.0.1:5255/api/v0/jobs/<id>
    curl http://127.0.0.1:5255/api/v0/jobs/<id>/stats
    curl "http://127.0.0.1:5255/api/v0/tasks?job=<ref>&name=%25load%25"
    ```

## Cancel a job

<!-- req: cancel-job -->
Atomically cancels the job and all its non-terminal tasks. Workers detect
cancellation by polling task status; a CPU-bound task won't interrupt until it
yields.

=== "Python"

    ```python
    from aaiclick.orchestration import cancel_job, orch_context

    async with orch_context():
        job = await cancel_job(job_id)  # raises JobNotFound / JobAlreadyTerminal
    ```

=== "CLI"

    ```bash
    python -m aaiclick job cancel <id>
    ```

=== "REST"

    ```bash
    curl -X POST http://127.0.0.1:5255/api/v0/jobs/<id>/cancel
    ```

## Re-run tasks (clear)

<!-- req: clear-task -->
`clear_task` resets a task and all its transitive downstream tasks to PENDING
for re-run (Airflow-style "clear task"); upstream tasks and their results are
untouched, and a finished job is reactivated.

=== "Python"

    ```python
    from aaiclick.orchestration import orch_context
    from aaiclick.orchestration.execution.claiming import clear_task

    async with orch_context():
        cleared_task_ids, job = await clear_task(task_id)
    ```

=== "REST"

    ```bash
    curl -X POST http://127.0.0.1:5255/api/v0/tasks/<task_id>/clear
    ```

# Task Logs

<!-- req: task-logs -->
Task stdout/stderr streams into the ClickHouse `task_logs` table while the
task runs (flushed every ~2 s), so a long task can be tailed live, from one
place, whichever host, container, or Pod ran it. Only stdout/stderr are captured: `logging` records appear only
when their handler writes to stdout/stderr (e.g. `logging.basicConfig()`
inside the task). The UI colors lines by keyword — `error` / `exception` /
`traceback` red, `warn` yellow, `debug` dim. Logs share
the job's retention lifecycle. Fetch them via
`GET /api/v0/tasks/<task_id>/logs?tail=100`.

## Worker logs

<!-- req: worker-logs -->
Every long-running command (`local start`, `execution-worker start`,
`background start`) writes timestamped `INFO` lines to stderr. The execution
worker logs `executing task` before each task and `completed task` (or the
failure state) after it, so a slow task shows up as a gap between its two
lines. Local mode also logs `chdb session ready in Ns` when the embedded
engine starts. The first task after a start is still slower than the rest: the
engine's first DDL and first insert fault more of its ~540 MB library into
memory.
