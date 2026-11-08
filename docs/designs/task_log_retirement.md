Task Log Retirement — ClickHouse as the Sole Log Store
---

**Implementation**: `aaiclick/orchestration/logging.py`

ClickHouse `task_logs` is the only place task stdout/stderr lives, for every
runner and entry type. File-based task logs are gone.

# Motivation

Module (Python) tasks already streamed logs to CH, so their file tee was pure
redundancy. Shell tasks were the opposite: their output landed **only** in a
host-local file (host-shell pipe, `docker logs`, `kubectl logs`), they never
got a registered run, and `GET /tasks/{id}/logs` returned `available=False`
for them — shell logs were invisible cross-host.

Retiring the file had an enabling half (shell logs reach CH) and a removal
half (every file path and `log_path` column deleted).

# Shell Logs to ClickHouse

## Run registration

Each attempt is a `task_runs` row keyed by a snowflake `run_id`, opened by a
shared helper in `aaiclick/orchestration/execution/runner.py` that shell
vehicles call at launch. Shell log rows carry a real `run_id`, so task-log
reads (latest `task_runs` row by default) work for them unchanged.

## Capture per runner

| Runner            | Capture (streamed → CH)                                     |
|-------------------|-------------------------------------------------------------|
| Host shell        | child stdout piped into the capture sink, drained every 2 s |
| Docker `shell`    | host follows `docker logs -f`, same streaming pipe          |
| Kubernetes shell  | host follows `kubectl logs -f -c task`, same streaming pipe |
| Kubernetes module | pod streams to CH itself                                    |

Captured lines are tagged with their source stream (stdout → `INFO`, stderr →
`WARNING`; the docker/kubectl wrappers preserve the split for non-TTY runs)
and written under a running `seq` offset.

## Flushing from the worker parent

The worker parent runs `orch_context()` but never queries ClickHouse, so it
never opens the chdb session its children need. Shell tasks therefore run
inside the same spawned task child module tasks use, which opens its own
`orch_context` and streams inline — one code path for local (chdb) and
distributed (remote CH). A run killed mid-flight keeps everything flushed up
to the last 2 s tick.

# Removal

No file log path remains: output capture has no file tee, there is no logs
directory or `AAICLICK_LOG_DIR`, and `log_path` is gone from runner results,
docker's result payload, task status updates, `tasks` and
`remote_task_results` (via Alembic), the task API / CLI views, and the
generated `src/api/schema.ts`. Image builds are ordinary tasks, so their logs
ride the same task-log path.
