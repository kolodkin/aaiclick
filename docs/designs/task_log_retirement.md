Task Log Retirement — ClickHouse as the Sole Log Store
---

**Implementation**: `aaiclick/orchestration/logging.py`

Design for retiring file-based task logs. After this change, ClickHouse
`task_logs` is the only place task stdout/stderr lives, for every runner and
entry type, and the `log_path` plumbing disappears end to end.

# Motivation

Module (Python) tasks already stream logs to CH via `capture_task_output`; their
file tee is pure redundancy. Shell tasks are the opposite: their output lands
**only** in a host file (host-shell pipe, docker `docker logs`, kubernetes
`kubectl logs`), they never get a registered run, and `GET /tasks/{id}/logs` returns
`available=False` for them. The file is host-local, so shell logs are invisible
cross-host today.

Retiring the file therefore has an enabling half (shell logs must reach CH
first) and a removal half (delete every file path and `log_path` column).

# Shell Logs to ClickHouse

## Run registration

Each attempt is a `task_runs` row keyed by a snowflake `run_id`, opened by a
shared helper (`register_run(task_id) -> run_id` in
`aaiclick/orchestration/execution/runner.py`) that shell vehicles call at launch.
Shell tasks then key their CH log rows by a real `run_id`, and `get_task_logs`
(latest `task_runs` row by default) works for them unchanged.

## Capture per runner

| Runner            | Capture (streamed → CH)                                        |
|-------------------|----------------------------------------------------------------|
| Host shell        | child stdout piped into the capture sink, drained every 2 s    |
| Docker `shell`    | host follows `docker logs -f`, same streaming pipe             |
| Kubernetes shell  | host follows `kubectl logs -f -c task`, same streaming pipe    |
| Kubernetes module | pod streams to CH itself via `capture_task_output`             |

Captured text is converted to `LogLine`s tagged with their source stream
(stdout → `INFO`, stderr → `WARNING`; the docker/kubectl wrappers preserve
the stream split for non-TTY runs) and written with `flush_task_logs` under a
running `seq` offset.

## Flushing from the worker parent

The worker parent runs `orch_context()` and never queries ClickHouse, so it
never opens the chdb session its children need. Shell tasks therefore execute inside the
same spawned task child module tasks use (`_child_run_task`), which opens its
own `orch_context` and streams inline. One code path for local (chdb) and
distributed (remote CH) modes. A run killed mid-flight keeps everything
flushed up to the last 2 s tick.

# Removal

- **`capture_task_output`** — no file tee; yields nothing. `get_logs_dir` and
  the `AAICLICK_LOG_DIR` env var are deleted with it.
- **`RunnerResult` / `ExecuteFn`** — `log_path` dropped from the result tuple
  everywhere, from docker's result payload, and from
  `update_task_status` / `clear_task`.
- **Schema** — `tasks.log_path` and `remote_task_results.log_path` dropped via
  Alembic. (Image builds are ordinary tasks now, so their logs ride the same
  task-log path — no separate build log location exists.)
- **API / UI** — `log_path` removed from `TaskDetail`, `TaskLogsView`,
  `internal_api.tasks`, and the CLI task renderer; `src/api/schema.ts`
  regenerated. No UI component read it.
