Task Log Retirement — ClickHouse as the Sole Log Store
---

Design for retiring file-based task logs. After this change, ClickHouse
`task_logs` is the only place task stdout/stderr lives, for every runner and
entry type, and the `log_path` plumbing disappears end to end.

**Implementation**: `aaiclick/orchestration/logging.py` — see
`capture_task_output`, `_SinkFlusher`;
`aaiclick/orchestration/execution/runner.py` — see `register_run`,
`execute_shell_task`, `ShellSpec`;
`aaiclick/orchestration/execution/dispatch.py` — see `build_shell_spec`.

# Motivation

Module (Python) tasks already stream logs to CH via `capture_task_output`; their
file tee is pure redundancy. Shell tasks are the opposite: their output lands
**only** in a host file (host-shell pipe, docker `docker logs`, kubernetes
`kubectl logs`), they never get `run_ids`, and `GET /tasks/{id}/logs` returns
`available=False` for them. The file is host-local, so shell logs are invisible
cross-host today.

Retiring the file therefore has an enabling half (shell logs must reach CH
first) and a removal half (delete every file path and `log_path` column).

# Shell Logs to ClickHouse

## Run registration

`execute_task` currently mints a per-attempt snowflake `run_id` and appends it
to `Task.run_ids` / `run_statuses`. That block is factored into a shared helper
(`register_run(task_id) -> run_id` in `aaiclick/orchestration/execution/runner.py`)
so shell vehicles can call it at launch. Shell tasks then key their CH log rows
by a real `run_id`, and the existing `get_task_logs` read path (`run_ids[-1]`)
works for them unchanged.

## Capture per runner

| Runner            | Capture (streamed → CH)                                        |
|-------------------|----------------------------------------------------------------|
| Host shell        | child stdout piped into the capture sink, drained every 2 s    |
| Docker `shell`    | foreground `docker run --rm` stdout, same streaming pipe       |
| Kubernetes shell  | `kubectl run --attach` stdout, same streaming pipe             |
| Kubernetes module | pod streams to CH itself via `capture_task_output`             |

Captured text is converted to `LogLine`s (stdout → `INFO`; docker/k8s merge
stderr into stdout) and written with `flush_task_logs` under a running `seq`
offset.

## Flushing from the worker parent

The worker parent runs `orch_context(with_ch=False)` — it must not hold the
chdb session while children need it. Shell tasks therefore execute inside the
same spawned task child module tasks use (`_child_run_task`), which opens its
own `orch_context` and streams inline. One code path for local (chdb) and
distributed (remote CH) modes. A run killed mid-flight keeps everything
flushed up to the last 2 s tick.

# Removal

- **`capture_task_output`** — no file tee; yields nothing. `get_logs_dir` and
  the `AAICLICK_LOG_DIR` env var are deleted with it.
- **`RunnerResult` / `ExecuteFn`** — `log_path` dropped from the result tuple
  everywhere, from docker's `result.json` payload, and from
  `update_task_status` / `clear_task`.
- **Schema** — `tasks.log_path` and `remote_task_results.log_path` dropped via
  Alembic. `build_tasks.log_path` stays; build logs are out of scope here.
- **API / UI** — `log_path` removed from `TaskDetail`, `TaskLogsView`,
  `internal_api.tasks`, and the CLI task renderer; `src/api/schema.ts`
  regenerated. No UI component read it.
