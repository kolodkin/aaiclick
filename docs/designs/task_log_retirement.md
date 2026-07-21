Task Log Retirement — ClickHouse as the Sole Log Store
---

Design for retiring file-based task logs. After this change, ClickHouse
`task_logs` is the only place task stdout/stderr lives, for every runner and
entry type, and the `log_path` plumbing disappears end to end.

**Implementation**: `aaiclick/orchestration/logging.py` — see
`capture_task_output`, `shell_text_to_lines`;
`aaiclick/orchestration/execution/log_flush.py` — see `flush_shell_logs`;
`aaiclick/orchestration/execution/runner.py` — see `register_run`; shell
vehicles in `mp_worker.py` (`_HostShellVehicle`), `docker_worker.py`
(`_DockerVehicle`), `kubernetes_worker.py` (`_KubernetesVehicle`).

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

| Runner            | Capture today (file)                   | Capture after (memory → CH)              |
|-------------------|----------------------------------------|------------------------------------------|
| Host shell        | child stdout piped to open file        | child stdout piped, read into memory     |
| Docker `shell`    | `docker logs` written to file          | `docker logs` text kept in memory        |
| Kubernetes shell  | `_capture_pod_logs` → file             | `kubectl logs` text kept in memory       |
| Kubernetes module | `_capture_pod_logs` → file (redundant) | removed — pod streams to CH itself       |

Captured text is converted to `LogLine`s (stdout → `INFO`, stderr folded into
stdout for docker/k8s where the streams are merged) and flushed with the
existing `flush_task_logs`.

## Flushing from the worker parent

The worker parent runs `orch_context(with_ch=False)` — it must not hold the
chdb session while children need it. The flush therefore runs in a short-lived
spawned child process (same `spawn` context as `mp_worker`) that opens its own
`orch_context` and calls `flush_task_logs`. One code path for local (chdb) and
distributed (remote CH) modes; the spawn cost is per shell-task attempt and
negligible next to the container/pod lifecycle around it.

Timing: the flush happens at the end of the vehicle `wait()` (where the file
capture happens today). A run killed before `wait()` completes loses its tail —
identical to today's file behavior.

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
