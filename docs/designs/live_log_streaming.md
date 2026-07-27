Live Log Streaming — Incremental task_logs Writes
---

Design for streaming task stdout/stderr to ClickHouse `task_logs` while the
task runs, instead of one batch write after it finishes. Covers every task
type (module and shell) on every runner (subprocess, docker, kubernetes) and
both backends (chdb local, remote ClickHouse distributed) through one code
path.

# Motivation

The read path is already live: the UI polls `GET /tasks/{id}/logs` every 2 s
(global `refetchInterval` in `src/main.tsx`) and `read_task_logs` serves
whatever `task_logs` holds. Only the write side is batch-at-end:

- Module tasks buffer everything in `_ChLogSink` and flush once when the task
  body exits (`capture_task_output` in `aaiclick/orchestration/logging.py`).
- Shell tasks flush once at process exit (`flush_shell_logs` spawn child, or
  inline on the in-process worker).

Consequences: no live tailing of long-running tasks, and a killed run loses
its entire log. The original deferral reason — "keep the writer off the
task's shared chdb client during execution" — turns out to be soft:
`ChdbClient` calls are sync on the event loop (`chdb_client.py`), so a
periodic flusher coroutine and the task body can never be inside chdb at the
same time; the loop serializes them.

# Incremental Sink

`_ChLogSink` gains `drain()` alongside `finalize()`:

- `drain()` returns the completed lines accumulated so far and clears them.
  Partial-line buffers stay untouched — a half-written line is never emitted
  early.
- `finalize()` keeps its behavior (drain plus flush of partials) for the
  final write.

`flush_task_logs` gains a `seq_offset` parameter. Today `seq` restarts at 0
on every insert; incremental flushing threads a running offset so `seq`
stays strictly increasing per `run_id`. The `ORDER BY seq` read path
(`read_task_logs`, including `tail`) works unchanged — no schema change, no
API change, no UI change.

Flush cadence is a module constant, `LOG_FLUSH_INTERVAL = 2.0`, matching the
UI poll interval. Tests monkeypatch it.

# Periodic Flusher (module tasks)

`capture_task_output` starts an asyncio task after installing the tee: every
`LOG_FLUSH_INTERVAL` seconds it drains the sink and calls `flush_task_logs`
with the running offset. The `finally` block cancels the flusher, then does
one last `finalize()` flush. Flush errors stay best-effort (swallowed inside
`flush_task_logs`); a flush failure never fails the task.

Because `capture_task_output` runs inside the task's own process on every
runner — subprocess child, docker container, kubernetes pod — this one
change makes module-task logs live everywhere.

Concurrency by backend:

| Backend                    | Flusher vs. task body                                                     |
|----------------------------|---------------------------------------------------------------------------|
| chdb (local)               | Client calls are sync on the loop — serialized automatically              |
| clickhouse-connect (remote)| Genuinely concurrent; at most one in-flight flush at a time               |

For remote ClickHouse, verify clickhouse-connect's concurrent-use contract
during implementation; if concurrent queries on one `AsyncClient` are not
safe, the flusher opens its own small client in remote mode.

!!! warning "Sync task bodies still flush late"
    A module task whose body is one long sync call blocks the event loop, so
    the flusher starves and logs land at the end — same as today, no
    regression. Async task bodies (the norm) stream live.

# Shell Tasks — One Asyncio Path

A docker or kubernetes *shell* task is, from the worker's perspective, just a
host subprocess whose argv wraps the container tool: `docker run` in the
foreground (with `--rm`) emits the container's merged output on stdout and
exits with the container's exit code; `kubectl run --attach --rm
--restart=Never` does the same for a pod. Shell tasks have no `result.json`
and no IPC mount — the detached-container machinery exists for module tasks
only.

So all shell tasks (host, docker, kubernetes) route through the same
execution path module tasks already use: `execute_task` →
`execute_shell_task`, running inside the chdb-holding process — the mp
worker's task child, or inline on the local server's in-process worker. Every
ClickHouse write happens in a process that legally holds the client.

`execute_shell_task` becomes streaming: register run → spawn the subprocess →
an async line-reader feeds the same `_ChLogSink` → the same periodic flusher
writes through the process's own client → final drain on exit. Module and
shell tasks share the sink, the flusher, and the seq logic.

Argv wrapping happens in the worker parent at dispatch: it already resolves
the image (`ensure_image`) and builds `JobDispatch.command`, so it produces
the runner-specific argv (plain for host shell; `docker run`- or `kubectl
run`-wrapped for container runners, named `aai-task-<task_id>` so cleanup can
address the container/pod — attempts are sequential, so the name is unique
among live containers) and passes it to the task child alongside the task id.

Lifecycle moves into the child, parent as backstop:

- The child enforces the task timeout itself (`asyncio.wait_for`) and polls
  cancellation (it has DB access).
- Cleanup runs in the child's `finally`: `proc.kill()`, plus `docker kill` /
  `kubectl delete pod` for wrapped argv — killing the client process alone
  would leave the container running.
- The parent's existing kill-the-child stays as a last-resort backstop.

Division of responsibilities — the parent's SQL role is unchanged (claiming,
final status updates, worker registration, heartbeats, dispatch); SQL is
accessed from both processes as today:

| Concern                              | Today                          | After                          |
|--------------------------------------|--------------------------------|--------------------------------|
| Claiming, status, heartbeats, dispatch | Parent                       | Parent (unchanged)             |
| `register_run`                       | Child (module) / parent (shell)| Child (all task types)         |
| Shell cancellation poll              | Parent (shell vehicles)        | Child                          |
| Shell timeout enforcement            | Parent `wait()`                | Child; parent kill as backstop |
| CH log writes                        | Child / spawn flush child      | Child (all task types)         |

!!! warning "Hard-killed child can orphan a container"
    If the parent hard-kills the child (backstop path), the child's `finally`
    never runs and a container/pod may be orphaned. Today's parent-side
    cleanup does not have this gap. Mitigation: the child's own timeout fires
    first, so the backstop only matters if the child itself wedges.

# Removal

- Shell branches of `_HostShellVehicle` (`mp_worker.py`), `_DockerVehicle`
  (`docker_worker.py`), `_KubernetesVehicle` (`kubernetes_worker.py`) —
  module branches stay; module containers/pods stream their own logs and
  keep parent-driven lifecycle.
- `log_flush.py` — the spawn-child flush machinery
  (`flush_shell_logs`, `_flush_child_target`) and `flush_shell_logs_inline`.
- `_container_logs_text` / `_pod_logs_text` — output arrives on the wrapper
  process's stdout.

# Distributed Mode

Same code path, different client: `orch_context()` in the task child opens a
clickhouse-connect `AsyncClient` when `AAICLICK_CH_URL` is a remote server.
Every writer on every host lands in the same central `task_logs`, and the API
server's `read_task_logs` already reads from it — the existing cross-host
contract. The only distributed-specific point is the `AsyncClient`
concurrency check above.

# Failure Semantics

- Flushes are best-effort; a failed insert is logged and swallowed, and the
  next tick retries nothing — those lines are gone from CH but the task is
  unaffected (same contract as today's single flush).
- A killed or timed-out run keeps everything flushed up to the last tick —
  strictly better than today, where a killed module task loses its whole log.
- Flusher-task crash inside `capture_task_output` is caught and logged; the
  final `finalize()` flush still runs.

# Testing

Per `python-testing-style`:

- Unit: `drain()` semantics (partials held back), `seq_offset` continuity
  across flushes, `finalize()` after `drain()` totals match.
- Module task: with a monkeypatched short interval, assert logs are readable
  via `read_task_logs` mid-run (task awaits between prints), and totals match
  after completion.
- Shell: mp-worker shell task shows lines before process exit; nonzero exit
  and timeout paths still report `exit <code>` / timeout errors.
- Docker/kubernetes: argv-wrapping builders unit-tested; vehicle-level
  behavior with existing fakes.
- Existing at-exit tests keep passing — the final flush preserves totals.

# Documentation Updates

All docs are rewritten to state what **is** after the change — current
behavior with implementation references — never as before/after refactor
narrative:

- `docs/designs/orchestration.md` — logging/shell-capture sections describe
  the streaming architecture as current state, with implementation
  references.
- `docs/designs/task_log_retirement.md` — capture table updated to the
  streamed reality.
- `docs/user_guide/orchestration.md` — logs section documents live tailing.
- `docs/designs/future.md` — Live Log Streaming item removed; the SSE item's
  `task.log` note stays (SSE remains deferred).
- This spec is deleted once the feature lands (project convention) — the
  code and the docs above are the record.
