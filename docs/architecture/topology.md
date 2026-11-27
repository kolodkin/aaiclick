Topology
---

Process roles, deployment modes and where task code runs.

# Topology

**Implementation**: `aaiclick/orchestration/local_runtime.py`

<!-- req: process-topology -->
| Component         | Count | Role                                                                 |
|-------------------|-------|----------------------------------------------------------------------|
| PostgreSQL        | 1     | Orchestration + user state: jobs, tasks, workers, auth, table refs   |
| ClickHouse        | 1     | Data factory: Object tables, `task_logs`, oplog                      |
| API server        | 1     | Combined REST + MCP FastAPI app                                      |
| Background worker | 1     | Cron scheduling, dead-worker detection, sole table-cleanup authority |
| Execution worker  | N     | Claims tasks; runs each in a child process / container / Pod         |

Processes never talk to each other: claiming, heartbeats and cancellation polling go through PostgreSQL; data and logs through ClickHouse. Local mode collapses the three process roles into one process (hence exactly one execution worker) on embedded chdb + SQLite.

# Deployment Modes

**Implementation**: `aaiclick/backend.py`

<!-- req: deployment-modes -->
The two modes and their URLs are in [Deployment](../user_guide/deployment.md#deployment-modes).
What changes inside the system between them:

| Aspect              | Local (chdb + SQLite)                     | Distributed (ClickHouse + PostgreSQL)    |
|---------------------|-------------------------------------------|------------------------------------------|
| **Detection**       | `is_local()`: CH URL `chdb://` and SQL URL `sqlite` | otherwise                      |
| **Schema**          | `create_all`, no revision chain           | Alembic revision chain                   |
| **Task claiming**   | Sequential SELECT + UPDATE                | Atomic CTE with `FOR UPDATE SKIP LOCKED` |
| **Table lifecycle** | `LocalLifecycleHandler` (in-process task) | `OrchLifecycleHandler` (SQL refcounts)   |

**One migration chain, not two.** Alembic revisions target PostgreSQL only — they use plain `ALTER` of constraints, which SQLite cannot run without alembic's batch (copy-and-move) mode. Supporting both would mean writing, reviewing and testing every revision twice for a local database meant to be thrown away. Local SQLite is built by `create_all` and recreated, not migrated, when it falls behind (`aaiclick setup --force`).

The cost: locally there is no `alembic_version` row to answer *is this database current?* `setup` builds a throwaway in-memory database from the same `create_all` and diffs the real one against it — tables, column types, indexes, unique constraints, foreign keys. Missing tables (`missing_local_tables`) are added by a plain `create_all`; a table in the wrong shape (`stale_local_db_reason`) means recreating the database, since SQLite cannot `ALTER` its way there.

Accepted limit: anything a revision does that `create_all` would not (a backfill, a data repair) is invisible — local databases are recreated, so there is nothing to backfill.

## Worker runner

**Implementation**: `aaiclick/orchestration/execution/runner_env.py`

<!-- req: worker-runner -->
The runner a task gets ([Runners — Runner modes](../user_guide/orchestration/runners.md#runner-modes))
is a property of the worker's deployment, read once from `AAICLICK_RUNNER`, not
of the job: the compose stack ships the `aaiclick-docker` worker with the host
socket mounted, the helm chart the `aaiclick-kubectl` worker with a
ServiceAccount, and a claimed task runs on whatever its worker has.

- Tasks without an image (`tasks.image_source IS NULL`) run as a host subprocess on every worker.
- A `kubernetes` worker runs the concurrent reconciler of `kubernetes_runner.md` instead of the serial loop.
- No auto-detection: `aaiclick-kubectl` inherits the docker CLI, so an installed binary says nothing.
- `execution-worker start` validates the value and rejects `kubernetes` with `AAICLICK_LOCAL_BUILD` (the cluster cannot pull from the worker's daemon).

# Execution layers

**Implementation**: `aaiclick/orchestration/execution/dispatch.py`

<!-- req: execution-layers -->
A runner's *invocation* sits at a different layer than the task it runs; the `module` path blurs the two. Every runner has the same three layers:

1. **Host worker** — the runner's ExecuteFn (`mp_worker.py`, `docker_worker.py`, `kubernetes_worker.py`): claim, heartbeat, cancellation poll, wait, result read.
2. **Runner invocation** — *what the runner launches*: the mp child target, `docker run <image> <argv>`, or the Pod `command`. **`entry_type` branches here.**
3. **Task execution** — `execute_task(task)` running the module entrypoint.

`python -m aaiclick.orchestration.execution.remote_result --task-id N --run-epoch M` (run by docker containers and Pods) is a **layer-2 bootstrap shim** — not a queue-claiming loop, not task execution: it loads one task by id, boots `orch_context()`, calls `execute_task`, writes a `RemoteTaskResult` row and exits.

| entry_type | layer-2 runner invocation                                                     | layer-3 execution                                                          |
|------------|-------------------------------------------------------------------------------|----------------------------------------------------------------------------|
| `module`   | bootstrap shim (mp child / `remote_result` shim in a container or Pod)        | shim calls `execute_task`                                                  |
| `shell`    | the user's argv directly, with `in.json` / `out.json` under `AAICLICK_IO_DIR` | none — the argv *is* the execution                                         |
| `jvm`      | the image's own `ENTRYPOINT` (the `aaiclick-task-api` shim) + the class name  | the shim reads `in.json`, invokes the `@AaiTask` method, writes `out.json` |

A `module` entrypoint runs *inside* the shim; a `shell` argv *replaces* it, bypassing bootstrap and `execute_task`. A `jvm` task is a shell task whose argv is a class name: the shim is the SDK jar embedded in the user's image (`docs/designs/java-sdk.md`) and speaks the shell file data plane.

# Architecture

<!-- req: server-layers -->
```
         ┌──────── aaiclick/internal_api/ (typed domain functions) ────────┐
         │   list_jobs(filter)  → Page[JobView]                            │
         │   get_job(ref)       → JobDetail                                │
         │   run_job(req)       → JobView                                  │
         │   ...                                                           │
         └───────────────────────────────┬─────────────────────────────────┘
                                         │
             ┌───────────────────────────┼───────────────────────────┐
             ▼                           ▼                           ▼
        CLI renderer              FastAPI routers              FastMCP tools
      (text / --json)          (response_model=...)          (typed return)
      aaiclick/__main__.py     aaiclick/server/routers/      aaiclick/server/mcp.py
```

**Key property**: every command lives in exactly one place. The surfaces only parse input, call `internal_api.*` and render the result natively.
