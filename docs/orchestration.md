aaiclick Orchestration Backend Specification
---

# Basic Example

```python
from aaiclick.orchestration import job, task, job_test, TaskResult

@task
async def add(a: int, b: int) -> int:
    return a + b

@task
async def multiply(x: int, y: int) -> int:
    return x * y

@job("pipeline")
def pipeline(x: int, y: int):
    sum_result = add(a=x, b=y)      # no dependency between tasks → run in parallel
    product = multiply(x=x, y=y)
    return TaskResult(tasks=[sum_result, product])

j = pipeline(x=3, y=4)
job_test(j)  # execute synchronously (testing/local)
```

See `aaiclick/orchestration/examples/orchestration_basic.py` for a full example.

# Decorators

**Implementation**: `aaiclick/orchestration/decorators.py` — see `TaskFactory` and `JobFactory`

Airflow-style TaskFlow API. Passing a Task result as an argument creates an upstream dependency automatically.

## @task

Wraps an async function into a `TaskFactory`. Parameters: `name` (default: function name), `max_retries` (default: `0`). On failure with retries remaining, resets the task to PENDING with incremented `attempt`.

## @job

Wraps a workflow function into a `JobFactory`. Auto-manages `orch_context()` and commits all tasks to SQL. Use `@job("name")`, `@job(name="name")`, or bare `@job`.

**Job testing**: `job_test(job)` and `ajob_test(job)` execute synchronously (`aaiclick/orchestration/execution/debug.py`).

# Deployment Modes

Two deployment modes, controlled by two independent environment variables:

| Aspect              | Local (default)                              | Distributed                                         |
|---------------------|----------------------------------------------|-----------------------------------------------------|
| **Data backend**    | chdb (embedded ClickHouse)                   | ClickHouse server                                   |
| **SQL backend**     | SQLite via aiosqlite                         | PostgreSQL via asyncpg                              |
| **SQL URL**         | `sqlite+aiosqlite:///~/.aaiclick/local.db`   | `postgresql+asyncpg://user:pass@host:5432/database` |
| **Setup**           | `python -m aaiclick setup`                   | Provision servers + `python -m aaiclick migrate upgrade head` |
| **Task claiming**   | Sequential SELECT + UPDATE                   | Atomic CTE with `FOR UPDATE SKIP LOCKED`            |
| **Table lifecycle** | `LocalLifecycleHandler` (background thread)  | `OrchLifecycleHandler` (SQL refcounts)              |
| **Detection**       | `is_chdb()` / `is_sqlite()` return `True`    | Both return `False`                                 |

**Implementation**: `aaiclick/backend.py` — see `get_ch_url()`, `get_db_url()`, `is_chdb()`, `is_sqlite()`

# Runners & Entry Types

**Implementation**: `aaiclick/orchestration/runner_config.py` (typed configs), `aaiclick/orchestration/docker_config.py` (resolution helpers)

Two orthogonal dials control how a task runs:

- **runner mode** (per *job*) — `subprocess` (host child process), `docker` (container), or `kubernetes` (Pod).
- **entry type** (per *task*) — `module` (import and run a Python entrypoint, the default) or `shell` (run a literal argv).

They compose freely: a `shell` task runs the same way — "run this argv, success = exit 0" — whether the environment is a host subprocess, a container, or a Pod.

## Image source (docker / kubernetes)

The container runners run against an image from one of two sources, nested in the job's runner config:

| Source     | How                                              | When built             |
|------------|--------------------------------------------------|------------------------|
| `build`    | git repo → `aaiclick-job:<sha>` image built from `git clone` + `docker build` | on demand at dispatch  |
| `prebuilt` | `image="python:3.12"` run verbatim, no build stage | never                  |

Pass `image=` (`run_job` / `RunJobRequest` / `run-job --image`, or `register-job --image` for a default) to select a prebuilt image — **mutually exclusive** with the git build fields (`git_remote` / `git_sha` / `git_branch` / `dockerfile`).

For a `build` source the image is **not** produced by a task in the job graph. The first worker to dispatch a container/Pod task builds it on demand via `ensure_image`, recorded in the `build_tasks` table keyed by image identity (`sha256(git_remote, git_sha, dockerfile)`). A `UNIQUE(image_key)` constraint plus an atomic claim/lease make the build run **exactly once** across all workers and jobs — every other task (including dynamically created ones) reuses the same tag, and a concurrent worker polls until the build is `READY`.

**Worker prerequisites** — because the build and the `docker run` happen on the worker's host, not in a separate service:

- **`docker` runner** — every worker that may run the job needs a reachable **Docker daemon + CLI** (`AAICLICK_DOCKER_BIN`, default `docker`), for both `build` (to build the image) and `prebuilt` (to `docker run` it).
- **`kubernetes` runner, `build` source** — the worker needs Docker **and** registry access (`AAICLICK_REGISTRY`): it builds locally and **pushes**, then the Pod pulls. Without a registry, each worker host can only run tasks it built itself.
- **`kubernetes` runner, `prebuilt` source** — no Docker on the worker; it only needs cluster access (`kubectl`), and the cluster pulls the image.
- **`subprocess` runner** — no Docker at all.

**Implementation**: `aaiclick/orchestration/execution/image_builder.py` — see `ensure_image()`, `resolve_image_tag()`; `aaiclick/orchestration/docker_config.py` — see `resolve_runner_config()`, `effective_image_tag()`, `image_key()`

## Shell entry type

A `shell` task runs a literal argv (`command`, a list) directly in the runner's environment instead of importing a Python `entrypoint`. Success is **exit code 0**; there is no `result.data()` and `result_ref` is always `None`. stdout/stderr lands in the normal per-task log file, so logs surface uniformly.

In an isolated environment (container/Pod) a shell task receives **only** `command_env` (a dict) — *not* the aaiclick runner env — so no DB credentials leak into an arbitrary image. The subprocess runner has no isolation boundary, so the command inherits the worker's process env with `command_env` overlaid.

| Task                          | Env injected                                  |
|-------------------------------|-----------------------------------------------|
| `module` (any runner)         | `build_runner_env()` — DB URLs + framework knobs |
| `shell` + docker / kubernetes | `command_env` only (on top of the image's env) |
| `shell` + subprocess          | worker process env + `command_env` overlay     |

Submit via:

```python
run_job(name, entry_type="shell", command=["python", "main.py"], command_env={"K": "v"})
```

```bash
python -m aaiclick run-job <name> --entry-type shell --command 'python main.py' --command-env K=v
```

REST/MCP submission uses the same fields on `RunJobRequest`.

**Implementation**: `aaiclick/orchestration/execution/mp_worker.py` — see `_run_shell_on_host()`; `aaiclick/orchestration/execution/docker_worker.py` / `kubernetes_worker.py` — shell branch in the runner vehicle

## Execution layers

The runner's *invocation* sits at a different layer than the task it runs, and the `module` path blurs the two. The same three layers exist for every runner, host → execution environment:

1. **Host worker** — the runner's ExecuteFn (`_run_task_in_child` / `_run_task_in_container` / `_run_task_in_pod`): claim, heartbeat, cancellation poll, wait, result read. Identical for `module` and `shell`.
2. **Runner invocation** — *what the runner launches*: the mp child target, the `docker run <image> <argv>` command, or the Pod `command`. This is the **runner** level, not the task's own definition. **`entry_type` branches here.**
3. **Task execution** — `execute_task(task)` running the module entrypoint.

`python -m aaiclick.orchestration.execution.docker_worker --task-id N` (and the Pod equivalent) is a **layer-2 bootstrap shim** — framework plumbing that only lives in the `docker_worker` module because that module is named for the host worker. Despite "worker" in the path it is *not* a queue-claiming loop and *not* task execution (layer 3): it loads one task by id, boots `orch_context()`, calls `execute_task`, writes the result, and exits.

| entry_type | layer-2 runner invocation                          | layer-3 execution               |
|------------|----------------------------------------------------|---------------------------------|
| `module`   | bootstrap shim (mp child / `docker_worker --task-id N` / Pod shim) | shim calls `execute_task` |
| `shell`    | the user's argv directly — the definition *is* the invocation | none — the argv *is* the execution |

So for a `module` task the user's entrypoint runs *inside* the shim; for a `shell` task the user's argv *replaces* the shim, bypassing both the bootstrap and `execute_task`, in whatever environment the runner provides.

**Implementation**: `aaiclick/orchestration/execution/dispatch.py` (ExecuteFn routing) — see `_run_task_in_child()` (`mp_worker.py`), `_run_task_in_container()` (`docker_worker.py`), `_run_task_in_pod()` (`kubernetes_worker.py`)

# DB Models

**Implementation**: `aaiclick/orchestration/models.py`

All entities use **Snowflake IDs** via ClickHouse [`generateSnowflakeID()`](https://clickhouse.com/docs/sql-reference/functions/uuid-functions#generateSnowflakeID) — distributed, time-ordered, no DB round-trip. Stored as `BIGINT`.

## Status Enums

| Enum           | Values                                          |
|----------------|-------------------------------------------------|
| `JobStatus`    | PENDING, RUNNING, COMPLETED, FAILED, CANCELLED  |
| `TaskStatus`   | PENDING, CLAIMED, RUNNING, COMPLETED, FAILED, CANCELLED, PENDING_CLEANUP, UPSTREAM_FAILED |
| `WorkerStatus` | ACTIVE, IDLE, STOPPING, STOPPED                 |
| `RunType`      | SCHEDULED, MANUAL                               |

Two `TaskStatus` values are set by the background sweep, not the worker:
`PENDING_CLEANUP` (transient — a failed or dead-worker run awaiting ref cleanup
before it retries to PENDING or settles to FAILED) and `UPSTREAM_FAILED`
(terminal — a pending task whose transitive upstream failed or was cancelled).

## Entities

- **RegisteredJob** — job catalog entry; fields: `id`, `name` (unique), `entrypoint`, `enabled`, `schedule` (cron), `default_kwargs` (JSON), `preservation_mode`, `next_run_at`, `created_at`, `updated_at`
- **Job** — a named workflow run; fields: `id`, `name`, `status`, `run_type`, `registered_job_id` (FK), `preservation_mode`, `created_at`, `started_at`, `completed_at`, `error`
- **Task** — single executable unit; fields: `id`, `job_id`, `group_id`, `entrypoint`, `kwargs` (JSONB), `status`, `result` (JSONB), `log_path`, `error`, `worker_id`, `run_epoch` (fencing token bumped by `clear_task`), timestamps
- **Group** — logical task grouping with optional nesting via `parent_group_id`; fields: `id`, `job_id`, `parent_group_id`, `name`, `created_at`
- **Dependency** — composite PK `(previous_id, previous_type, next_id, next_type)`; types are `'task'` or `'group'`; supports all four combinations
- **Worker** — active worker process; fields: `id`, `hostname`, `pid`, `status`, `last_heartbeat`, `tasks_completed`, `tasks_failed`, `started_at`

## Task Parameter Serialization

Task kwargs and results are stored as JSONB via `_serialize_ref()` on Object/View — see `aaiclick/data/object.py`.

**Object ref**: `{"object_type": "object", "table": "t123...", "job_id": 789}`

**View ref**: Adds `where`, `limit`, `offset`, `order_by`, `selected_fields` fields.

`job_id` marks the producing task's job — the background worker skips tables with a non-NULL `job_id` pin until the job completes. Schema is reconstructed from ClickHouse column comments via `_get_table_schema()`.

**Return values**: `None` → `null`; Object/View → serialized ref + `job_id`; any other value → auto-converted via `create_object_from_value()`. See `aaiclick/orchestration/execution/runner.py` — `serialize_task_result()`.

# Task Execution

**Implementation**: `aaiclick/orchestration/execution/`

## Worker Main Loop

**Implementation**: `aaiclick/orchestration/execution/worker.py` — see `worker_main_loop()`

Polls for tasks, executes, updates status. Handles registration, heartbeats (30s), graceful shutdown, and per-task lifecycle creation.

## Task Claiming

**Implementation**: `aaiclick/orchestration/execution/claiming.py` — see `claim_next_task()`, `pg_handler.py`, `sqlite_handler.py`

Finds the oldest pending task with all dependencies satisfied and atomically claims it. Transitions job PENDING→RUNNING on first claim. PostgreSQL uses `FOR UPDATE SKIP LOCKED`; SQLite uses sequential SELECT + UPDATE.

# Job Management

**Implementation**: `aaiclick/orchestration/jobs/`

- `queries.py` — `get_job()`, `list_jobs(status, name_like, limit, offset)`, `count_jobs()`, `get_tasks_for_job()`
- `stats.py` — `compute_job_stats()`, `print_job_stats()`
- `cancel_job(job_id)` — atomically cancels a job and all non-terminal tasks; returns `True` if cancelled, `False` if not found or already terminal. See `execution/claiming.py`.
- `clear_task(task_id)` — resets a task and all its transitive downstream tasks to PENDING for re-run (Airflow-style "clear task"); leaves upstream tasks and their output tables untouched, and reactivates a terminal job to RUNNING. Each reset bumps the task's `run_epoch` fence so an in-flight worker's late writes are rejected. See `execution/claiming.py`.

Workers detect cancellation by polling task status. **Known limitation**: CPU-bound tasks won't interrupt until they yield.

# Registered Jobs

**Implementation**: `aaiclick/orchestration/registered_jobs.py`, `aaiclick/orchestration/models.py` — see `RegisteredJob`

Catalog of known jobs, separate from individual runs. Each entry stores entrypoint, optional cron schedule, default kwargs, preservation-mode defaults, and enabled flag.

## Registration & CRUD

- `register_job(name, entrypoint, schedule, default_kwargs, preservation_mode, enabled)` — create a new catalog entry
- `get_registered_job(name)` — lookup by name
- `upsert_registered_job(...)` — insert or update
- `enable_job(name)` / `disable_job(name)` — toggle enabled, recompute `next_run_at`
- `list_registered_jobs(enabled_only)` — list all or enabled only

## Preservation Config Precedence

`preservation_mode` follows a four-level precedence chain resolved by `factories.resolve_job_config()`:

| Level | Source                                        | Wins when                                    |
|-------|-----------------------------------------------|-----------------------------------------------|
| 1     | Explicit `run_job(...)` / `create_job(...)` argument | The caller passes a non-`None` value   |
| 2     | `RegisteredJob.preservation_mode`             | The registered job carries a default          |
| 3     | `AAICLICK_DEFAULT_PRESERVATION_MODE` env var  | Set in environment                            |
| 4     | `PRESERVATION_NONE`                           | Hardcoded fallback                            |

`None` at any level means "inherit from the next level"; an explicit mode terminates the chain.

Scheduled runs inherit the registered job's level-2 defaults automatically. Manual runs via `run_job()` or the CLI can override at level 1.

## run_job

`run_job(name, entrypoint, kwargs, preservation_mode)` — links to the existing `RegisteredJob` if one matches `name` (and merges `kwargs` over its `default_kwargs`); otherwise runs standalone with `registered_job_id=None`. Resolves preservation config via the precedence chain above and creates a Job with `run_type=MANUAL` plus the entry point Task. See [DataContext — Preservation Modes](data_context.md#preservation-modes) for the two modes' semantics.

## Cron Scheduling

**Implementation**: `aaiclick/orchestration/background/background_worker.py` — see `BackgroundWorker._check_schedules()`

`BackgroundWorker` polls enabled jobs where `next_run_at <= NOW()` (~10s). Optimistic locking on `next_run_at` prevents duplicates. Cron parsed by `croniter`; `next_run_at` recomputed on registration, enable, and after each run.

# CLI

**Implementation**: `aaiclick/orchestration/cli.py`, `aaiclick/__main__.py`

## Local Mode (chdb + SQLite)

Single process, no infrastructure required. `local start` runs the
combined REST + MCP server with the background and execution workers
wired into the FastAPI lifespan — auto-runs setup if needed.

```bash
python -m aaiclick local start [--host HOST] [--port PORT] [--reload]
# Stop with Ctrl+C / SIGTERM — the lifespan tears down workers cleanly.
```

## Distributed Mode (ClickHouse + PostgreSQL)

Independent processes; tasks run in child processes for isolation.

```bash
python -m aaiclick worker start [--max-tasks N]
python -m aaiclick worker stop <worker_id>
python -m aaiclick worker list
python -m aaiclick background start
```

!!! warning "`worker start`/`background start` require distributed backends"
    In local mode, use `local start` instead.

## Common Commands

```bash
python -m aaiclick job get <id>
python -m aaiclick job cancel <id>
python -m aaiclick job list [--status RUNNING] [--like "%etl%"] [--limit 20 --offset 40]
python -m aaiclick job enable <name>          # Enable a registered job
python -m aaiclick job disable <name>         # Disable a registered job
python -m aaiclick register-job <entrypoint> [--name NAME] [--schedule "0 8 * * *"] [--kwargs '{"key": "val"}'] [--preservation-mode NONE|FULL] [--runner subprocess|docker|kubernetes] [--image python:3.12]
python -m aaiclick run-job <name> [--kwargs '{"key": "val"}'] [--preservation-mode NONE|FULL] [--entry-type module|shell] [--command 'python main.py'] [--command-env K=v] [--image python:3.12]
python -m aaiclick registered-job list        # List registered jobs
```

# Orchestration Operators

**Implementation**: `aaiclick/orchestration/operators.py`

| Operator                                                  | Description                                                                 |
|-----------------------------------------------------------|-----------------------------------------------------------------------------|
| `map(cbk, obj, partition, args, kwargs) -> Group`         | Partitions Object into Views, creates N `_map_part` child tasks.            |
| `_map_part(cbk, part, out) -> None`                       | Applies `cbk(row, *args, **kwargs)` to each row in a partition View.        |
| `reduce(cbk, obj, partition, args, kwargs) -> Group`      | Layered parallel reduction. Each layer reduces partitions into one row.     |
| `_expand_reduce(cbk, obj, ...) -> (Object, [Groups])`     | Pre-allocates all layer Objects and tasks at once.                          |
| `_reduce_part(cbk, part, layer_obj) -> None`              | Calls `cbk(partition, output)` — callback writes directly into `layer_obj`. |

## reduce()

Layered parallel reduction. Callback receives input partition and pre-allocated output Object; writes via `output.insert()`. All layers created at once; each layer depends on the previous.

```
Layer 0  input=N      tasks=⌈N/P⌉   → layer_0_obj
Layer 1  input=⌈N/P⌉  tasks=⌈.../P⌉ → layer_1_obj
…continues until 1 row remains
```

Empty input raises `TypeError("reduce() of empty sequence with no initial value")`.

# Distributed Object Lifecycle

**Implementation**: `aaiclick/orchestration/lifecycle/`

In distributed mode, Object table lifecycle is managed through three PostgreSQL tables:

| Table                | PK                         | Purpose                                              |
|----------------------|----------------------------|------------------------------------------------------|
| `table_context_refs` | `(table_name, context_id)` | Registry of which tasks created/used a table         |
| `table_run_refs`     | `(table_name, run_id)`     | Active run references (incref/decref)                |
| `table_pin_refs`     | `(table_name, task_id)`    | Per-consumer pins protecting result tables           |

The background worker is the sole cleanup authority.

```
Worker Process (spawns child per task)
├── OrchLifecycleHandler (per task, per child process)
│   ├── incref → insert into table_context_refs + table_run_refs
│   ├── decref → delete from table_run_refs
│   ├── pin → fan out: insert one pin_ref per downstream consumer
│   └── unpin → delete own pin_ref
├── task_scope exit → decrefs ALL objects, stale-marks
├── BackgroundWorker (sole cleanup authority)
│   ├── DROP where no pin_refs AND no run_refs
│   ├── deletes context_refs + pin_refs alongside dropped tables
│   └── detects dead workers → marks tasks FAILED
└── orch_context() — shared SQL session
```

## Pin Lifecycle

Only at runtime — when a producer task returns an Object — do we know
both the `table_name` and the upstream `task_id`. The downstream consumer
task_ids are discovered via the dependencies table. This is the only point
where the table→task mapping exists.

```
Task A executes, returns Object(table=T)
  ├── PIN fans out via dependencies table:
  │   SELECT next_id FROM dependencies WHERE previous_id = A.id
  │   → inserts pin_ref(T, B.task_id), pin_ref(T, C.task_id)
  └── task_scope exit: decref all → run_refs removed, pin_refs protect T

Task B starts, deserializes T
  ├── incref → run_ref(T, B.run_id)     ← FIFO queue
  └── unpin → delete pin_ref(T, B.task_id)  ← FIFO: after incref

Task C starts, deserializes T
  ├── incref → run_ref(T, C.run_id)
  └── unpin → delete pin_ref(T, C.task_id)

All consumers started → 0 pin_refs, run_refs protect during execution
All consumers finished → 0 pin_refs, 0 run_refs → eligible for cleanup
```

## OrchLifecycleHandler

**Implementation**: `aaiclick/orchestration/orch_context.py` — see `OrchLifecycleHandler` class

Uses `task_id` as `context_id` for context_refs. Pin fans out to downstream consumers via dependencies table. Unpin removes the consumer's own pin_ref row.

**PostgreSQL tables**: `TableContextRef` — composite PK `(table_name, context_id)`; `TableRunRef` — composite PK `(table_name, run_id)`; `TablePinRef` — composite PK `(table_name, task_id)`.

## BackgroundWorker

**Implementation**: `aaiclick/orchestration/background/background_worker.py` — see `BackgroundWorker` class

Five operations per poll:

1. **Job cleanup** — clear `job_id` on pin refs for completed/failed/cancelled jobs
2. **Table cleanup** — DROP tables where no context_ref has non-NULL `job_id` AND no run_refs exist
3. **Dead worker detection** — expired heartbeats → mark tasks FAILED, workers STOPPED
4. **Job scheduling** — create Job runs for registered jobs whose `next_run_at` is due

Config: `poll_interval` (default 10s), `worker_timeout` (default 90s).

## View Lifecycle

Views share the underlying ClickHouse table with their source Object. This interacts with `table_run_refs` in a non-obvious way.

**`table_run_refs` is a set, not a counter.** The composite PK `(table_name, run_id)` means at most one row exists per table per run — `INSERT … ON CONFLICT DO NOTHING`. A second `incref` for the same table in the same run is a no-op; the matching `decref` still deletes the one row.

**Views do not own lifecycle refs.** `_owns_lifecycle_ref = False` on every View. Only the source Object that originally called `_register()` (and actually inserted a row into `table_run_refs`) issues the corresponding `decref`. This prevents a View's `__del__` from deleting the source's run_ref while the source Object is still alive.

**Within-task View lifetime** is managed by Python reference counting: `View.__init__` stores `_source_obj = source`, keeping the source Object alive as long as the View is alive. When the View is GC'd the Python ref is released — if the source has no other refs, it is also GC'd and its `decref` fires normally.

**Cross-task View lifetime** uses the PIN mechanism — the same as for Objects:

```
Task A returns View(table=T)
  ├── pin fans out → pin_ref(T, B.task_id)
  └── task_scope exit → decref(T) [run_refs → 0, pin protects]

Task B deserializes View
  ├── fresh source = Object(table=T); source._register() → INCREF(T)
  ├── view = View(source=source)   [_source_obj keeps source alive]
  └── unpin(T)                     [FIFO: after INCREF commits]
```

The deserialized source Object holds its own `_owns_lifecycle_ref = True` and issues a normal `decref` at `task_scope` exit.

## Write-Ahead Incref

`create_object()` calls `incref` before `CREATE TABLE` — crash between the two is harmless (`DROP TABLE IF EXISTS`).

## TableSweeper

Periodic sweeper: lists `t*` tables in ClickHouse, extracts timestamp from snowflake ID, drops tables older than threshold with no `table_context_refs` row.

## Local Mode

`LocalLifecycleHandler` wraps `TableWorker` — immediate DROP on refcount 0, no PostgreSQL. See [DataContext](data_context.md).

# Operation Provenance (Oplog)

All Object operations within a task are automatically logged when `data_context(oplog=...)` is active. See `docs/oplog.md` for the full specification.

# Configuration

**Implementation**: `aaiclick/backend.py` — see `get_root()`, `is_local()`

| Variable           | Default                              | Description                               |
|--------------------|--------------------------------------|-------------------------------------------|
| `AAICLICK_LOCAL_ROOT`    | `~/.aaiclick`                        | Base directory for all local-mode state   |
| `AAICLICK_SQL_URL` | `sqlite+aiosqlite:///{root}/local.db`| SQLAlchemy async URL for orchestration DB |
| `AAICLICK_CH_URL`  | `chdb://{root}/chdb_data`            | ClickHouse connection URL for data ops    |
| `AAICLICK_LOG_DIR` | mode-dependent (see below)           | Task log directory override               |

`is_local()` returns `True` when `AAICLICK_CH_URL` starts with `chdb://` and `AAICLICK_SQL_URL` starts with `sqlite`.

**Log directory defaults** (see `aaiclick/orchestration/logging.py` — `get_logs_dir()`):

| Mode                | Default               |
|---------------------|-----------------------|
| Local               | `{AAICLICK_LOCAL_ROOT}/logs`|
| Distributed (macOS) | `~/.aaiclick/logs`    |
| Distributed (Linux) | `/var/log/aaiclick`   |

**Cross-host logs**: `capture_task_output` tees task stdout/stderr to the local
file *and* streams it into the ClickHouse `task_logs` table from inside the task
process. It also installs a `logging` handler (taking over the root logger for
the task) so each `logging.*` record is captured with its true `level`; raw
`print()` output defaults to `INFO` (stdout) / `ERROR` (stderr), and
`AAICLICK_LOG_LEVEL` sets the captured root level (default `INFO`). Every row is
tagged with its `stream` (`stdout`/`stderr`), its `level`, and a per-line
`created_at` (emit time, not flush time) so the UI can color by severity and
optionally show timestamps. Because every runner (subprocess, docker,
kubernetes) shares that path, `get_task_logs` reads one host-independent source
regardless of where the task ran — `aaiclick/orchestration/logging.py`,
`aaiclick/oplog/models.py`. The rows are job-scoped: the background worker's
`_delete_job_data` drops a job's `task_logs` alongside its `operation_log` on TTL
expiry, so logs share the job's retention lifecycle.

- **Setup (local)**: `python -m aaiclick setup`
- **Migrations (PostgreSQL)**: `python -m aaiclick migrate upgrade head` — see `aaiclick/orchestration/migrate.py`
