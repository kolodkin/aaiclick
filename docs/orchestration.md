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

See `aaiclick/examples/orchestration_basic.py` for a full example.

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

# DB Models

**Implementation**: `aaiclick/orchestration/models.py`

All entities use **Snowflake IDs** via ClickHouse [`generateSnowflakeID()`](https://clickhouse.com/docs/sql-reference/functions/uuid-functions#generateSnowflakeID) — distributed, time-ordered, no DB round-trip. Stored as `BIGINT`.

## Status Enums

| Enum           | Values                                          |
|----------------|------------------------------------------------|
| `JobStatus`    | PENDING, RUNNING, COMPLETED, FAILED, CANCELLED  |
| `TaskStatus`   | PENDING, CLAIMED, RUNNING, COMPLETED, FAILED, CANCELLED |
| `WorkerStatus` | ACTIVE, IDLE, STOPPED                            |
| `RunType`      | SCHEDULED, MANUAL                                |

## Entities

- **RegisteredJob** — job catalog entry; fields: `id`, `name` (unique), `entrypoint`, `enabled`, `schedule` (cron), `default_kwargs` (JSON), `next_run_at`, `created_at`, `updated_at`
- **Job** — a named workflow run; fields: `id`, `name`, `status`, `run_type`, `registered_job_id` (FK), `created_at`, `started_at`, `completed_at`, `error`
- **Task** — single executable unit; fields: `id`, `job_id`, `group_id`, `entrypoint`, `kwargs` (JSONB), `status`, `result` (JSONB), `log_path`, `error`, `worker_id`, timestamps
- **Group** — logical task grouping with optional nesting via `parent_group_id`; fields: `id`, `job_id`, `parent_group_id`, `name`, `created_at`
- **Dependency** — composite PK `(previous_id, previous_type, next_id, next_type)`; types are `'task'` or `'group'`; supports all four combinations
- **Worker** — active worker process; fields: `id`, `hostname`, `pid`, `status`, `last_heartbeat`, `tasks_completed`, `tasks_failed`, `started_at`

## Task Parameter Serialization

Task kwargs and results are stored as JSONB via `_serialize_ref()` on Object/View — see `aaiclick/data/object.py`.

**Object ref**: `{"object_type": "object", "table": "t123...", "job_id": 789}`

**View ref**: Adds `where`, `limit`, `offset`, `order_by`, `selected_fields` fields.

`job_id` enables ownership tracking — `lifecycle.claim()` releases the job-scoped pin ref during deserialization.

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

Workers detect cancellation by polling task status. **Known limitation**: CPU-bound tasks won't interrupt until they yield.

# Registered Jobs

**Implementation**: `aaiclick/orchestration/registered_jobs.py`, `aaiclick/orchestration/models.py` — see `RegisteredJob`

Catalog of known jobs, separate from individual runs. Each entry stores entrypoint, optional cron schedule, default kwargs, and enabled flag.

## Registration & CRUD

- `register_job(name, entrypoint, schedule, default_kwargs, enabled)` — create a new catalog entry
- `get_registered_job(name)` — lookup by name
- `upsert_registered_job(...)` — insert or update
- `enable_job(name)` / `disable_job(name)` — toggle enabled, recompute `next_run_at`
- `list_registered_jobs(enabled_only)` — list all or enabled only

## run_job

`run_job(name, entrypoint, kwargs)` — auto-registers if not found, merges `kwargs` over `default_kwargs`, creates a Job with `run_type=MANUAL` and `registered_job_id` FK, plus the entry point Task.

## Cron Scheduling

**Implementation**: `aaiclick/orchestration/background/background_worker.py` — see `BackgroundWorker._check_schedules()`

`BackgroundWorker` polls enabled jobs where `next_run_at <= NOW()` (~10s). Optimistic locking on `next_run_at` prevents duplicates. Cron parsed by `croniter`; `next_run_at` recomputed on registration, enable, and after each run.

# CLI

**Implementation**: `aaiclick/orchestration/cli.py`, `aaiclick/__main__.py`

## Local Mode (chdb + SQLite)

Single process runs worker + background cleanup together, sharing one chdb
session.  No infrastructure required — just `setup` and `local start`.

```bash
python -m aaiclick setup                      # Initialize local DB + chdb
python -m aaiclick local start                # Worker + background in one process
python -m aaiclick local start --max-tasks 10
python -m aaiclick local stop <worker_id>     # Graceful stop
```

## Distributed Mode (ClickHouse + PostgreSQL)

Workers and background run as independent processes.  Each task executes
in a dedicated child process for isolation.

```bash
python -m aaiclick worker start               # Start a worker process
python -m aaiclick worker start --max-tasks 10
python -m aaiclick worker stop <worker_id>    # Graceful stop
python -m aaiclick worker list
python -m aaiclick background start           # Standalone background worker
```

!!! warning "`worker start` and `background start` require distributed backends"
    Running these commands in local mode (chdb + SQLite) raises `RuntimeError`.
    Use `local start` instead.

## Common Commands

```bash
python -m aaiclick job get <id>
python -m aaiclick job cancel <id>
python -m aaiclick job list [--status RUNNING] [--like "%etl%"] [--limit 20 --offset 40]
python -m aaiclick job enable <name>          # Enable a registered job
python -m aaiclick job disable <name>         # Disable a registered job
python -m aaiclick register-job <entrypoint> [--name NAME] [--schedule "0 8 * * *"] [--kwargs '{"key": "val"}']
python -m aaiclick run-job <name> [--kwargs '{"key": "val"}']
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

In distributed mode, Object table lifecycle is managed through PostgreSQL with explicit ownership transfer via pin/claim.

```
Worker Process
├── OrchLifecycleHandler (per task, uses get_sql_session())
│   ├── incref/decref → table_context_refs (context_id = task_id, job_id = NULL)
│   ├── pin → sets job_id on existing ref row (no refcount change)
│   └── stop → drains queue (no inline drops, no bulk DELETE)
├── task_scope exit
│   ├── decrefs ALL live objects uniformly (deterministic cleanup)
│   └── stale-marks all → __del__ becomes a no-op
├── BackgroundWorker (sole cleanup authority)
│   ├── polls completed/failed jobs → clears job_id on pin refs
│   ├── polls table_context_refs → DROP where refcount <= 0 AND job_id IS NULL
│   └── detects dead workers → marks tasks FAILED
└── orch_context() — SQL engine shared via get_sql_session()
```

## Ownership Transfer (Pin/Claim)

```
Task A executes
  ├── incref intermediates → (table_name, task_a.id, N) rows in SQL
  ├── PIN result: sets job_id on (t_result, task_id) — marks as pinned
  └── task_scope exit:
      ├── decref ALL objects → refcount goes to 0
      ├── job_id marker protects pinned tables from background cleanup
      └── stale-mark all → __del__ is a no-op

Task B starts, deserializes task_a.result
  ├── incref → (t_result, task_b.id, 1)  ← consumer owns it
  └── claim → clears job_id on pin row  ← release pin marker

Job completes → BackgroundWorker deletes remaining refs + drops orphaned tables
```

## OrchLifecycleHandler

**Implementation**: `aaiclick/orchestration/orch_context.py` — see `OrchLifecycleHandler` class

Uses `task_id` as `context_id`; pin operations use `job_id`. On `task_scope` exit, all objects are decreffed uniformly. Pinned tables are protected by `job_id` — background worker skips where `MAX(job_id) IS NOT NULL` (sole cleanup authority).

**Pin lifecycle**: `pin()` sets `job_id` on existing ref row (no refcount bump). `claim()` clears it. Job completion clears remaining pins via `_cleanup_completed_jobs`.

**PostgreSQL table**: `TableContextRef` — composite PK `(table_name, context_id)` with `refcount` and nullable `job_id`.

## BackgroundWorker

**Implementation**: `aaiclick/orchestration/background/background_worker.py` — see `BackgroundWorker` class

Four operations per poll:

1. **Job cleanup** — delete job-scoped pin refs for completed/failed jobs
2. **Table cleanup** — `HAVING SUM(refcount) <= 0` → DROP in CH
3. **Dead worker detection** — expired heartbeats → mark tasks FAILED, workers STOPPED
4. **Job scheduling** — create Job runs for registered jobs whose `next_run_at` is due

Config: `poll_interval` (default 10s), `worker_timeout` (default 90s).

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

- **Setup (local)**: `python -m aaiclick setup`
- **Migrations (PostgreSQL)**: `python -m aaiclick migrate upgrade head` — see `aaiclick/orchestration/migrate.py`
