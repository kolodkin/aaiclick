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

Workers detect cancellation by polling task status. **Known limitation**: CPU-bound tasks won't interrupt until they yield.

# Registered Jobs

**Implementation**: `aaiclick/orchestration/registered_jobs.py`, `aaiclick/orchestration/models.py` — see `RegisteredJob`

Catalog of known jobs, separate from individual runs. Each entry stores entrypoint, optional cron schedule, default kwargs, preservation-mode / sampling-strategy defaults, and enabled flag.

## Registration & CRUD

- `register_job(name, entrypoint, schedule, default_kwargs, preservation_mode, sampling_strategy, enabled)` — create a new catalog entry
- `get_registered_job(name)` — lookup by name
- `upsert_registered_job(...)` — insert or update
- `enable_job(name)` / `disable_job(name)` — toggle enabled, recompute `next_run_at`
- `list_registered_jobs(enabled_only)` — list all or enabled only

## Preservation Config Precedence

Both `preservation_mode` and `sampling_strategy` follow a four-level precedence chain resolved by `factories.resolve_job_config()`:

| Level | Source                                        | Wins when                                    |
|-------|-----------------------------------------------|-----------------------------------------------|
| 1     | Explicit `run_job(...)` / `create_job(...)` argument | The caller passes a non-`None` value   |
| 2     | `RegisteredJob.preservation_mode` / `.sampling_strategy` | The registered job carries a default  |
| 3     | `AAICLICK_DEFAULT_PRESERVATION_MODE` env var  | Mode only — no global strategy default        |
| 4     | `PreservationMode.NONE` + empty strategy      | Hardcoded fallback                            |

Same chain for both fields. `None` at any level means "inherit from the next level"; an explicit mode / non-empty strategy terminates the chain.

Invariants enforced after resolution:
- `STRATEGY` mode requires a non-empty strategy (at whichever level supplies it)
- A non-empty strategy is only valid under `STRATEGY` mode

Scheduled runs inherit the registered job's level-2 defaults automatically. Manual runs via `run_job()` or the CLI can override at level 1. Replay (Phase 3b) always supplies both explicitly, so it's never affected by the registered job's baseline.

## run_job

`run_job(name, entrypoint, kwargs, preservation_mode, sampling_strategy)` — auto-registers if not found, merges `kwargs` over `default_kwargs`, resolves preservation config via the precedence chain above, creates a Job with `run_type=MANUAL` and `registered_job_id` FK, plus the entry point Task. See [DataContext — Preservation Modes](data_context.md#preservation-modes) for the three modes' semantics.

## Cron Scheduling

**Implementation**: `aaiclick/orchestration/background/background_worker.py` — see `BackgroundWorker._check_schedules()`

`BackgroundWorker` polls enabled jobs where `next_run_at <= NOW()` (~10s). Optimistic locking on `next_run_at` prevents duplicates. Cron parsed by `croniter`; `next_run_at` recomputed on registration, enable, and after each run.

# CLI

**Implementation**: `aaiclick/orchestration/cli.py`, `aaiclick/__main__.py`

## Local Mode (chdb + SQLite)

Single process, no infrastructure required.

```bash
python -m aaiclick local start [--max-tasks N]  # auto-runs setup if needed
python -m aaiclick local stop <worker_id>
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
python -m aaiclick register-job <entrypoint> [--name NAME] [--schedule "0 8 * * *"] [--kwargs '{"key": "val"}'] [--preservation-mode NONE|FULL|STRATEGY] [--sampling-strategy '{"p_foo": "x = 1"}']
python -m aaiclick run-job <name> [--kwargs '{"key": "val"}'] [--preservation-mode NONE|FULL|STRATEGY] [--sampling-strategy '{"p_foo": "x = 1"}']
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
3. **Sample cleanup** — drop expired `_sample` tables older than oplog TTL
4. **Dead worker detection** — expired heartbeats → mark tasks FAILED, workers STOPPED
5. **Job scheduling** — create Job runs for registered jobs whose `next_run_at` is due

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
