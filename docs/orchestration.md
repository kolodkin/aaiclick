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

Airflow-style TaskFlow API with automatic dependency detection. Passing a Task result as an argument to another task creates an upstream dependency. Native Python values work alongside Object/View parameters.

## @task

Wraps an async function into a `TaskFactory`. Parameters: `name` (default: function name), `max_retries` (default: `0`). On failure with retries remaining, resets the task to PENDING with incremented `attempt`.

## @job

Wraps a workflow function into a `JobFactory`. Auto-manages `orch_context()` (SQLAlchemy `AsyncEngine`, aiosqlite or asyncpg) and commits all tasks to SQL. Parameter: `name` — accepts positional (`@job("my_job")`), keyword, or bare (`@job`).

**Job testing**: `job_test(job)` and `ajob_test(job)` execute synchronously. See `aaiclick/orchestration/execution/debug.py`.

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
| `JobStatus`    | PENDING, RUNNING, COMPLETED, FAILED             |
| `TaskStatus`   | PENDING, CLAIMED, RUNNING, COMPLETED, FAILED    |
| `WorkerStatus` | ACTIVE, IDLE, STOPPED                            |

## Entities

- **Job** — a named workflow; fields: `id`, `name`, `status`, `created_at`, `started_at`, `completed_at`, `error`
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

Continuously polls for tasks, executes them, and updates status. Handles auto-registration/deregistration, periodic heartbeats (30s), graceful SIGTERM/SIGINT shutdown, and per-task lifecycle handler creation via `lifecycle_factory`.

## Task Claiming

**Implementation**: `aaiclick/orchestration/execution/claiming.py` — see `claim_next_task()`, `pg_handler.py`, `sqlite_handler.py`

Finds the oldest pending task with all dependencies satisfied, atomically claims it, and transitions the parent job PENDING→RUNNING on first claim.

- Prioritizes oldest running jobs (`ORDER BY j.started_at ASC`)
- **PostgreSQL**: Single atomic CTE with `FOR UPDATE SKIP LOCKED`
- **SQLite**: Sequential SELECT + UPDATE — sufficient for single-worker local mode

# Job Management

**Implementation**: `aaiclick/orchestration/jobs/`

- `queries.py` — `get_job()`, `list_jobs(status, name_like, limit, offset)`, `count_jobs()`, `get_tasks_for_job()`
- `stats.py` — `compute_job_stats()`, `print_job_stats()`
- `cancel_job(job_id)` — atomically cancels a job and all non-terminal tasks; returns `True` if cancelled, `False` if not found or already terminal. See `execution/claiming.py`.

Workers detect cancellation by polling task status and cancelling the asyncio.Task. **Known limitation**: CPU-bound tasks without `await` points won't be interrupted until they yield.

# CLI

**Implementation**: `aaiclick/orchestration/cli.py`, `aaiclick/__main__.py`

```bash
python -m aaiclick worker start               # Start a worker
python -m aaiclick worker start --max-tasks 10
python -m aaiclick worker list
python -m aaiclick job get <id>
python -m aaiclick job cancel <id>
python -m aaiclick job list [--status RUNNING] [--like "%etl%"] [--limit 20 --offset 40]
python -m aaiclick background start           # Standalone cleanup worker
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

Layered parallel reduction. Callback receives input partition and pre-allocated output Object; writes results via `output.insert()`. All layers and tasks are created at once inside `_expand_reduce`. Each `Group("layer_L+1")` depends on `Group("layer_L")` completing.

```
Layer 0  input=N      tasks=⌈N/P⌉   → layer_0_obj
Layer 1  input=⌈N/P⌉  tasks=⌈.../P⌉ → layer_1_obj
…continues until 1 row remains
```

**Example — 1300 rows, partition=500:** 2 layers, 4 tasks. **Example — 210 rows, partition=10:** 3 layers, 25 tasks.

Empty input raises `TypeError("reduce() of empty sequence with no initial value")`.

# Distributed Object Lifecycle

**Implementation**: `aaiclick/orchestration/lifecycle/`

In distributed mode, Object table lifecycle is managed through PostgreSQL with explicit ownership transfer via pin/claim.

```
Worker Process
├── OrchLifecycleHandler (per task, uses get_sql_session())
│   ├── incref/decref → table_context_refs (context_id = task_id)
│   ├── pin → table_context_refs (context_id = job_id)
│   └── stop → DELETE WHERE context_id = task_id
├── PgCleanupWorker (own PG engine + CH client)
│   ├── polls completed/failed jobs → deletes job-scoped refs
│   ├── polls table_context_refs → DROP TABLE where total refcount <= 0
│   └── detects dead workers → marks tasks FAILED
└── orch_context() — SQL engine shared via get_sql_session()
```

## Ownership Transfer (Pin/Claim)

```
Task A executes
  ├── incref intermediates → (table_name, task_a.id, N) rows in SQL
  ├── PIN result: inserts (t_result, job_id, 1) — survives stop()
  └── stop() → DELETE WHERE context_id=task_a.id (intermediates cleaned)

Task B starts, deserializes task_a.result
  ├── incref → (t_result, task_b.id, 1)  ← consumer owns it
  └── claim → deletes (t_result, job_id, 1)  ← release job ref

Job completes → PgCleanupWorker deletes remaining refs + drops orphaned tables
```

## OrchLifecycleHandler

**Implementation**: `aaiclick/orchestration/orch_context.py` — see `OrchLifecycleHandler` class

Uses `task_id` as `context_id`; pin operations use `job_id`. SQL via `get_sql_session()`.

**Sync-to-async bridge**: `Object.__del__` calls incref/decref synchronously → `queue.Queue` → asyncio.Task drains via `run_in_executor`.

**PostgreSQL table**: `TableContextRef` in `lifecycle/db_lifecycle.py` — composite PK `(table_name, context_id)` with `refcount`.

## PgCleanupWorker

**Implementation**: `aaiclick/orchestration/lifecycle/pg_cleanup.py` — see `PgCleanupWorker` class

Three cleanup operations per poll: (1) job cleanup — delete job-scoped pin refs for completed/failed jobs; (2) table cleanup — `HAVING SUM(refcount) <= 0` → DROP in CH; (3) dead worker detection — expired heartbeats → mark tasks FAILED, workers STOPPED. Config: `poll_interval` (default 10s), `worker_timeout` (default 90s).

## Write-Ahead Incref

**Implementation**: `aaiclick/data/data_context.py` — see `create_object()`

`create_object()` calls `incref` before `CREATE TABLE`. Crash after incref but before CREATE → cleanup runs `DROP TABLE IF EXISTS` (harmless).

## TableSweeper

Periodic sweeper: lists `t*` tables in ClickHouse, extracts timestamp from snowflake ID, drops tables older than threshold with no `table_context_refs` row.

## Local Mode

Without an injected lifecycle handler, `data_context()` creates `LocalLifecycleHandler` wrapping `TableWorker` — background thread, immediate DROP on refcount 0, no PostgreSQL required. See [DataContext documentation](data_context.md).

# Operation Provenance (Oplog)

All Object operations within a task are automatically logged when `data_context(oplog=...)` is active. See `docs/oplog.md` for the full specification.

# Configuration

See [Getting Started](getting_started.md) for connection URL env vars (`AAICLICK_CH_URL`, `AAICLICK_SQL_URL`).

- **Log directory**: `AAICLICK_LOG_DIR`, or OS defaults (`~/.aaiclick/logs` macOS, `/var/log/aaiclick` Linux). See `aaiclick/orchestration/logging.py` — `get_logs_dir()`.
- **Setup (local)**: `python -m aaiclick setup`
- **Migrations (PostgreSQL)**: `python -m aaiclick migrate upgrade head` — see `aaiclick/orchestration/migrate.py`
