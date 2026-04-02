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

Airflow-style TaskFlow API with automatic dependency detection. Passing a Task result as an
argument to another task automatically creates an upstream dependency. Native Python values
(int, float, list, etc.) work alongside Object/View parameters.

## @task

Wraps an async function into a `TaskFactory`. Calling it inside a `@job` body creates a `Task` record. At runtime, workers resolve upstream task results by querying completed task output.

Parameters: `name` (default: function name), `max_retries` (default: `0`). On failure with retries remaining, `_schedule_retry()` resets the task to PENDING with incremented `attempt`.

## @job

Wraps a workflow function into a `JobFactory`. Calling it creates a `Job`, auto-manages `orch_context()`, and commits all tasks to SQL via `commit_tasks()`.

Parameter: `name` (default: function name). Accepts positional (`@job("my_job")`), keyword (`@job(name="my_job")`), or bare (`@job`).

**Job testing**: `job_test(job)` and `ajob_test(job)` execute synchronously for testing. See `aaiclick/orchestration/debug_execution.py`.

# Overview

The aaiclick orchestration backend manages job scheduling, task distribution, and execution coordination. It supports two deployment modes:

- **Local mode** (default): SQLite + chdb — zero infrastructure, single-process execution for development and testing
- **Distributed mode**: PostgreSQL + ClickHouse server — multi-worker execution across processes and machines

# Deployment Modes

The orchestration layer supports two deployment modes, controlled by two independent environment variables:

| Aspect              | Local (default)                              | Distributed                                         |
|---------------------|----------------------------------------------|-----------------------------------------------------|
| **Data backend**    | chdb (embedded ClickHouse)                   | ClickHouse server                                   |
| **Data URL**        | `chdb://~/.aaiclick/chdb_data`               | `clickhouse://user:pass@host:8123/database`         |
| **SQL backend**     | SQLite via aiosqlite                         | PostgreSQL via asyncpg                              |
| **SQL URL**         | `sqlite+aiosqlite:///~/.aaiclick/local.db`   | `postgresql+asyncpg://user:pass@host:5432/database` |
| **Setup**           | `python -m aaiclick setup`                   | Provision servers + `python -m aaiclick migrate upgrade head` |
| **Task claiming**   | Sequential SELECT + UPDATE (no row locking)  | Atomic CTE with `FOR UPDATE SKIP LOCKED`            |
| **Table lifecycle** | `LocalLifecycleHandler` (async task)         | `OrchLifecycleHandler` (SQL refcounts via `get_sql_session()`) |
| **Concurrency**     | Single process                               | Multiple workers across processes/machines          |
| **Detection**       | `is_chdb()` / `is_sqlite()` return `True`    | Both return `False`                                 |

**Implementation**: `aaiclick/backend.py` — see `get_ch_url()`, `get_db_url()`, `is_chdb()`, `is_sqlite()`

## Mixing Backends

The two URL variables are independent — you can mix backends (e.g., remote ClickHouse server + SQLite for orchestration). However, the typical combinations are:

| Combination                        | Use Case                                            |
|------------------------------------|-----------------------------------------------------|
| chdb + SQLite                      | Local development, testing, single-machine scripts  |
| ClickHouse server + PostgreSQL     | Production distributed execution                    |
| ClickHouse server + SQLite         | Single-worker with remote data storage              |

# Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                       Global Resources                           │
│  ┌─────────────────────┐                                         │
│  │  ClickHouse Pool    │  chdb Session (local)                   │
│  │  (urllib3 Pool)     │  OR urllib3 Pool (distributed)          │
│  └─────────────────────┘                                         │
└──────────────────────────────────────────────────────────────────┘
           │
           │
           ▼
┌──────────────────────┐           ┌──────────────────────────┐
│   data_context()     │           │    orch_context()        │
│  (ClickHouse data)   │           │  (Orchestration state)   │
│  ┌────────────────┐  │           │  ┌────────────────────┐ │
│  │ ChClient       │  │           │  │ AsyncEngine        │ │
│  │ (chdb or       │  │           │  │ (SQLite or         │ │
│  │  clickhouse)   │  │           │  │  PostgreSQL)       │ │
│  └────────────────┘  │           │  └────────────────────┘ │
│                      │           │  Creates/disposes on   │
│                      │           │  enter/exit            │
└──────────────────────┘           └──────────────────────────┘
           │                                      │
           │ Objects/Views                        │ Jobs/Tasks/Groups
           ▼                                      ▼
┌────────────────────┐           ┌──────────────────────────────┐
│   ClickHouse       │           │   SQL Database               │
│   (chdb or server) │           │   (SQLite or PostgreSQL)     │
│   (Object data)    │           │  ┌────┐ ┌──────┐ ┌────────┐ │
└────────────────────┘           │  │Jobs│─│Tasks │ │Workers │ │
                                 │  └────┘ └──────┘ └────────┘ │
                                 │  ┌──────┐ ┌──────────────┐  │
                                 │  │Groups│ │Dependencies  │  │
                                 │  └──────┘ └──────────────┘  │
                                 └──────────────────────────────┘
                                             ▲
                                             │ Polls and executes
                                             │
                                 ┌───────────┴────────────────┐
                                 │ Worker Pool (N processes)  │
                                 │ (uses both contexts)       │
                                 └────────────────────────────┘
```

## Dual-Context Design

Both contexts are async context managers using `ContextVar` for async-safe global access:

- **data_context()** (`aaiclick/data/data_context.py`): ClickHouse data operations
  - Connection (`ChClient` — chdb Session or clickhouse-connect AsyncClient)
  - Object tracking (weakref dict — marks Objects stale on exit)
  - Lifecycle/refcounting (`incref`/`decref` for table cleanup)
  - Public API — used directly by user code

- **orch_context()** (`aaiclick/orchestration/context.py`): SQL orchestration state
  - Connection (SQLAlchemy AsyncEngine — aiosqlite or asyncpg, created/disposed per context)
  - `commit_tasks()` — persists task/group DAGs to the SQL database
  - **Not public API** — `@job` decorator manages it automatically (see `JobFactory.__call__()`)

Workers use **both** contexts internally: `data_context()` for data, `orch_context()` for state.

SQL handles orchestration because it provides ACID consistency, row-level locking (`FOR UPDATE SKIP LOCKED`), and foreign keys. ClickHouse handles data because of columnar storage and fast aggregations. SQLite/chdb give the same guarantees locally without running servers.


# Data Models

**Implementation**: `aaiclick/orchestration/models.py`

All entities use **Snowflake IDs** (not database auto-increment) — distributed generation, time-ordered, no DB round-trip. Stored as PostgreSQL `BIGINT`.

## Status Enums

| Enum           | Values                                          |
|----------------|------------------------------------------------|
| `JobStatus`    | PENDING, RUNNING, COMPLETED, FAILED             |
| `TaskStatus`   | PENDING, CLAIMED, RUNNING, COMPLETED, FAILED    |
| `WorkerStatus` | ACTIVE, IDLE, STOPPED                            |

## Job

Represents a workflow containing tasks. See `Job` class in `models.py`.

Key fields: `id`, `name`, `status`, `created_at`, `started_at`, `completed_at`, `error`.

**Status lifecycle**: `PENDING → RUNNING → COMPLETED / FAILED`

## Task

Single executable unit of work. See `Task` class in `models.py`.

Key fields: `id`, `job_id`, `group_id`, `entrypoint` (importable callable like `module.function`), `kwargs` (JSONB — serialized Object/View refs), `status`, `result` (JSONB), `log_path`, `error`, `worker_id`, `created_at`, `claimed_at`, `started_at`, `completed_at`.

**Status lifecycle**: `PENDING → CLAIMED → RUNNING → COMPLETED / FAILED`

## Group

Logical grouping of tasks. Supports nesting via `parent_group_id`. See `Group` class in `models.py`.

Key fields: `id`, `job_id`, `parent_group_id`, `name`, `created_at`.

## Dependency

Unified dependency table — composite PK `(previous_id, previous_type, next_id, next_type)`. Types are `'task'` or `'group'`. Supports all four combinations: task→task, task→group, group→task, group→group.

See `Dependency` class in `models.py`.

## Worker

Active worker process. See `Worker` class in `models.py`.

Key fields: `id`, `hostname`, `pid`, `status`, `last_heartbeat`, `tasks_completed`, `tasks_failed`, `started_at`.

# Task Parameter Serialization

Task kwargs and results are stored as JSONB. Serialization uses polymorphic `_serialize_ref()` on Object/View — see `aaiclick/data/object.py`.

**Object ref**: `{"object_type": "object", "table": "t123...", "job_id": 789}`

**View ref**: Adds `where`, `limit`, `offset`, `order_by`, `selected_fields` fields.

`job_id` enables ownership tracking — `lifecycle.claim()` releases the job-scoped pin ref during deserialization.

## Task Return Values

- **`None`**: `task.result` is `null`
- **Object/View**: Stored via `_serialize_ref()` + `job_id`
- **Any other value**: Auto-converted via `create_object_from_value()`

**Implementation**: `aaiclick/orchestration/execution.py` — see `serialize_task_result()`

# Job Management APIs

**Implementation**: `aaiclick/orchestration/job_queries.py` — see `get_job()`, `list_jobs()`, `count_jobs()`

- `get_job(job_id)` — retrieve a single job by ID
- `list_jobs(status, name_like, limit, offset)` — list jobs with filtering and pagination
- `count_jobs(status, name_like)` — count matching jobs

**CLI**: `python -m aaiclick job get <id>` and `python -m aaiclick job list [--status] [--like] [--limit] [--offset]`

# Job Cancellation

**Implementation**: `aaiclick/orchestration/claiming.py` — see `cancel_job()`, `check_task_cancelled()`

- `cancel_job(job_id)` — cancel a job and all its non-terminal tasks
  - Only PENDING and RUNNING jobs can be cancelled
  - Atomically sets job to CANCELLED and bulk-updates PENDING/CLAIMED/RUNNING tasks to CANCELLED
  - Tasks already COMPLETED or FAILED are preserved
  - Returns `True` if cancelled, `False` if not found or already terminal
- Workers detect cancellation via `_cancellation_monitor()` in `worker.py`, which polls task status and cancels the asyncio.Task via `task.cancel()`

**Known limitation**: asyncio cancellation is cooperative — CPU-bound tasks without `await` points won't be interrupted until they yield.

**CLI**: `python -m aaiclick job cancel <id>`

# Custom Operators

**Implementation**: `aaiclick/orchestration/orch_helpers.py` — see `map()` and `_map_part()` functions

Plain `@task`-decorated functions for parallel data processing. Callbacks are serialized via `_serialize_value()` in `decorators.py` and deserialized via `_deserialize_value()` in `execution.py`.

| Operator                                                  | Description                                                                 |
|-----------------------------------------------------------|-----------------------------------------------------------------------------|
| `map(cbk, obj, partition, args, kwargs) -> Group`         | Partitions Object into Views, creates N `_map_part` child tasks.            |
| `_map_part(cbk, part, out) -> None`                       | Applies `cbk(row, *args, **kwargs)` to each row in a partition View.        |
| `reduce(cbk, obj, partition, args, kwargs) -> Group`      | Layered parallel reduction. Each layer reduces partitions into one row.     |
| `_expand_reduce(cbk, obj, ...) -> (Object, [Groups])`     | Expander: pre-allocates all layer Objects, creates all tasks at once.       |
| `_reduce_part(cbk, part, layer_obj) -> None`              | Calls `cbk(partition, output)` — callback writes directly into `layer_obj`. |

## reduce()

**Implementation**: `aaiclick/orchestration/orch_helpers.py` — see `reduce()`, `_expand_reduce()`, `_reduce_part()`

Layered parallel reduction over an Object. The callback receives both the input partition and
the pre-allocated output Object, and writes results directly into `output` using the native
`output.insert()` API. Each layer partitions the current input into Views, applies the callback
to each partition concurrently, then repeats until one row remains.

All layers, subgroups, and tasks are created at once inside `_expand_reduce` — no lazy
layer-by-layer expansion. `_expand_reduce` returns `(layer_last_obj, [layer_groups])` — the
final single-row Object as task result, plus all layer subgroups as dynamic children.
Each `Group("layer_L+1")` depends on `Group("layer_L")` completing.

### Layer count

Given `N` input rows and `partition` size `P`:

```
Layer 0  input=N    tasks=⌈N/P⌉   → layer_0_obj
Layer 1  input=⌈N/P⌉ tasks=⌈.../P⌉ → layer_1_obj
…continues until 1 row remains
```

**Example — 1300 rows, partition=500:** 2 layers, 4 `_reduce_part` tasks.

**Example — 210 rows, partition=10:** 3 layers, 25 `_reduce_part` tasks.

When the input Object is empty, raises `TypeError("reduce() of empty sequence with no initial value")`.

# Task Execution

## Worker Main Loop

**Implementation**: `aaiclick/orchestration/worker.py` — see `worker_main_loop()`

Workers continuously poll for tasks, execute them, and update status. Handles auto-registration/deregistration, periodic heartbeats (30s), graceful SIGTERM/SIGINT shutdown, and per-task lifecycle handler creation via `lifecycle_factory`.

## Task Claiming

**Implementation**: `aaiclick/orchestration/claiming.py` — see `claim_next_task()`, `aaiclick/orchestration/pg_handler.py`, `aaiclick/orchestration/sqlite_handler.py`

Finds the oldest pending task whose dependencies are all satisfied, atomically claims it, and transitions the parent job PENDING→RUNNING on first claim.

- Checks all four dependency types (task→task, group→task, task→group, group→group)
- Prioritizes oldest running jobs (`ORDER BY j.started_at ASC`)
- **PostgreSQL**: Single atomic CTE with `FOR UPDATE SKIP LOCKED` — safe for concurrent multi-worker claiming
- **SQLite**: Sequential SELECT + UPDATE — sufficient for single-worker local mode

## CLI

**Implementation**: `aaiclick/orchestration/cli.py`, `aaiclick/__main__.py`

```bash
# Worker management
python -m aaiclick worker start               # Start a worker
python -m aaiclick worker start --max-tasks 10 # Limit task count
python -m aaiclick worker list                 # List workers

# Job management
python -m aaiclick job get <id>               # Get job details
python -m aaiclick job cancel <id>            # Cancel a job
python -m aaiclick job list                   # List all jobs
python -m aaiclick job list --status RUNNING  # Filter by status
python -m aaiclick job list --like "%etl%"    # Filter by name pattern
python -m aaiclick job list --limit 20 --offset 40  # Pagination

# Background services
python -m aaiclick background start            # Standalone cleanup worker
```

# Distributed Object Lifecycle

In distributed mode, Object table lifecycle is managed through PostgreSQL with **explicit ownership transfer** via pin/claim.

## Architecture

```
Worker Process
├── OrchLifecycleHandler (per task, asyncio.Task, uses get_sql_session())
│   ├── incref/decref → writes to table_context_refs (context_id = task_id)
│   ├── pin → writes to table_context_refs (context_id = job_id)
│   └── stop → DELETE WHERE context_id = task_id (destructive for intermediates)
├── PgCleanupWorker (asyncio.Task, own PG engine + CH client)
│   ├── polls completed/failed jobs → deletes job-scoped refs
│   ├── polls table_context_refs → DROP TABLE where total refcount <= 0
│   └── detects dead workers → marks tasks FAILED
├── orch_context() (SQL engine for jobs/tasks/workers, shared via get_sql_session())
└── data_context() (per task, ClickHouse)
    └── lifecycle = OrchLifecycleHandler (injected, not owned)
```

`OrchLifecycleHandler` shares the `orch_context()` SQL engine via `get_sql_session()` — no private engine needed.

## Ownership Transfer (Pin/Claim)

```
Task A executes (handler context_id=task_a.id, job_id=job.id)
  ├── incref intermediates → (table_name, task_a.id, N) rows in SQL
  ├── Returns result Object (table t_result)
  ├── data_context() exits → Objects marked stale, handler still alive
  ├── PIN: inserts (t_result, job_id, 1) — survives stop()
  └── stop() → DELETE WHERE context_id=task_a.id (intermediates cleaned)

Task B starts (handler context_id=task_b.id, job_id=job.id)
  ├── Deserializes task_a.result:
  │   ├── incref → inserts (t_result, task_b.id, 1)  ← consumer owns it
  │   └── claim → deletes (t_result, job_id, 1)       ← release job ref
  └── obj_a now owned by Task B's data_context()

Job completes → PgCleanupWorker deletes remaining refs + drops orphaned tables
```

## OrchLifecycleHandler

**Implementation**: `aaiclick/orchestration/context.py` — see `OrchLifecycleHandler` class

Each handler uses the task's own `task_id` as `context_id`. Pin operations use `job_id` as context_id so pinned results survive `stop()`. All SQL operations go through `get_sql_session()` — no private engine.

**Sync-to-async bridge**: `Object.__del__` calls `incref`/`decref` synchronously → `queue.Queue` → asyncio.Task drains via `run_in_executor` → writes to PG.

**PostgreSQL table**: `TableContextRef` in `pg_lifecycle.py` — composite PK `(table_name, context_id)` with `refcount`.

Operations: INCREF (upsert +1), DECREF (update -1), PIN (upsert with job_id), stop() (delete by context_id), claim() (release job-scoped pin).

## PgCleanupWorker

**Implementation**: `aaiclick/orchestration/pg_cleanup.py` — see `PgCleanupWorker` class

Three cleanup operations per poll:
1. **Job cleanup**: Completed/failed jobs → delete job-scoped pin refs
2. **Table cleanup**: `HAVING SUM(refcount) <= 0` → DROP in CH → DELETE from PG
3. **Dead worker detection**: Expired heartbeats → mark tasks FAILED, workers STOPPED

Config: `poll_interval` (default 10s), `worker_timeout` (default 90s).

## Write-Ahead Incref

**Implementation**: `aaiclick/data/data_context.py` — see `create_object()`

`create_object()` calls `incref` **before** `CREATE TABLE` in ClickHouse. Crash after incref but before CREATE → cleanup tries `DROP TABLE IF EXISTS` (harmless no-op).

## TableSweeper

Periodic sweeper: lists `t*` tables in ClickHouse, extracts timestamp from snowflake ID, drops tables older than threshold with no `table_context_refs` row. Complements PgCleanupWorker (catches tables refcount system missed entirely).

# Configuration

See CLAUDE.md for connection URL env vars (`AAICLICK_CH_URL`, `AAICLICK_SQL_URL`).

Orchestration-specific:

- **Log directory**: `AAICLICK_LOG_DIR`, or OS defaults (`~/.aaiclick/logs` macOS, `/var/log/aaiclick` Linux). See `aaiclick/orchestration/logging.py` — `get_logs_dir()`.
- **Setup (local)**: `python -m aaiclick setup` — creates chdb data dir and SQLite database with tables
- **Migrations (PostgreSQL)**: `python -m aaiclick migrate upgrade head` — see `aaiclick/orchestration/migrate.py`
- Legacy env vars (`POSTGRES_HOST`, etc.) are read by Alembic as fallback when `AAICLICK_SQL_URL` is not set

# Operation Provenance (Oplog)

All Object operations within a task are automatically logged when `data_context(oplog=...)` is active. See `docs/oplog.md` for the full specification and `docs/future.md` for planned Phase 3 orchestration integration.

# References

- [SQLModel Documentation](https://sqlmodel.tiangolo.com/)
- [Alembic Documentation](https://alembic.sqlalchemy.org/)
- [PostgreSQL Locking](https://www.postgresql.org/docs/current/explicit-locking.html)

- [Operation Provenance](./oplog.md)
