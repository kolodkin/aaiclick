aaiclick Orchestration Backend Specification
---

# Basic Example

```python
from aaiclick.orchestration import job, task, job_test

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
    return [sum_result, product]

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

Wraps an async function into a `TaskFactory`. Calling it inside a `@job` body creates a `Task`
record. At runtime, workers resolve upstream task results by querying completed task output.

| Parameter     | Type | Default       | Description                           |
|---------------|------|---------------|---------------------------------------|
| `name`        | str  | function name | Human-readable name for created tasks |
| `max_retries` | int  | 0             | Maximum retry attempts on failure     |

**Retry behavior**: When a task fails and has retries remaining, `_schedule_retry()` in
`worker.py` resets it to PENDING with an incremented `attempt` count. Workers pick it up again
via normal claiming.

## @job

Wraps a workflow function into a `JobFactory`. Calling it creates a `Job`, auto-manages
`orch_context()`, and commits all tasks to the SQL database via `commit_tasks()`.

| Parameter | Type | Default       | Description                   |
|-----------|------|---------------|-------------------------------|
| `name`    | str  | function name | Human-readable name for the job |

Accepts name as positional arg (`@job("my_job")`), keyword (`@job(name="my_job")`), or bare
(`@job` — defaults to function name).

**Job testing**: `job_test(job)` and `ajob_test(job)` execute a job synchronously in the
current process for testing/debugging. See `aaiclick/orchestration/debug_execution.py`.

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
| **Table lifecycle** | `LocalLifecycleHandler` (background thread)  | `PgLifecycleHandler` (PostgreSQL refcounts)         |
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

## Task Claiming: SQLite vs PostgreSQL

**Implementation**: `aaiclick/orchestration/db_handler.py` (factory), `sqlite_handler.py`, `pg_handler.py`

Both handlers implement `DbHandler` with identical dependency-checking SQL (`DEPENDENCY_WHERE`). The difference is concurrency control:

- **`SqliteDbHandler`**: Sequential SELECT then UPDATE — safe for single-worker mode since SQLite doesn't support `FOR UPDATE`. Created when `is_sqlite()` is `True`.
- **`PgDbHandler`**: Single atomic CTE with `FOR UPDATE SKIP LOCKED` — enables multiple workers to claim tasks concurrently without conflicts. Created when `is_sqlite()` is `False`.

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

# Job Management APIs ✅ IMPLEMENTED

**Implementation**: `aaiclick/orchestration/job_queries.py` — see `get_job()`, `list_jobs()`, `count_jobs()`

- `get_job(job_id)` — retrieve a single job by ID
- `list_jobs(status, name_like, limit, offset)` — list jobs with filtering and pagination
- `count_jobs(status, name_like)` — count matching jobs

**CLI**: `python -m aaiclick job get <id>` and `python -m aaiclick job list [--status] [--like] [--limit] [--offset]`

# Job Cancellation ✅ IMPLEMENTED

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

| Operator                                  | Status                   | Description                                                   |
|-------------------------------------------|--------------------------|---------------------------------------------------------------|
| `map(cbk, obj, partition, args, kwargs) -> Group` | ✅ IMPLEMENTED           | Partitions Object into Views, creates N `_map_part` child tasks. `args`/`kwargs` forwarded to `cbk`. |
| `_map_part(cbk, part, out) -> None`               | ✅ IMPLEMENTED (internal) | Applies `cbk(row, *args, **kwargs)` to each row in a partition View |
| `reduce()`                                | ⚠️ NOT YET IMPLEMENTED  | Collect and aggregate partition results from a Group          |

## reduce() ⚠️ NOT YET IMPLEMENTED

Layered parallel reduction over an Object. See [docs/reduce.md](reduce.md) for full design.

Each layer partitions the current input into Views, applies the function to each partition
concurrently (all INSERTing into a pre-allocated `layer_obj`), then repeats until one row remains.

All layers, subgroups, and tasks are created at once inside `_expand_reduce` — no lazy
layer-by-layer expansion. `reduce()` returns `_expand_reduce` as a `Task[Object]`; the
task's result value is the final single-row Object. The Group contains one subgroup per
layer (`"layer_0"`, `"layer_1"`, …); each depends on the previous one completing.
No separate terminal task — `_expand_reduce` returns the final Object and all dynamic
children in one shot.

### Layer count

Given `N` input rows and `partition` size `P`, the number of layers is:

```
layers = ⌈log_P(N)⌉
```

Or equivalently:

```python
def num_layers(N: int, P: int) -> int:
    layers = 0
    while N > 1:
        N = ceil(N / P)
        layers += 1
    return layers
```

**Example — 1300 rows, partition=500:**

```
Layer 0  input=1300  tasks=⌈1300/500⌉=3   layer_obj → 3 rows
Layer 1  input=3     tasks=⌈3/500⌉   =1   layer_obj → 1 row  ✓
```
Total: **2 layers**, 4 `_reduce_part` tasks.

**Example — 210 rows, partition=10:**

```
Layer 0  input=210  tasks=⌈210/10⌉=21  layer_obj → 21 rows
Layer 1  input=21   tasks=⌈21/10⌉ = 3  layer_obj →  3 rows
Layer 2  input=3    tasks=⌈3/10⌉  = 1  layer_obj →  1 row  ✓
```
Total: **3 layers**, 25 `_reduce_part` tasks.

## Spark Methods vs aaiclick Capabilities

| Spark Method           | aaiclick Equivalent               | Notes                               |
|------------------------|-----------------------------------|-------------------------------------|
| `map(func)`            | Object operators (`+`, `*`, etc.) | Element-wise SQL operations         |
| `mapPartitions(func)`  | **`map(cbk, obj)`** ✅           | Custom Python logic per partition   |
| `filter(pred)`         | `View(where=...)`                 | SQL WHERE clause                    |
| `groupByKey`           | `obj.group_by(...)`               | SQL GROUP BY                        |
| `count()`              | `obj.count()`                     | SQL COUNT aggregation               |
| `sum/mean/min/max/std` | `obj.sum()` etc.                  | SQL aggregation functions           |
| `union/concat`         | `concat(a, b)`                    | INSERT INTO ... SELECT              |
| `sort/orderBy`         | `View(order_by=...)`              | SQL ORDER BY                        |

Planned (not yet implemented): `reduce()`, `flatMap()`, `join()`.

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
├── PgLifecycleHandler (per task, asyncio.Task, own PG engine)
│   ├── incref/decref → writes to table_context_refs (context_id = auto snowflake)
│   ├── pin → writes to table_context_refs (context_id = job_id)
│   └── stop → DELETE WHERE context_id = handler's snowflake (destructive for intermediates)
├── PgCleanupWorker (asyncio.Task, own PG engine + CH client)
│   ├── polls completed/failed jobs → deletes job-scoped refs
│   ├── polls table_context_refs → DROP TABLE where total refcount <= 0
│   └── detects dead workers → marks tasks FAILED
├── orch_context() (PG engine for jobs/tasks/workers)
└── data_context() (per task, ClickHouse)
    └── lifecycle = PgLifecycleHandler (injected, not owned)
```

Each component owns its own PG engine — fully decoupled from `orch_context()`.

## Ownership Transfer (Pin/Claim)

```
Task A executes (handler context_id=auto_snowflake, job_id=job.id)
  ├── incref intermediates → (table_name, auto_snowflake, N) rows in PG
  ├── Returns result Object (table t_result)
  ├── data_context() exits → Objects marked stale, handler still alive
  ├── PIN: inserts (t_result, job_id, 1) — survives stop()
  └── stop() → DELETE WHERE context_id=auto_snowflake (intermediates cleaned)

Task B starts (handler context_id=auto_snowflake_2, job_id=job.id)
  ├── Deserializes task_a.result:
  │   ├── incref → inserts (t_result, auto_snowflake_2, 1)  ← consumer owns it
  │   └── claim → deletes (t_result, job_id, 1)             ← release job ref
  └── obj_a now owned by Task B's data_context()

Job completes → PgCleanupWorker deletes remaining refs + drops orphaned tables
```

## PgLifecycleHandler

**Implementation**: `aaiclick/orchestration/pg_lifecycle.py` — see `PgLifecycleHandler` class

Each handler gets a unique `context_id` (snowflake). Pin operations use `job_id` as context_id so pinned results survive `stop()`.

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

## TableSweeper ⚠️ NOT YET IMPLEMENTED

Periodic sweeper: lists `t*` tables in ClickHouse, extracts timestamp from snowflake ID, drops tables older than threshold with no `table_context_refs` row. Complements PgCleanupWorker (catches tables refcount system missed entirely).

## Local Mode (chdb + SQLite)

Without injected lifecycle handler, `data_context()` creates `LocalLifecycleHandler` wrapping `TableWorker` — background thread, immediate DROP on refcount 0, no PostgreSQL required. Works with both chdb and remote ClickHouse backends. See [DataContext documentation](data_context.md) — "Table Lifecycle Tracking".

# Configuration

See CLAUDE.md for connection URL env vars (`AAICLICK_CH_URL`, `AAICLICK_SQL_URL`).

Orchestration-specific:

- **Log directory**: `AAICLICK_LOG_DIR`, or OS defaults (`~/.aaiclick/logs` macOS, `/var/log/aaiclick` Linux). See `aaiclick/orchestration/logging.py` — `get_logs_dir()`.
- **Setup (local)**: `python -m aaiclick setup` — creates chdb data dir and SQLite database with tables
- **Migrations (PostgreSQL)**: `python -m aaiclick migrate upgrade head` — see `aaiclick/orchestration/migrate.py`
- Legacy env vars (`POSTGRES_HOST`, etc.) are read by Alembic as fallback when `AAICLICK_SQL_URL` is not set

# References

- [SQLModel Documentation](https://sqlmodel.tiangolo.com/)
- [Alembic Documentation](https://alembic.sqlalchemy.org/)
- [PostgreSQL Locking](https://www.postgresql.org/docs/current/explicit-locking.html)
- [aaiclick Architecture](./aaiclick.md)
