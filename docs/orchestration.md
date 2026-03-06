# aaiclick Orchestration Backend Specification

## Overview

The aaiclick orchestration backend enables distributed execution of data processing workflows across multiple workers. It manages job scheduling, task distribution, and execution coordination using PostgreSQL as the state store.

### Motivation

As aaiclick scales to handle large-scale data processing, we need:
- **Distributed execution**: Parallelize work across multiple workers
- **Dynamic task generation**: Create new tasks during execution (e.g., via `map()` operations)
- **Reliable state management**: Track job progress with crash recovery
- **Ordered execution**: Preserve temporal causality via creation timestamps

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                       Global Resources                           │
│  ┌─────────────────────┐                                         │
│  │  ClickHouse Pool    │                                         │
│  │  (urllib3 Pool)     │                                         │
│  └─────────────────────┘                                         │
└──────────────────────────────────────────────────────────────────┘
           │
           │
           ▼
┌──────────────────────┐           ┌──────────────────────────┐
│   data_context()     │           │    orch_context()        │
│  (ClickHouse data)   │           │  (Orchestration state)   │
│  ┌────────────────┐  │           │  ┌────────────────────┐ │
│  │ ClickHouse     │  │           │  │ AsyncEngine        │ │
│  │ Client         │  │           │  │ (per-context)      │ │
│  └────────────────┘  │           │  └────────────────────┘ │
│                      │           │  Creates/disposes on   │
│                      │           │  enter/exit            │
└──────────────────────┘           └──────────────────────────┘
           │                                      │
           │ Objects/Views                        │ Jobs/Tasks/Groups
           ▼                                      ▼
┌────────────────────┐           ┌──────────────────────────────┐
│   ClickHouse DB    │           │      PostgreSQL Database     │
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

### Dual-Context Design

Both contexts are async context managers using `ContextVar` for async-safe global access:

- **data_context()** (`aaiclick/data/data_context.py`): ClickHouse data operations
  - Connection (AsyncClient via shared urllib3 pool)
  - Object tracking (weakref dict — marks Objects stale on exit)
  - Lifecycle/refcounting (`incref`/`decref` for table cleanup)
  - Public API — used directly by user code

- **orch_context()** (`aaiclick/orchestration/context.py`): PostgreSQL orchestration state
  - Connection (SQLAlchemy AsyncEngine, created/disposed per context)
  - `commit_tasks()` — persists task/group DAGs to PostgreSQL
  - **Not public API** — `@job` decorator manages it automatically (see `JobFactory.__call__()`)

Workers use **both** contexts internally: `data_context()` for data, `orch_context()` for state.

### Why Dual-Database?

**PostgreSQL for orchestration**: ACID consistency, row-level locking (`FOR UPDATE SKIP LOCKED`), JSONB for task params, Alembic migrations, foreign keys.

**ClickHouse for data**: Columnar storage, fast aggregations, scalability for large datasets.

## Data Models

**Implementation**: `aaiclick/orchestration/models.py`

All entities use **Snowflake IDs** (not database auto-increment) — distributed generation, time-ordered, no DB round-trip. Stored as PostgreSQL `BIGINT`.

### Status Enums

| Enum           | Values                                          |
|----------------|------------------------------------------------|
| `JobStatus`    | PENDING, RUNNING, COMPLETED, FAILED             |
| `TaskStatus`   | PENDING, CLAIMED, RUNNING, COMPLETED, FAILED    |
| `WorkerStatus` | ACTIVE, IDLE, STOPPED                            |

### Job

Represents a workflow containing tasks. See `Job` class in `models.py`.

Key fields: `id`, `name`, `status`, `created_at`, `started_at`, `completed_at`, `error`.

**Status lifecycle**: `PENDING → RUNNING → COMPLETED / FAILED`

### Task

Single executable unit of work. See `Task` class in `models.py`.

Key fields: `id`, `job_id`, `group_id`, `entrypoint` (importable callable like `module.function`), `kwargs` (JSONB — serialized Object/View refs), `status`, `result` (JSONB), `log_path`, `error`, `worker_id`, `created_at`, `claimed_at`, `started_at`, `completed_at`.

**Status lifecycle**: `PENDING → CLAIMED → RUNNING → COMPLETED / FAILED`

### Group

Logical grouping of tasks. Supports nesting via `parent_group_id`. See `Group` class in `models.py`.

Key fields: `id`, `job_id`, `parent_group_id`, `name`, `created_at`.

### Dependency

Unified dependency table — composite PK `(previous_id, previous_type, next_id, next_type)`. Types are `'task'` or `'group'`. Supports all four combinations: task→task, task→group, group→task, group→group.

See `Dependency` class in `models.py`.

### Worker

Active worker process. See `Worker` class in `models.py`.

Key fields: `id`, `hostname`, `pid`, `status`, `last_heartbeat`, `tasks_completed`, `tasks_failed`, `started_at`.

## Task Parameter Serialization

Task kwargs and results are stored as JSONB. Serialization uses polymorphic `_serialize_ref()` on Object/View — see `aaiclick/data/object.py`.

**Object ref**: `{"object_type": "object", "table": "t123...", "job_id": 789}`

**View ref**: Adds `where`, `limit`, `offset`, `order_by`, `selected_fields` fields.

`job_id` enables ownership tracking — `lifecycle.claim()` releases the job-scoped pin ref during deserialization.

### Task Return Values

- **`None`**: `task.result` is `null`
- **Object/View**: Stored via `_serialize_ref()` + `job_id`
- **Any other value**: Auto-converted via `create_object_from_value()`

**Implementation**: `aaiclick/orchestration/execution.py` — see `serialize_task_result()`

## User-Facing API

**Implementation**: `aaiclick/orchestration/decorators.py` — see `TaskFactory` and `JobFactory`

### @task and @job Decorators

Airflow-style TaskFlow API with automatic dependency detection. For usage examples, see `aaiclick/examples/orchestration_basic.py`.

**How it works**:
- `@task` wraps async functions into `TaskFactory` — calling it creates Task objects
- Passing a Task as an argument automatically creates an upstream dependency
- `@job("name")` wraps workflow functions into `JobFactory` — creates Job, auto-manages `orch_context()`, commits all tasks to PostgreSQL via `commit_tasks()`
- Native Python values (int, float, list, etc.) work alongside Object/View parameters
- At runtime, workers resolve upstream refs by querying completed task results

### Job Testing

**Implementation**: `aaiclick/orchestration/debug_execution.py` — see `job_test()` and `ajob_test()`

`job_test(job)` executes a job synchronously in the current process for testing/debugging.

### Internal APIs

Not for direct use:
- `create_task()`, `create_job()` — low-level factories in `aaiclick/orchestration/factories.py`
- `commit_tasks()` — commits DAG to PostgreSQL
- `>>` / `<<` dependency operators on Task/Group

### Job Management APIs ✅ IMPLEMENTED

**Implementation**: `aaiclick/orchestration/job_queries.py` — see `get_job()`, `list_jobs()`, `count_jobs()`

- `get_job(job_id)` — retrieve a single job by ID
- `list_jobs(status, name_like, limit, offset)` — list jobs with filtering and pagination
- `count_jobs(status, name_like)` — count matching jobs

**CLI**: `python -m aaiclick job get <id>` and `python -m aaiclick job list [--status] [--like] [--limit] [--offset]`

### Not Yet Implemented

- `cancel_job()` — cancel an in-flight job
- Dynamic task creation during execution (Phase 8+)

## Task Execution

### Worker Main Loop

**Implementation**: `aaiclick/orchestration/worker.py` — see `worker_main_loop()`

Workers continuously poll for tasks, execute them, and update status. Handles auto-registration/deregistration, periodic heartbeats (30s), graceful SIGTERM/SIGINT shutdown, and per-task lifecycle handler creation via `lifecycle_factory`.

### Task Claiming

**Implementation**: `aaiclick/orchestration/claiming.py` — see `claim_next_task()`

Uses PostgreSQL CTE with `FOR UPDATE SKIP LOCKED` for atomic concurrent task claiming:

- Finds oldest pending task whose dependencies are all satisfied
- Checks all four dependency types (task→task, group→task, task→group, group→group)
- Atomically transitions job PENDING→RUNNING on first claim
- Prioritizes oldest running jobs (`ORDER BY j.started_at ASC`)

### Worker CLI

**Implementation**: `aaiclick/orchestration/cli.py`, `aaiclick/__main__.py`

```bash
python -m aaiclick worker start               # Start a worker
python -m aaiclick worker start --max-tasks 10 # Limit task count
python -m aaiclick worker list                 # List workers
python -m aaiclick background start            # Standalone cleanup worker
```

## Distributed Object Lifecycle

In distributed mode, Object table lifecycle is managed through PostgreSQL with **explicit ownership transfer** via pin/claim.

### Architecture

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

### Ownership Transfer (Pin/Claim)

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

### PgLifecycleHandler

**Implementation**: `aaiclick/orchestration/pg_lifecycle.py` — see `PgLifecycleHandler` class

Each handler gets a unique `context_id` (snowflake). Pin operations use `job_id` as context_id so pinned results survive `stop()`.

**Sync-to-async bridge**: `Object.__del__` calls `incref`/`decref` synchronously → `queue.Queue` → asyncio.Task drains via `run_in_executor` → writes to PG.

**PostgreSQL table**: `TableContextRef` in `pg_lifecycle.py` — composite PK `(table_name, context_id)` with `refcount`.

Operations: INCREF (upsert +1), DECREF (update -1), PIN (upsert with job_id), stop() (delete by context_id), claim() (release job-scoped pin).

### PgCleanupWorker

**Implementation**: `aaiclick/orchestration/pg_cleanup.py` — see `PgCleanupWorker` class

Three cleanup operations per poll:
1. **Job cleanup**: Completed/failed jobs → delete job-scoped pin refs
2. **Table cleanup**: `HAVING SUM(refcount) <= 0` → DROP in CH → DELETE from PG
3. **Dead worker detection**: Expired heartbeats → mark tasks FAILED, workers STOPPED

Config: `poll_interval` (default 10s), `worker_timeout` (default 90s).

### Write-Ahead Incref

**Implementation**: `aaiclick/data/data_context.py` — see `create_object()`

`create_object()` calls `incref` **before** `CREATE TABLE` in ClickHouse. Crash after incref but before CREATE → cleanup tries `DROP TABLE IF EXISTS` (harmless no-op).

### TableSweeper ⚠️ NOT YET IMPLEMENTED

Periodic sweeper: lists `t*` tables in ClickHouse, extracts timestamp from snowflake ID, drops tables older than threshold with no `table_context_refs` row. Complements PgCleanupWorker (catches tables refcount system missed entirely).

### Local Mode

Without injected lifecycle handler, `data_context()` creates `LocalLifecycleHandler` wrapping `TableWorker` — background thread, immediate DROP on refcount 0, no PostgreSQL required. See [Object documentation](object.md) — "Table Lifecycle Tracking".

## Configuration

See CLAUDE.md for environment variables. Orchestration-specific:

- **Log directory**: `AAICLICK_LOG_DIR` env var, or OS defaults (macOS: `~/.aaiclick/logs`, Linux: `/var/log/aaiclick`). See `aaiclick/orchestration/logging.py` — `get_logs_dir()`.
- **Migrations**: `python -m aaiclick migrate` — see `aaiclick/orchestration/migrate.py`

## Implementation Status

Phases 1–7 complete. Phase 8+: Dynamic task creation, retry logic ⚠️ NOT YET IMPLEMENTED.

## References

- [SQLModel Documentation](https://sqlmodel.tiangolo.com/)
- [Alembic Documentation](https://alembic.sqlalchemy.org/)
- [PostgreSQL Locking](https://www.postgresql.org/docs/current/explicit-locking.html)
- [aaiclick Architecture](./aaiclick.md)
