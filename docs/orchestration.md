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
| **Table lifecycle** | `LocalLifecycleHandler` (in-process refs)    | `TaskLifecycleHandler` (per-task scope, SQL pin/lock) |
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

- **RegisteredJob** — job catalog entry; fields: `id`, `name` (unique), `entrypoint`, `enabled`, `schedule` (cron), `default_kwargs` (JSON), `preserve_all` (BOOLEAN; keeps anonymous `t_*` scratch alive past task exit), `next_run_at`, `created_at`, `updated_at`
- **Job** — a named workflow run; fields: `id`, `name`, `status`, `run_type`, `registered_job_id` (FK), `preserve_all` (BOOLEAN), `created_at`, `started_at`, `completed_at`, `error`
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

Catalog of known jobs, separate from individual runs. Each entry stores entrypoint, optional cron schedule, default kwargs, default `preserve_all` flag, and enabled flag.

## Registration & CRUD

- `register_job(name, entrypoint, schedule, default_kwargs, preserve_all, enabled)` — create a new catalog entry
- `get_registered_job(name)` — lookup by name
- `upsert_registered_job(...)` — insert or update
- `enable_job(name)` / `disable_job(name)` — toggle enabled, recompute `next_run_at`
- `list_registered_jobs(enabled_only)` — list all or enabled only

## Lifecycle

Tasks are conceptually `DataContext`s with two orchestration extras: pin/unpin for cross-task Object handoff, and a per-job `preserve_all` flag that controls whether anonymous ``t_*`` scratch tables survive past task exit. Naming a table is itself the preservation signal — every ``j_<id>_<name>`` is job-scoped and survives the run.

| Table type                              | On successful task exit            | On task failure / worker death | At job completion          |
|-----------------------------------------|------------------------------------|--------------------------------|----------------------------|
| `t_*` not pinned (pure scratch)         | Inline DROP by task                | BackgroundWorker sweeps        | n/a                        |
| `t_*` pinned (Object output)            | Leave (pin-controlled)             | Leave (pin-controlled)         | BackgroundWorker drops     |
| `t_*` with `preserve_all=True`          | Leave                              | Leave                          | BackgroundWorker drops     |
| `j_<id>_<name>` (named, job-scoped)     | Leave (job-end)                    | Leave (job-end)                | BackgroundWorker drops     |
| `p_*` (persistent)                      | Never                              | Never                          | Never (user-managed)       |

See `docs/superpowers/specs/2026-04-25-simplify-orchestration-lifecycle-design.md` for the rationale.

## Declaring preserve_all

`create_job(preserve_all=...)` and `@register_job(preserve_all=...)` declare whether anonymous ``t_*`` scratch tables should survive past task exit. Named ``j_<id>_<name>`` tables are always job-scoped and survive regardless.

```python
job = await create_job(
    "train_embeddings",
    "myapp.train",
    preserve_all=True,  # Tier 2 / full-replay debugging
)
```

`preserve_all` accepts:

- `False` (default) — anonymous `t_*` tables drop at task exit; named tables survive the run.
- `True` — keep anonymous `t_*` tables alive past task exit too.

Resolution order: explicit > `RegisteredJob.preserve_all` > `False`.

Inside a task, `create_object(scope="job", name=...)` issues `CREATE TABLE IF NOT EXISTS j_<job_id>_<name>` — concurrent calls from sibling tasks are naturally idempotent.

## run_job

`run_job(name, entrypoint, kwargs, preserve_all)` — auto-registers if not found, merges `kwargs` over `default_kwargs`, resolves the `preserve_all` value via the precedence rule above, creates a Job with `run_type=MANUAL` and `registered_job_id` FK, plus the entry point Task.

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
python -m aaiclick register-job <entrypoint> [--name NAME] [--schedule "0 8 * * *"] [--kwargs '{"key": "val"}'] [--preservation-mode NONE|FULL]
python -m aaiclick run-job <name> [--kwargs '{"key": "val"}'] [--preservation-mode NONE|FULL]
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

# Object Lifecycle

**Implementation**: `aaiclick/orchestration/lifecycle/task_lifecycle.py`, `aaiclick/orchestration/orch_context.py` — see `task_scope`; `aaiclick/orchestration/background/background_worker.py`

One SQL coordinator sits alongside the per-task `LocalLifecycleHandler`:

| Table            | PK                         | Purpose                                                            |
|------------------|----------------------------|--------------------------------------------------------------------|
| `table_pin_refs` | `(table_name, task_id)`    | Per-consumer pins; producer fans out, consumer deletes its own row |

`table_registry` holds ownership metadata (`table_name`, `job_id`, `task_id`, `advisory_id`, `created_at`, `schema_doc`); it is the join target every cleanup path uses.

## task_scope

`task_scope(task_id, job_id, run_id)` activates a `TaskLifecycleHandler` and on exit (`__aexit__`):

1. Stale-marks every live `Object` so post-scope access raises clearly.
2. Iterates `iter_tracked_tables()` — every table touched in the body, with `(owned, pinned)` flags.
3. Builds a drop list of `t_*` scratch tables that are `owned=True` and `pinned=False`. If the list is non-empty, fetches `Job.preserve_all`; when `False` (default), inline-DROPs each. `j_<id>_<name>` named tables and `p_*` globals are never dropped here.

The handler tracks tables via two synchronous calls:

- `register_table(table_name, schema_doc=...)` — called from `create_object`. Marks `owned=True` and enqueues a `table_registry` row write (with a freshly-minted `advisory_id`).
- `incref(table_name)` — called when an Object is read across tasks. Marks `owned=False` so the consumer doesn't drop the producer's table on its own exit.

Plus the cross-task pin path:

- `pin(table_name)` — sync `mark_pinned(table_name)` plus an OPLOG_PIN message that fans out one `table_pin_refs` row per downstream consumer task (resolved via the `dependencies` table).
- `unpin(table_name)` — consumer-side delete of its own pin_refs row.

## ctx.table

`create_object(scope="job", name=...)` issues `CREATE TABLE IF NOT EXISTS j_<job_id>_<name>` and `track_table(owned=True)`. Concurrent calls from sibling tasks are naturally idempotent — no lock needed.

## BackgroundWorker

**Implementation**: `aaiclick/orchestration/background/background_worker.py` — see `BackgroundWorker` class

Six operations per poll:

1. **Pending cleanup** — process `PENDING_CLEANUP` tasks: drop pin_refs, transition to `PENDING` (retries left) or `FAILED`. Re-queries each affected job and runs `_cleanup_at_job_completion` for any that just transitioned to a terminal status.
2. **Failed task tables** — for `PENDING_CLEANUP` tasks, drop unpinned `t_*` registry rows. Named `j_<id>_*` tables wait for job completion.
3. **Orphan scratch** — sweep `t_*` whose owning task is no longer live and which aren't pinned.
4. **Expired jobs** — TTL sweep based on `AAICLICK_JOB_TTL_DAYS`.
5. **Dead worker detection** — heartbeat cutoff marks tasks `PENDING_CLEANUP`.
6. **Job scheduling** — create new runs for registered jobs whose `next_run_at` is due.

Config: `poll_interval` (default 10s), `worker_timeout` (default 90s).

## View lifecycle

Views share the underlying ClickHouse table with their source Object. They do not own lifecycle refs (`_owns_lifecycle_ref = False`); only the source Object's `_register()` writes a `table_pin_refs` row, so a View's `__del__` cannot strand the source.

Within-task lifetime is Python reference counting — `View.__init__` stores `_source_obj = source`, keeping the source Object alive while any View references it. Cross-task lifetime piggybacks on the same pin mechanism as Objects.

## Local Mode

`LocalLifecycleHandler` wraps `AsyncTableWorker` — refcount-based immediate DROP on count 0, no PostgreSQL. The flag-tracking surface (`track_table`, `mark_pinned`, `iter_tracked_tables`) defaults to no-ops on the base `LifecycleHandler`. See [DataContext](data_context.md).

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
