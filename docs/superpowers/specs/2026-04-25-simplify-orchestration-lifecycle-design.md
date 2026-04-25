Simplify Orchestration Lifecycle
---

# Motivation

The current orchestration table-lifecycle stack has three independent tracking mechanisms:

1. **Distributed refcounts** (`table_run_refs`) — every Object incref/decref crosses SQL.
2. **Per-context tracking** (`table_context_refs`) — which task touched which table.
3. **Pin/unpin** (`table_pin_refs`) — producer→consumer Object handoff between tasks.

Plus a `PreservationMode` enum (`NONE` / `FULL`) controlling whether `j_<id>_*` tables survive to the job's TTL (~90 days). All cleanup is deferred to the BackgroundWorker via polling.

Meanwhile, `DataContext` already has a clean local lifecycle: weakref tracking, queue-based incref/decref, immediate DROP at refcount 0. Tasks should work the same way: a task is conceptually a `DataContext` with a few extras for cross-task handoff.

This refactor collapses the three tracking mechanisms into one (the `LocalLifecycleHandler` from `DataContext`), keeps pinning for Object outputs, and replaces `PreservationMode` with an explicit `preserve` list of named tables.

# Scope

In scope:

- Replace `OrchLifecycleHandler` with `LocalLifecycleHandler` + a thin `TaskExtensions` wrapper.
- Delete `table_run_refs` and `table_context_refs` SQL tables and all related code paths.
- Trim `table_registry` to `(table_name, job_id, task_id, preserved, created_at)`.
- Add `task_name_locks` SQL table for non-preserved-name collision detection.
- Replace `PreservationMode` enum with `preserve: list[str] | Literal["*"] | None` on `Job` and `RegisteredJob`.
- Inline DROP of task-local tables on successful task exit.
- BackgroundWorker drops all `j_<id>_*` tables at job completion (preserved or not).
- Migrate examples and docs.

Out of scope:

- Changes to ClickHouse table-naming conventions (`t_*` / `j_<id>_*` / `p_*`).
- Changes to the Object class or its serialization format.
- Changes to scheduler / RegisteredJob run-creation logic.
- Job TTL semantics (still applies as a safety net for forgotten metadata).

# Cleanup ownership

Single rule: **only the owning task drops `t_*` and only on the happy path; only BackgroundWorker drops `j_<id>_*`.**

| Table type                              | On successful task exit | On task failure / worker death | At job completion          |
|-----------------------------------------|-------------------------|--------------------------------|----------------------------|
| `t_*` (anonymous / unnamed)             | Inline DROP by task     | BackgroundWorker sweeps        | n/a (gone earlier)         |
| `j_<id>_<name>`, not preserved          | Inline DROP by task     | Leave for job-end cleanup      | BackgroundWorker drops     |
| `j_<id>_<name>`, preserved              | Leave (job-end)         | Leave (job-end)                | BackgroundWorker drops     |
| `j_<id>_<uuid>` (Object output, pinned) | Leave (pin-controlled)  | Leave (pin-controlled)         | BackgroundWorker drops     |
| `p_*` (persistent)                      | Never                   | Never                          | Never (user-managed)       |

Preserved tables die at **job completion**, not at TTL. "Preserved" means "lives for the run", not "lives 90 days."

# Public API

## `preserve` type

```python
# aaiclick/orchestration/models.py
from typing import Literal

Preserve = list[str] | Literal["*"] | None
```

- `None` — nothing preserved (default; pure task-local semantics).
- `["foo", "bar"]` — these named tables survive the run.
- `"*"` — every `j_<id>_<name>` created during the job survives the run.
- `[]` — explicit "nothing preserved"; does NOT fall through to RegisteredJob default.

## Job creation

```python
job = await create_job(
    registered_name="train_embeddings",
    preserve=["training_set", "vocab"],
)
```

**Resolution precedence** (single source of truth in `resolve_preserve(explicit, registered)`):

1. Explicit `create_job(preserve=...)` if not `None`.
2. `RegisteredJob.preserve`.
3. `None`.

## Registered job default

```python
@register_job(
    name="train_embeddings",
    preserve=["training_set", "vocab"],
)
async def train_embeddings(ctx: TaskContext) -> None:
    ...
```

## Table creation inside a task

```python
ctx.table()                  # → t_<snowflake>; task-local; inline-dropped on success
ctx.table("tmp_agg")         # → j_<job_id>_tmp_agg; task-local (acquires TaskNameLock)
ctx.table("training_set")    # → j_<job_id>_training_set; preserved (if in preserve list or preserve == "*")
                             #   no lock; idempotent CREATE IF NOT EXISTS across tasks
```

## New exception

```python
class TableNameCollision(Exception):
    """Raised when a task tries to take a non-preserved name currently held by another live task in the same job."""
    def __init__(self, name: str, held_by_task_id: str): ...
```

## Removed API

- `PreservationMode` enum (`NONE` / `FULL` / `STRATEGY`)
- `resolve_job_config(mode, registered, env)` — replaced by `resolve_preserve(explicit, registered)`
- `AAICLICK_DEFAULT_PRESERVATION_MODE` env var — no replacement; `RegisteredJob.preserve` covers the default-config use case
- `OrchLifecycleHandler` (internal)

## Migration table

| Old                            | New                |
|--------------------------------|--------------------|
| `PreservationMode.NONE`        | `preserve=None`    |
| `PreservationMode.FULL`        | `preserve="*"`     |
| `PreservationMode.STRATEGY`    | `preserve=[...]`   |

`STRATEGY` was never implemented, so the practical migration is: drop `NONE`, rewrite `FULL` as `"*"`.

# Lifecycle flow

## Task lifecycle

```
worker claims task → enters task_scope()
  ↓
LocalLifecycleHandler created, registered in ContextVars
TaskExtensions wraps it: pin/unpin hooks, name-lock acquisition, preserved-name registration
  ↓
Task body runs:
  - ctx.table()           → unnamed t_<snowflake>; incref local
  - ctx.table("foo")      → if "foo" preserved or preserve == "*":
                              j_<id>_foo; mark `preserved=TRUE` in table_registry; no lock
                            else:
                              acquire TaskNameLock(job_id, "foo", task_id)
                                - if held by another live task → raise TableNameCollision
                              j_<id>_foo; incref local; preserved=FALSE
  - return Object(table)  → producer marks TablePinRef rows for declared consumers
  ↓
Task body completes:
  ↓
task_scope.__aexit__ (success path):
  1. Stale-mark all weakref-tracked Objects (existing DataContext pattern)
  2. For each table tracked by LocalLifecycleHandler:
       - if preserved → leave it (BackgroundWorker drops at job end)
       - if pinned    → leave it (BackgroundWorker drops when pin_refs == 0 OR job ends)
       - else         → DROP TABLE inline using task's CH connection
  3. Release TaskNameLock rows for this task
  4. Reset ContextVars

task_scope.__aexit__ (failure path):
  1. Stale-mark weakref-tracked Objects
  2. Drop ONLY t_* tables inline. Leave all j_<id>_* tables (BackgroundWorker handles).
  3. Release TaskNameLock rows for this task
  4. Reset ContextVars
  Task status → PENDING_CLEANUP
```

## Worker death

Process is gone, so nothing inline. BackgroundWorker dead-worker sweep:

- Marks task PENDING_CLEANUP.
- Releases TaskNameLock rows where the holding task is dead.
- Leaves `j_<id>_*` tables alone (job-end cleanup picks them up).
- Sweeps orphan `t_*` tables (no live owner, no pin_refs).

## Job completion

When all tasks reach a terminal state, `BackgroundWorker.try_complete_job()`:

- DROP every `j_<job_id>_*` table in `table_registry` for this job — unconditionally. Preserved, non-preserved, and pinned Object outputs all share the `j_<job_id>_` prefix and are all gone at job end.
- Delete `table_pin_refs` and `task_name_locks` rows for this job.
- Mark job COMPLETED / FAILED.
- Leave the job row + oplog in SQL until TTL expiry (unchanged).

# Components

| Component                                              | Status                              |
|--------------------------------------------------------|-------------------------------------|
| `LocalLifecycleHandler` (`data_context/lifecycle.py`)  | Kept; lightly extended              |
| `task_scope()` (`orchestration/orch_context.py`)       | Rewritten over `LocalLifecycleHandler` + `TaskExtensions` |
| `OrchLifecycleHandler`                                 | **Deleted**                         |
| `TablePinRef` (SQL)                                    | Kept                                |
| `TableRunRef` (SQL)                                    | **Deleted**                         |
| `TableContextRef` (SQL)                                | **Deleted**                         |
| `TableRegistry` (SQL)                                  | Trimmed (drop `run_id`, add `preserved`) |
| `TaskNameLock` (SQL)                                   | **New**                             |
| `BackgroundWorker`                                     | Trimmed (see below)                 |
| `PreservationMode` enum                                | **Deleted**                         |

## BackgroundWorker changes

Deleted:

- `_cleanup_unreferenced_tables()` — `table_run_refs` is gone.

Modified / new:

- `_cleanup_failed_task_tables()` — for tasks in PENDING_CLEANUP, scan `table_registry` for `j_<job_id>_*` rows created by the failed task where `preserved=FALSE` and DROP them on CH.
- `_cleanup_orphan_scratch_tables()` — sweeps `t_*` CH tables with no live owning task.
- `_cleanup_at_job_completion()` — on terminal transition, DROP every `j_<job_id>_*` table in `table_registry` for the job.
- `_cleanup_pin_refs()` — unchanged.
- `_cleanup_dead_workers()` — extended to release `task_name_locks` for the dead worker's tasks.
- `_cleanup_expired_jobs()` — unchanged (TTL safety net).

# SQL schema changes

Single Alembic revision. Both upgrade and downgrade paths must run.

## Upgrade

1. Add `preserve JSONB NULL` to `jobs` and `registered_jobs`.
2. Backfill: `preservation_mode='NONE'` → `preserve=NULL`; `preservation_mode='FULL'` → `preserve='"*"'`.
3. Drop `preservation_mode` column from `jobs` and `registered_jobs`.
4. Drop tables: `table_run_refs`, `table_context_refs`.
5. On `table_registry`: drop `run_id`; add `preserved BOOLEAN NOT NULL DEFAULT FALSE`.
6. Create `task_name_locks`:

   ```sql
   CREATE TABLE task_name_locks (
       job_id      VARCHAR NOT NULL,
       name        VARCHAR NOT NULL,
       task_id     VARCHAR NOT NULL,
       acquired_at TIMESTAMP NOT NULL,
       PRIMARY KEY (job_id, name)
   );
   ```

## Downgrade

Reverse, with the constraint that only `NONE` and `FULL` round-trip cleanly. List values in `preserve` cannot map back to a real `PreservationMode` value (`STRATEGY` was never implemented), so downgrade fails loudly if any non-`NULL`, non-`"*"` `preserve` value exists.

# Concurrency

`task_name_locks` is the single point of coordination for non-preserved named tables. Acquisition is `INSERT ... ON CONFLICT DO NOTHING` followed by a check: if the row exists with a different `task_id`, look up that task's status — if RUNNING, raise `TableNameCollision`; if terminal, the dead-worker sweep should have released the row, so the lock is taken atomically by retrying the insert after sweep runs. The simpler conservative path: raise `TableNameCollision` whenever the row exists with a different task_id, and let the operator retry after the dead task is cleaned up.

Preserved-name tables are coordination-free: they're created idempotently with `CREATE TABLE IF NOT EXISTS`, schema reconciled by `table_registry`. Two concurrent tasks both calling `ctx.table("training_set")` get the same physical table.

# Testing

## Unit tests

| File                                  | Covers                                                                                  |
|---------------------------------------|-----------------------------------------------------------------------------------------|
| `test_preserve_resolution.py`         | Precedence: explicit > RegisteredJob > None. `[]` is explicit-empty, not fallthrough.   |
| `test_task_scope_lifecycle.py`        | Success path: drop `t_*` and non-preserved `j_<id>_<name>` inline; preserved survives.  |
| `test_task_scope_failure.py`          | Failure: drop only `t_*`; leave `j_<id>_*`; release name locks.                         |
| `test_name_collision.py`              | Concurrent tasks taking same non-preserved name raise `TableNameCollision`; lock releases on exit. |
| `test_pin_unchanged.py`               | Existing pin/unpin semantics still work for Object outputs across tasks.                |
| `test_background_worker_cleanup.py`   | Job completion drops all `j_<id>_*`; failed-task cleanup only touches non-preserved; dead-worker sweep handles orphan `t_*` and releases locks. |

## Integration tests

- Two-task chain: producer A returns Object consumed by B (pin path) AND A creates preserved `training_set` read by B (preserve path).
- Failure mid-task: preserved table survives to job-end; non-preserved named table survives to job-end (per failure rule); `t_*` cleaned by failed-task sweep.
- Worker death mid-task: preserved + named tables intact; replay creates fresh names; lock released by dead-worker sweep.

## Tests to delete

- `PreservationMode.NONE` / `FULL` direct exercise — migrate to `preserve` arg.
- `table_run_refs` incref/decref tests — machinery gone.
- `OrchLifecycleHandler` direct exercise — replaced by `LocalLifecycleHandler` coverage.

# Documentation updates

- `docs/orchestration.md` — new lifecycle diagram (this spec's flow), `preserve` API, removed `PreservationMode` references.
- `docs/future.md` — remove the "Switch DB Enums from `StrEnum` to `Literal` + `sa_column`" entry insofar as `PreservationMode` is concerned (the enum is being deleted, not migrated). Other enums (`JobStatus`, `TaskStatus`, etc.) stay on the list.
- `aaiclick/orchestration/examples/` — replace any `preservation_mode=` usage with `preserve=`.
- `CLAUDE.md` — update the `resolve_job_config` example to `resolve_preserve` (or remove if it's no longer the clearest example of the testing rule).

# Open risks

- **TaskNameLock semantics around retry**: the conservative rule (raise on any existing row with a different task_id) means a failed task's lock blocks retries until the dead-worker sweep runs (10s poll). For most workloads this is acceptable; if it becomes a problem, BackgroundWorker can be triggered immediately on PENDING_CLEANUP transition.
- **CH-side orphan tables on worker death**: `_cleanup_orphan_scratch_tables()` needs a "no live owner" check that's robust to clock skew. Use `task_id NOT IN (SELECT task_id FROM tasks WHERE status IN ('CLAIMED','RUNNING'))` plus a minimum age threshold (e.g. 5 minutes) before dropping any `t_*` CH table.
- **Idempotent preserved-table creation**: two concurrent tasks calling `ctx.table("training_set")` race on `CREATE TABLE IF NOT EXISTS` — the schema must match. If schemas diverge, the second caller errors. Document this; recommend declaring the table once in a setup task.
