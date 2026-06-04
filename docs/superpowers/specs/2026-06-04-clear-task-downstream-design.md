Clear Task + Downstream — Design
---

Reset a specific task and all its transitive downstream tasks to `PENDING`,
leaving upstream tasks and their output tables untouched — the same concept as
Airflow's "clear task". Lets an operator re-run part of a pipeline without
re-executing the whole job.

Tracked in `docs/future.md` (Medium Priority).

---

# Goals

- Reset a target task **and** every transitively-downstream task to `PENDING`.
- Leave upstream tasks and their output tables exactly as-is.
- Stop any affected task that is currently running on a worker, in a way that
  **cannot** be clobbered by the stale run's late writes.
- Reactivate a terminal job so the cleared tasks actually re-run.
- Expose the operation on the same surfaces as `cancel_job`: orchestration
  function, internal API, REST route, and MCP tool.

# Non-goals

- Clearing *upstream* tasks (only the target and its downstream).
- Actively dropping the cleared tasks' own output tables — left to the
  existing ref-count cleaner (see Known limitations).
- Bumping the `attempt` / retry counter on a clear.

---

# Fencing token: `run_epoch`

The crux of the feature is stopping an in-flight task without a race. A worker
that is mid-execution is about to write `COMPLETED` (`update_task_status`) or
`PENDING_CLEANUP` (`_set_pending_cleanup`), and `_set_pending_cleanup` writes
**unconditionally**. If `clear_task` resets the task to `PENDING`, a worker
write that lands a moment later would clobber the reset back to a terminal
state. A status check before the write does not close this — there is always a
gap between the check and the write.

The fix is a **fencing token**: a per-task version number that makes the check
*be* the write.

- New column on `tasks`: `run_epoch: int`, `default 0`, `NOT NULL`,
  `server_default="0"`.
- A worker reads `run_epoch` when it claims the task (it is already on the
  `Task` row returned by `claim_next_task`) and holds that value for the run.
- Every worker status write is a compare-and-set:
  `UPDATE ... WHERE id = :id AND run_epoch = :captured_epoch`.
- `clear_task` increments `run_epoch`.

The instant a clear bumps the epoch, the in-flight worker holds a stale value;
its conditional UPDATE matches zero rows and is silently dropped. Correctness
no longer depends on timing.

## Timeline

Task T is `RUNNING` on worker W with `run_epoch = 0` (W captured `0` at claim):

| t  | event |
|----|-------|
| t0 | W executing T, holding epoch `0` |
| t1 | `clear_task(T)`: `status='PENDING'`, `run_epoch = 1`, `worker_id=NULL`, … |
| t2 | cancellation monitor sees `run_epoch(1) != captured(0)` → cancels W's asyncio run |
| t3 | `CancelledError` unwinds through `task_scope`'s `finally` → T's CH table refs decreffed automatically |
| t4 | W's terminal write `... WHERE id=T AND run_epoch=0` matches 0 rows → rejected; W logs "task cleared, discarding result" |
| t5 | T sits at `PENDING`, `run_epoch=1`; a worker re-claims with epoch `1` and runs fresh |

Two pieces, two roles:

- **The monitor** provides *promptness* — it stops wasted compute quickly and
  triggers ref cleanup via `task_scope` unwinding. Best-effort (CPU-bound code
  without `await` points cancels only when it next yields).
- **The epoch** provides *correctness* — no matter when the stale write lands,
  it can never overwrite the reset. Even if the monitor never fired, state
  stays consistent.

Verified: `task_scope`'s `finally` (`aaiclick/orchestration/orch_context.py:463`)
decrefs all live objects on **any** exit, including `CancelledError`, so an
aborted run releases its `table_run_refs` automatically — no `PENDING_CLEANUP`
detour is needed for ref hygiene.

---

# `clear_task(task_id)` — orchestration layer

Lives alongside `cancel_job` in `aaiclick/orchestration/execution/claiming.py`.

1. Resolve the task; raise `TaskNotFound` if missing.
2. Compute the affected set = `{target} ∪ transitive-downstream(target)` via a
   Python BFS over `dependencies`, expanding the four edge shapes already
   encoded in `DEPENDENCY_WHERE` / `_CASCADE_UPSTREAM_FAILED_SQL`:
   - task→task: `next_id` where `previous_id=t, previous_type='task', next_type='task'`
   - task→group: tasks in groups where `previous_id=t, previous_type='task', next_type='group'`
   - group→task (when `t.group_id` set): `next_id` where `previous_id=t.group_id, previous_type='group', next_type='task'`
   - group→group (when `t.group_id` set): tasks in groups where `previous_id=t.group_id, previous_type='group', next_type='group'`

   Expanding group edges means clearing one task in a group also re-runs the
   group's consumers (the group is no longer fully complete) — but **not** the
   group's sibling tasks, which are parallel, not downstream.
3. One bulk `UPDATE … WHERE id IN (:ids)` (using the existing `in_clause`
   helper from `background/handler.py`):
   - `status='PENDING'`, `run_epoch = run_epoch + 1`
   - null `worker_id, claimed_at, started_at, completed_at, error, result,
     log_path, retry_after`
   - leave `run_ids` / `run_statuses` intact — append-only history; the next
     run appends a fresh entry via `execute_task`
   - leave `attempt` unchanged (no retry bump on clear)
4. Reactivate the job: if `job.status` is terminal
   (`COMPLETED` / `FAILED` / `CANCELLED`) set it to `RUNNING` and clear
   `completed_at` / `error`. `started_at` is already set, so `RUNNING` is the
   correct active state. `PENDING` / `RUNNING` jobs are left untouched.
5. Commit; return `(cleared_task_ids, job)`.

## Why job reactivation is required

`claim_next_task` excludes tasks whose job is `CANCELLED` or `FAILED`
(`execution/sqlite_handler.py:36`), and a `COMPLETED` job keeps `started_at`
set so the claim path never flips it back to `RUNNING`. Without reactivation
the cleared `PENDING` tasks would never be claimed.

## Composition with the existing cascade

The existing `cascade_upstream_failed` / `try_complete_job` machinery composes
correctly without special-casing:

- Clearing a failed task plus its downstream (e.g. `A` failed → clear `A`,
  resets `A,B,C`) re-runs cleanly.
- Clearing *only* a task whose upstream is still `FAILED` (e.g. clear `C` while
  `B` is `FAILED`) leaves `C` un-claimable (dependency gate) and
  `try_complete_job` re-marks it `UPSTREAM_FAILED` → job `FAILED`. This is the
  correct Airflow-like behavior: clear from the failure point.

---

# Worker fencing

Changes in `aaiclick/orchestration/execution/worker.py` and `claiming.py`:

- `update_task_status(..., expected_epoch: int | None = None)` and
  `_set_pending_cleanup(..., expected_epoch: int | None = None)` add
  `run_epoch = :epoch` to their guards and return `False` / no-op on mismatch.
  Worker call-sites pass `task.run_epoch`; direct callers (tests, `cancel_job`)
  omit it and behave exactly as before.
- The cancellation monitor aborts the asyncio run when status is `CANCELLED`
  **or** `run_epoch != expected_epoch`. The worker passes the captured epoch.

## Clearing vs. crash cleanup

A separate background actor — the dead-worker sweep — also touches in-flight
tasks, and a clear can race it. The sweep runs in two steps:

1. **`mark_dead_workers`** flips a crashed worker's `RUNNING` / `CLAIMED` tasks
   to `PENDING_CLEANUP`.
2. **`transition_pending_cleanup`** later reads those `PENDING_CLEANUP` tasks,
   releases their refs, and moves each to `PENDING` (retries left) or `FAILED`.

Step 1 can never touch a cleared task: it searches by
`worker_id = <dead> AND status IN ('RUNNING','CLAIMED')`, and `clear_task`
atomically nulls `worker_id` and sets `status = 'PENDING'` — so a cleared task
no longer matches the search. No epoch needed there.

Step 2 is the real race — the sweep might decide "this task → FAILED" from a
read taken *before* a clear, then write it *after*. The guard:

> **The UPDATE only fires while the task is *still* `PENDING_CLEANUP`.**
> `transition_pending_cleanup` adds `AND status = 'PENDING_CLEANUP'` to its
> WHERE clause, so if a clear reset the task to `PENDING` in the gap between the
> sweep's read and this write, the UPDATE matches zero rows and does nothing —
> the clear is not overwritten.

---

# API surface

Mirrors `cancel_job` across all four layers.

- **View model** (`aaiclick/orchestration/view_models.py` or
  `aaiclick/view_models.py`, matching where `JobView` lives):
  `ClearTaskView { job: JobView, cleared_task_ids: list[int] }` — reports the
  fan-out honestly.
- **Internal API** (`aaiclick/internal_api/tasks.py`): `clear_task(task_id)`
  wraps the orchestration call, mapping `TaskNotFound → NotFound` (404).
- **REST** (`aaiclick/server/routers/tasks.py`):
  `POST /tasks/{task_id}/clear` → `ClearTaskView`,
  `responses=problem_responses(404)`.
- **MCP**: register a `clear_task` tool mirroring the existing `cancel_job`
  registration.

---

# Testing

Following `python-testing-style` and the `test_cancel_job.py` patterns.

Orchestration (`execution/test_clear_task.py`):

- direct target reset to `PENDING`
- transitive chain `A→B→C`: clearing `A` resets all three
- group-edge fan-out: clearing a grouped task resets the group's consumers
- upstream tasks and a `COMPLETED` sibling are left untouched
- terminal job (`COMPLETED` / `FAILED`) revived to `RUNNING`
- fencing: a stale-epoch `update_task_status` and `_set_pending_cleanup` are
  rejected (return `False` / no row changed)
- in-flight: clearing a `RUNNING` task lands it `PENDING` and the monitor's
  abort check fires on epoch mismatch

Internal API (`internal_api/test_tasks.py`):

- 404 on missing task
- `ClearTaskView` shape round-trips with the right `cleared_task_ids`

Router (`server/routers/test_tasks.py`):

- status code + `Problem` envelope shape only (business logic lives in the
  internal-API tests, per the server package guidelines)

---

# Known limitations

- **Producer-completes-during-clear window**: if a producer task completes in
  the exact instant of a clear, its output table may linger with a stale pin
  until ref-count cleanup reclaims it. Consistent with the "leave tables to
  refcount" decision; not solved here.

---

# Migration

The `run_epoch` column is added via the `generate-migration` skill
(GitHub Actions autogenerate) — never hand-written, per the project's Alembic
rule.
