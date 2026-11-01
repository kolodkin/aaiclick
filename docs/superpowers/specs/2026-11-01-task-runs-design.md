Task Runs — Per-Attempt Status and Timing
---

# Intent

A retried task keeps `run_statuses` like `[FAILED, FAILED, COMPLETED]` but only
one `started_at` / `completed_at` pair, overwritten on every retry. The earlier
attempts' timings are gone, and the logs endpoint's attempt selector shows a
status per attempt but no times.

Fix: each attempt becomes its own row in a new `task_runs` table, one `tasks`
row to many `task_runs` rows. The `run_ids` / `run_statuses` JSON arrays on
`tasks` go away.

# Goals

- Every attempt keeps its own `status`, `started_at` and `completed_at`.
- `GET /tasks/{id}/logs` returns those times per attempt; the UI's attempt
  picker shows them.
- Ending a run is one status-guarded `UPDATE`, with no read-modify-write of a
  JSON array first.

# Non-Goals

- Backward compatibility. No data is carried over from `run_ids` /
  `run_statuses`; existing rows simply have no attempt history after upgrade.
- Moving `error`, `result`, `execution_worker_id` or any other per-task field
  to the run row.
- An ORM relationship (`Task.runs`). Orchestration code uses explicit selects.
- Changing retry semantics, `attempt`, `run_epoch` fencing, or the Pod-side
  `remote_task_results` handoff.

# Anti-Goals

- A second source of truth for "the current run". The current run is the
  task's latest row (`max(run_id)`), exactly as it is the last array entry
  today; there is no index column.
- Reintroducing the reaper double-write race. A container or Pod inserts its
  own `RUNNING` row via `register_run`, as it appends to `run_statuses` today,
  but never writes a terminal run status. Terminal writes stay with the host
  worker and the reaper.

# Constraints

- Migrations come only from the `generate-migration` skill (Alembic
  autogenerate via GitHub Actions). That is why no data migration exists.
- Both SQLite and Postgres backends; the guarded `UPDATE` pattern is the
  cross-backend atomicity tool already used for task status writes.
- `run_id` stays a snowflake: ClickHouse `task_logs` rows are keyed by it.
- Follow the `python-testing-style` skill for tests and `markdown-style` for
  docs.

# Approaches Considered

| Approach                                   | Trade-off                                                                                                          |
|--------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| A. Two more JSON arrays on `tasks`         | Smallest diff, no data loss. Datetimes stored as strings or epoch ms; four parallel arrays to keep in step.         |
| B. One JSON array of `{run_id, status, …}` | Cleaner read, same string-datetime limit, same churn and history loss as C, still read-modify-write on every end.  |
| C. `task_runs` table (chosen)              | Real typed columns, deletes the array helpers and their pre-read SELECTs. ~20 files change, history not carried.   |

C was chosen because it removes code from every writer and makes attempts a
real entity, which the Kubernetes runner design already treats them as.

# Data Model

```python
class TaskRun(SQLModel, table=True):
    """One attempt of a task. The in-flight attempt is the task's latest row
    while its status is ``RUNNING``; attempt N is the Nth row ordered by
    ``run_id``."""

    __tablename__: ClassVar[str] = "task_runs"

    run_id: int = Field(sa_column=Column(BigInteger, primary_key=True))  # snowflake
    task_id: int = Field(
        sa_column=Column(BigInteger, ForeignKey("tasks.id", ondelete="CASCADE"), index=True, nullable=False)
    )
    status: TaskStatus = Field(sa_column=Column(String, nullable=False))
    started_at: datetime = utc_field(default_factory=utc_now)
    completed_at: datetime | None = utc_field(default=None)
```

`Task` loses `run_ids` and `run_statuses`. It keeps `started_at` /
`completed_at` as the latest-attempt mirror, so timing stats
(`queue_time_ms`, `exec_time_ms`) and group roll-ups in `view_models.py` are
untouched. `attempt`, `max_retries`, `retry_after` and `run_epoch` are
unchanged.

Statuses written to `task_runs.status`: `RUNNING`, `COMPLETED`, `FAILED`,
`CANCELLED`. Cleanup statuses never land on a run row.

# Writes

| Today                                                                 | After                                                                                              |
|-----------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| `register_run` appends to `run_ids` and `run_statuses`                | `register_run` inserts a `RUNNING` row with `started_at = now` and returns its `run_id`            |
| `end_current_run(run_statuses, status)` rewrites the last array entry | `end_run(session, task_id, status)` → `UPDATE task_runs SET status, completed_at = now WHERE run_id = (latest for task) AND status = RUNNING` |
| `fail_run_values(run_statuses, error)` + a prior SELECT               | Task `UPDATE` as today minus the array; run `UPDATE` above in the same transaction; SELECT dropped |
| `_apply_task_status` mirrors status into the array                    | Same transaction issues the run `UPDATE` for `COMPLETED` / `FAILED`                                |
| `release_cancelled_run` stamps `CANCELLED` on the last entry          | Run `UPDATE` with `CANCELLED`                                                                      |
| Dead-worker sweep selects `run_statuses` per task, then updates        | One task `UPDATE` plus one run `UPDATE` per expired task; the SELECT is gone                        |

The run `UPDATE` targets the latest row only while it is still `RUNNING`, so
a finished run keeps how it ended and a second ender affects zero rows. It is
issued only after the task-level write landed, in the same transaction: in the
ORM paths the task row is already locked and checked, and in the statement
paths (`_set_pending_failure_cleanup`, the dead-worker sweep) the run `UPDATE`
runs only when the task `UPDATE`'s rowcount is nonzero. A fenced late writer
whose epoch guard failed therefore never touches the new attempt's row. The
task-level status guard remains the verdict, as today.

`clear_task` drops `table_run_refs` for the latest run. It reads
`max(run_id)` per task from `task_runs` instead of `run_ids[-1]`. It closes
any still-`RUNNING` run of the cleared tasks as `CANCELLED`, so a cleared
attempt neither reads as running forever nor absorbs the next attempt's
outcome. Earlier attempts stay visible. `get_cleanup_tasks` in `handler.py` and
`clean_task_runs` in `background_worker.py` read `run_ids[-1]` the same way
and get the same `max(run_id)` subquery.

`_delete_job_data` in `background_worker.py` deletes child rows with explicit
SQL rather than FK cascade; `task_runs` gets its own `DELETE` there. The
`ondelete="CASCADE"` on the FK is belt and braces for Postgres.

`register_run` is called from inside module containers and Pods as well as
from the host. Today it silently tolerates a missing task; an FK insert will
not, so the missing-task case must raise or be checked explicitly.

# Reads

- `get_task_logs`: `SELECT run_id, status, started_at, completed_at FROM task_runs WHERE task_id = ? ORDER BY run_id`.
  Attempt N is row N. Raises `NotFound` outside `1..len(rows)` as today.
- `TaskAttemptView` gains `started_at: datetime` and `completed_at: datetime | None`.
- `task_to_detail`'s `runs` count becomes a `COUNT(*)` on `task_runs`. The
  detail endpoint serves one task, so no N+1. The builder confirms the list
  view does not use it.
- `get_run_count` queries the table. The output-rescue check in
  `execution_worker.py` detects "the container never registered its run" by
  the count not having moved. The worker captures the pre-launch count
  explicitly instead of reading `len(task.run_ids)` off the claimed ORM
  object. `log_test_helpers.container_registers_run` models the same thing.
- `task_to_detail` is sync, so the `COUNT(*)` is fetched in `get_task` and
  passed in.

# API and UI

- `TaskLogsView.attempts[*]` carries `started_at` and `completed_at`.
  Regenerate `src/api/schema.ts`.
- `AttemptPicker` in `LogViewer.tsx` adds the times to the button's `title`
  tooltip as start time and duration. No new layout.

# Migration

One autogenerated migration: create `task_runs`, drop `tasks.run_ids` and
`tasks.run_statuses`. No data copy.

# Docs

- `docs/designs/task_log_retirement.md`, `kubernetes_runner.md`, `java-sdk.md`
  mention `run_ids` / `run_statuses`; update the references to `task_runs`.
- Delete this spec and its plan once the change lands.

# Left to the Builder

- Exact module for `TaskRun` and the run-update helper (`models.py` beside
  `Task` is the obvious home; the helper replaces `end_current_run`).
- Whether `set_task_runs` in `testing.py` inserts rows directly or calls
  `register_run`; it keeps the same signature shape for log tests.
- The tooltip's duration formatting; reuse whatever `view_models._ms_between`
  or the UI already uses for task durations.
