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

- A second source of truth for "the current run". The current run is the one
  row with `status = RUNNING`; there is no index column.
- Reintroducing the reaper double-write race. The Pod never writes
  `task_runs`, exactly as it never wrote `run_statuses`.

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
    """One attempt of a task. The in-flight attempt is the row with
    ``status = RUNNING``; attempt N is the Nth row ordered by ``run_id``."""

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
| `end_current_run(run_statuses, status)` rewrites the last array entry | `end_run_values(status)` → `UPDATE task_runs SET status, completed_at = now WHERE task_id = ? AND status = RUNNING` |
| `fail_run_values(run_statuses, error)` + a prior SELECT               | Task `UPDATE` as today minus the array; run `UPDATE` above in the same transaction; SELECT dropped |
| `_apply_task_status` mirrors status into the array                    | Same transaction issues the run `UPDATE` for `COMPLETED` / `FAILED`                                |
| `release_cancelled_run` stamps `CANCELLED` on the last entry          | Run `UPDATE` with `CANCELLED`                                                                      |
| Dead-worker sweep selects `run_statuses` per task, then updates        | One task `UPDATE` plus one run `UPDATE` per expired task; the SELECT is gone                        |

The run `UPDATE` is idempotent and race-free by construction: a second ender
finds no `RUNNING` row and affects zero rows. Callers do not check its
rowcount; the task-level status guard remains the verdict, as today.

`clear_task` drops `table_run_refs` for the latest run. It reads
`max(run_id)` per task from `task_runs` instead of `run_ids[-1]`. It does not
delete run rows; a cleared task's earlier attempts remain visible, as today.

`delete_job` relies on the `ondelete="CASCADE"` FK. SQLite needs
`PRAGMA foreign_keys` on, so the builder checks how the existing cascades
(`remote_task_results`, dependencies) are enforced and follows that.

# Reads

- `get_task_logs`: `SELECT run_id, status, started_at, completed_at FROM task_runs WHERE task_id = ? ORDER BY run_id`.
  Attempt N is row N. Raises `NotFound` outside `1..len(rows)` as today.
- `TaskAttemptView` gains `started_at: datetime` and `completed_at: datetime | None`.
- `task_to_detail`'s `runs` count becomes a `COUNT(*)` on `task_runs`. The
  detail endpoint serves one task, so no N+1. The builder confirms the list
  view does not use it.
- `get_run_count` queries the table. The output-rescue check in
  `execution_worker.py` compares a count taken before launch against the count
  after, so the worker captures the pre-launch count explicitly instead of
  reading `len(task.run_ids)` off the claimed ORM object.

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
