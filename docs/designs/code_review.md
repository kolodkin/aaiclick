Code Review — 2026-10-18
---

Open findings from a whole-repo audit (security findings are fixed), assuming a distributed
deployment with several workers. All come from reading the code, none reproduced at runtime.
Remove an entry once its fix lands.

# Summary

| #  | Finding                                              | Severity | Area          |
|----|------------------------------------------------------|----------|---------------|
| 6  | `clear_task` loses its input tables                  | High     | Orchestration |
| 10 | Failed `create_job` leaves a job with no tasks       | Medium   | Orchestration |
| 11 | IN-lists over 32,767 parameters                      | Medium   | Orchestration |
| 15 | Dead-worker sweep races with heartbeat               | Medium   | Orchestration |
| 16 | `concat` scrambles row order within a source         | Medium   | Data          |
| 17 | `join` does not quote identifiers                    | Low      | Data          |
| 18 | Dashboard panels run without a limit                 | Low      | Viewer        |
| 19 | Dead attempt stays RUNNING in `run_statuses`         | Low      | Orchestration |
| 20 | Dead code and redundant writes                       | Low      | Lean          |

# Orchestration

## 6. `clear_task` loses its input tables

- **Where:**
  - `aaiclick/orchestration/execution/claiming.py`, function `clear_task`
  - `aaiclick/orchestration/execution/runner.py`
- **Issue:** A task releases its pins once it succeeds. When `clear_task` resets it, no pin or run ref holds its upstream tables, so the cleanup pass may already have dropped them and the re-run fails with `LookupError`.
- **Fix:** In `clear_task`, re-pin the upstream result tables for each cleared task, and refuse the clear when one is already gone.

## 10. Failed `create_job` leaves a job with no tasks

- **Where:** `aaiclick/orchestration/factories.py`
- **Issue:** The job is committed before `commit_tasks`. If validation or the DB write fails, the job stays PENDING with no tasks:
  - `roll_up_job` skips it.
  - TTL cleanup ignores it.
- **Fix:** On an exception from `commit_tasks`, mark the job FAILED with the error.

## 11. IN-lists over 32,767 parameters

- **Where:**
  - `aaiclick/orchestration/background/background_worker.py`, functions `_delete_job_data` and `_delete_table_refs`
  - `aaiclick/orchestration/execution/claiming.py`, function `clear_task`
- **Issue:** `in_clause` emits one bind parameter per id, and asyncpg allows at most 32,767. A `map()` that creates 40k part tasks then cannot be deleted or cleared.
- **Fix:**
  - Use subqueries (`IN (SELECT id FROM tasks WHERE job_id = :job_id)`).
  - Add a `LIMIT` to the unreferenced-tables SELECT.

## 15. Dead-worker sweep races with heartbeat

- **Where:**
  - `aaiclick/orchestration/background/background_worker.py`
  - `aaiclick/orchestration/background/pg_handler.py`
  - `aaiclick/orchestration/background/sqlite_handler.py`
- **Issue:** The stale workers are found with a SELECT, then the UPDATE is keyed on id alone. A worker that heartbeats between the two is still marked dead, and its running task is retried.
- **Fix:** Use one statement: `UPDATE ... WHERE id IN (...) AND last_heartbeat < :cutoff RETURNING id`.

## 19. Dead attempt stays RUNNING in `run_statuses`

- **Where:**
  - `aaiclick/orchestration/background/pg_handler.py`
  - `aaiclick/orchestration/background/sqlite_handler.py`
- **Issue:** `mark_dead_execution_workers` changes the task status but not `run_statuses`. The UI then shows the dead attempt as running forever.
- **Fix:** In `register_run`, close a trailing RUNNING entry as FAILED before appending a new one.

# Data and Viewer

## 16. `concat` scrambles row order within a source

- **Where:** `aaiclick/data/object/ingest.py`
- **Issue:** The INSERT orders only by `_src_ord`, so rows within one source come back in scan order. Single-part chdb hides this, but multi-part ClickHouse permutes the rows.
- **Fix:** Also select each source's `aai_id`, and order by `_src_ord, aai_id`.

## 17. `join` does not quote identifiers

- **Where:** `aaiclick/data/object/join.py`
- **Issue:** Column and key names go into the SQL unquoted. A column named `from` or `a b` breaks the join.
- **Fix:** Wrap `src`, `out`, `lk`, `rk` and the insert columns in `quote_identifier`.

## 18. Dashboard panels run without a limit

- **Where:**
  - `aaiclick/internal_api/viewer.py`, function `run_dashboard`
  - `aaiclick/viewer/view_models.py`
- **Issue:** `DashboardIn.queries` has no size cap, and every panel runs at once through `asyncio.gather`. A 10k-panel dashboard fires 10k ClickHouse queries.
- **Fix:** Cap it with `Field(max_length=50)`.

# Lean

## 20. Dead code and redundant writes

- **Never written outside tests:** `TASK_CLAIMED` and `EXECUTION_WORKER_IDLE`.
- **Used only by tests:**
  - `aaiclick/auth/store.py`: `get_active_api_token`
  - `aaiclick/internal_api/setup.py`: `stale_local_db`
- **Registered twice:** `aaiclick/server/app.py` registers `execution_workers.router` a second time, outside the loop.
- **Redundant write:** `aaiclick/orchestration/execution/execution_worker.py` re-sets RUNNING right after the claim. Set `started_at` in the claim SQL instead.

# Smaller Findings

- **Username race:** a concurrent duplicate username returns 500 instead of 409 (`aaiclick/auth/store.py`).
- **Reset links outlive a password change:** they stay usable after the password is set (`aaiclick/internal_api/password_reset.py`).
- **Duplicate insert helpers:** `create_user` and `create_refresh_token` repeat `_insert` (`aaiclick/auth/store.py`).
- **Header parsed twice:** the Authorization header is parsed in both `PrincipalAuthMiddleware` and `resolve_principal` (`aaiclick/server/auth.py`).

# Not Covered

- **Not read:**
  - the Java SDK and the web UI (`src/`)
  - migrations and examples
  - most of `aaiclick/data/object/object.py` and `operators.py`
  - the Docker and Kubernetes image builds
- **Not run:** the project's test suite.
