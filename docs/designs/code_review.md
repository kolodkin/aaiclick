Code Review — 2026-10-18
---

Open findings from a whole-repo audit (the first round's security findings are fixed),
assuming a distributed deployment with several workers. All come from reading the code, none reproduced at runtime.
Remove an entry once its fix lands.

# Summary

| #  | Finding                                              | Severity | Area          |
|----|------------------------------------------------------|----------|---------------|
| 17 | `join` does not quote identifiers                    | Low      | Data          |
| 18 | Dashboard panels run without a limit                 | Low      | Viewer        |
| 19 | Dead attempt stays RUNNING in `run_statuses`         | Low      | Orchestration |
| 20 | Dead code and redundant writes                       | Low      | Lean          |
| 21 | Returned tasks escalate entry_type, image and env    | High     | Orchestration |
| 22 | Ref key rename (#484) has no migration               | Medium   | Data          |
| 23 | k8s wait loops count only sleep time toward timeout  | Medium   | Orchestration |
| 24 | Login lockout replies before bcrypt                  | Low      | Auth          |

# Orchestration

## 19. Dead attempt stays RUNNING in `run_statuses`

- **Where:** `aaiclick/orchestration/background/handler.py`
- **Issue:** `release_expired_claims` changes the task status but not `run_statuses`. The UI then shows the dead attempt as running forever.
- **Fix:** In `register_run`, close a trailing RUNNING entry as FAILED before appending a new one.

## 21. Returned tasks escalate entry_type, image and env

- **Where:** `aaiclick/orchestration/execution/runner.py` (`finish_shell_result` → `_build_dag_from_dicts`), `aaiclick/orchestration/image_injection.py`
- **Issue:** A shell/jvm container's `out.json` task dicts pass `entry_type`, `image`, `command_env` and `git_*` verbatim to `create_task`. A compromised user image can return a *module* child that inherits the runner env (SQL credentials) in Docker, or runs as a host subprocess (any importable callable) when images are NULL. `validate_plain_data_plane_tasks` skips module children; nothing requires a child's entry_type to be no stronger than its parent's.
- **Also:** `_handle_ref` treats only string `task_id`s as handles, so a returned `{"_aaiclick_type": "upstream", "task_id": <int>}` reaches `_resolve_upstream_ref`, which has no `job_id` filter — a cross-job result read (needs a guessed snowflake id).
- **Fix:** at registration, reject returned tasks whose `entry_type` differs from the parent's (or whitelist), and scope `_resolve_upstream_ref` to the parent's job.

## 22. Ref key rename (#484) has no migration

- **Where:** `aaiclick/data/object/refs.py`
- **Issue:** #484 renamed the ref discriminators (`ref_type` / `object_type` / `native_value` → `_aaiclick_type`) with no compat shim or data migration. Rows written before the upgrade (in-flight `tasks.kwargs`, `remote_task_results`, persistent object refs) silently deserialize as plain dicts; old persistent refs stop being recognized by `is_persistent_object_ref`, so their tables become sweepable. The stale-DB gate checks schema drift only, not row contents.
- **Fix:** either accept the old keys in `ref_kind` for one release, or extend `stale_local_db_reason` to refuse a DB holding pre-#484 rows.

## 23. k8s wait loops count only sleep time toward timeout

- **Where:** `aaiclick/orchestration/execution/kubernetes_worker.py` (shell-pod loop and `_KubernetesVehicle.wait`)
- **Issue:** `elapsed += POLL_INTERVAL` ignores the kubectl calls each iteration makes, so the enforced timeout drifts with API-server latency. For shell pods, `activeDeadlineSeconds = timeout + 60` then kills the pod first, surfacing an opaque pod failure (or no error) instead of "Task timed out after Ns"; module pods just time out late. Docker (`asyncio.wait_for`) is unaffected.
- **Fix:** compute `elapsed` from a monotonic clock.

# Data and Viewer

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

## 24. Login lockout replies before bcrypt

- **Where:** `aaiclick/internal_api/auth.py`
- **Issue:** The locked branch raises "too many failed attempts" before any bcrypt runs, while unknown users get the dummy-hash + uniform "invalid credentials". The distinct message (and faster reply) confirms an account exists — the oracle the dummy hash exists to prevent. The probe also locks the real user out.
- **Fix:** run the dummy verify first and return the uniform message for locked accounts too (keep recording the attempt).

# Smaller Findings

- **`stage_in_json` leaks its temp dir on failure:** the `aaiclick-io-*` dir is created before kwarg resolution; a resolve error propagates before any caller learns the path, leaking one empty dir per failed attempt (`aaiclick/orchestration/execution/runner.py`).
- **`LazyChClient.close()` before first use can reconnect:** with `_client` still `None` nothing is marked closed, so a holder calling `query()` later opens a connection nothing closes; add a `_closed` flag (`aaiclick/data/object_context/ch_client.py`).
- **mp parent's "never query ClickHouse" is unenforced:** since #489 every `orch_context` installs a `LazyChClient`, so an accidental parent-side query silently takes the chdb lock instead of raising (`aaiclick/orchestration/orch_context.py`, `aaiclick/orchestration/execution/mp_worker.py`).
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
