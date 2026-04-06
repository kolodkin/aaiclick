Registered Jobs — Implementation Plan
---

Separates job *registration* (catalog of known jobs) from job *execution* (individual runs).
Adds cron-based scheduling driven by the background worker.

# Phase 1: Model & Migration

**Objective**: Add `RegisteredJob` model, `RunType` enum, and new columns on `Job`.

**Tasks**:

- Add `RunType` StrEnum (`SCHEDULED`, `MANUAL`) to `models.py`
- Add `RegisteredJob` SQLModel with fields:
  - `id`: BigInteger (snowflake), PK
  - `name`: str, unique, indexed
  - `entrypoint`: str (Python dotted path)
  - `enabled`: bool, default `True`
  - `schedule`: Optional[str] (cron expression)
  - `default_kwargs`: Optional[Dict], JSON column
  - `next_run_at`: Optional[datetime], indexed
  - `created_at`: datetime
  - `updated_at`: datetime
- Add columns to `Job`:
  - `registered_job_id`: Optional BigInteger, FK → `registered_jobs.id`
  - `run_type`: RunType, default `MANUAL`
- Generate Alembic migration

**Deliverables**: Models compile, migration applies and rolls back cleanly.

# Phase 2: Registration Logic

**Objective**: CRUD operations for `RegisteredJob`.

**Tasks**:

- Create `aaiclick/orchestration/registered_jobs.py` with:
  - `register_job(name, entrypoint, schedule=None, default_kwargs=None, enabled=True)` — insert, compute `next_run_at` from cron if schedule provided
  - `get_registered_job(name)` — lookup by name
  - `upsert_registered_job(name, entrypoint, ...)` — insert or update (for `run-job` auto-register)
  - `enable_job(name)` / `disable_job(name)` — toggle `enabled`, recompute `next_run_at`
  - `list_registered_jobs(enabled_only=False)` — list all or enabled only
  - `compute_next_run(cron_expr, after=None)` — helper using `croniter` to compute next fire time
- Add `croniter` to dependencies in `pyproject.toml`

**Deliverables**: Unit tests for register, upsert, enable/disable, next-run computation.

# Phase 3: Update JobFactory and run-job Flow

**Objective**: Wire `RegisteredJob` into job creation.

**Tasks**:

- Update `JobFactory._create_job()` to:
  - Accept optional `registered_job_id` and `run_type` parameters
  - Set `run_type=MANUAL` by default
  - Set `registered_job_id` FK when provided
- Create `run_job(name_or_entrypoint, kwargs=None)` function that:
  - Calls `upsert_registered_job()` (auto-register if not present, no schedule)
  - Merges `kwargs` over `default_kwargs`
  - Creates `Job` with `run_type=MANUAL` and `registered_job_id` set
  - Creates the entry point `Task`
- Update `Job` queries in `jobs.py` to include `run_type` and `registered_job_id` in display

**Deliverables**: Existing tests still pass. New test for `run_job()` with auto-registration.

# Phase 4: Scheduler in Background Worker

**Objective**: `BackgroundWorker` (or renamed `BackgroundWorker`) checks cron schedules.

**Tasks**:

- Add `_check_schedules()` method to `BackgroundWorker`:
  - Query: `SELECT * FROM registered_jobs WHERE enabled = True AND next_run_at <= NOW()`
  - For each match, optimistic lock: `UPDATE registered_jobs SET next_run_at = :next WHERE id = :id AND next_run_at = :old`
  - If update affected 1 row → create `Job` with `run_type=SCHEDULED` + entry point `Task`
  - If 0 rows → another process handled it, skip
- Call `_check_schedules()` from `_do_cleanup()` (runs every poll interval, default 10s)
- The scheduler needs to resolve the entrypoint to create the Task — reuse `_callable_to_string` / entrypoint format already stored

**Deliverables**: Test that a registered job with cron schedule produces a Job row at the right time.
Test that two concurrent schedule checks don't create duplicate runs.

# Phase 5: CLI Commands

**Objective**: Add `register-job`, `run-job`, `job enable`, `job disable` CLI commands.

**Tasks**:

- `aaiclick register-job <entrypoint> --name <name> --schedule "cron" --kwargs '{...}'`
  - Calls `register_job()`
- `aaiclick run-job <name_or_entrypoint> [--kwargs '{...}']`
  - Calls `run_job()`
- `aaiclick job enable <name>` / `aaiclick job disable <name>`
  - Calls `enable_job()` / `disable_job()`
- `aaiclick job list` — update to show `run_type` column
- `aaiclick job get` — update to show `registered_job_id` and `run_type`
- `aaiclick registered-job list` — list registered jobs with schedule and enabled status

**Deliverables**: All CLI commands work end-to-end. Help text updated.

# Phase 6: Tests & Documentation

**Objective**: Comprehensive test coverage and documentation updates.

**Tasks**:

- Tests in `aaiclick/orchestration/test_registered_jobs.py`:
  - Registration, upsert, enable/disable
  - `next_run_at` computation from cron
  - Optimistic lock dedup
  - `run_job` with and without prior registration
  - Scheduled job creation by background worker
- Update `docs/orchestration.md` with registered jobs section
- Update `docs/future.md` — remove scheduling if it was listed there

**Deliverables**: All tests pass. Documentation references implementation.
