Registered Jobs — Implementation Plan
---

Separates job *registration* (catalog of known jobs) from job *execution* (individual runs).
Adds cron-based scheduling driven by the background worker.

# Phase 1: Model & Migration ✅ IMPLEMENTED

**Implementation**: `aaiclick/orchestration/models.py` — see `RegisteredJob`, `RunType`; migration `2bde1ead8ddf`

- `RunType` StrEnum (`SCHEDULED`, `MANUAL`)
- `RegisteredJob` SQLModel: `id`, `name` (unique), `entrypoint`, `enabled`, `schedule`, `default_kwargs`, `next_run_at`, `created_at`, `updated_at`
- `Job` new columns: `run_type` (mandatory), `registered_job_id` (FK)

# Phase 2: Registration Logic ✅ IMPLEMENTED

**Implementation**: `aaiclick/orchestration/registered_jobs.py`

- `register_job()`, `get_registered_job()`, `upsert_registered_job()`
- `enable_job()` / `disable_job()` — return job ID
- `list_registered_jobs(enabled_only)`
- `compute_next_run(cron_expr, after)` — uses `croniter`
- `croniter>=6.0.0` added to dependencies

**Tests**: `aaiclick/orchestration/test_registered_jobs.py` — 21 tests

# Phase 3: Update JobFactory and run-job Flow ✅ IMPLEMENTED

**Implementation**: `aaiclick/orchestration/decorators.py` — see `JobFactory._create_job()`;
`aaiclick/orchestration/factories.py` — see `create_job()`;
`aaiclick/orchestration/registered_jobs.py` — see `run_job()`

- `JobFactory._create_job()` and `create_job()` accept `run_type` and `registered_job_id`
- `run_job()` — auto-registers if not found, merges `default_kwargs` with overrides

# Phase 4: Scheduler in Background Worker ✅ IMPLEMENTED

**Implementation**: `aaiclick/orchestration/background/background_worker.py` — see `BackgroundWorker._check_schedules()`

- Optimistic lock on `next_run_at` prevents duplicate runs
- Creates Job (`run_type=SCHEDULED`) + entry Task via raw SQL
- Backend-specific SQL split into `sqlite_handler.py` / `pg_handler.py` via `BackgroundHandler` ABC

**Tests**: `aaiclick/orchestration/background/test_scheduler.py` — 4 tests

# Phase 5: CLI Commands ✅ IMPLEMENTED

**Implementation**: `aaiclick/orchestration/cli.py`, `aaiclick/__main__.py`

- `aaiclick register-job <entrypoint> [--name] [--schedule] [--kwargs]`
- `aaiclick run-job <name> [--kwargs]`
- `aaiclick job enable <name>` / `aaiclick job disable <name>`
- `aaiclick registered-job list`
- `aaiclick job list` / `job get` — show `run_type` column

# Phase 6: Tests & Documentation ✅ IMPLEMENTED

- `docs/orchestration.md` — registered jobs section, updated CLI reference, updated BackgroundWorker docs
- All 156 tests pass (25 new: 21 registered jobs CRUD + 4 scheduler)
