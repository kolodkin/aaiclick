API Server: Shared Pydantic I/O Layer
---

Single source of truth for the **input/output schemas** and **internal API**
that power three surfaces of aaiclick:

1. **CLI** (`python -m aaiclick ...`)
2. **REST API** (FastAPI — future orchestration UI backend)
3. **MCP** (FastMCP — AI agent tool surface)

All three are thin renderers over one internal API whose signatures are typed
with pydantic view models. The CLI keeps its current human output and gains
`--json` for free. The REST and MCP surfaces derive from the same types, so
their schemas, docs, and client SDKs cannot drift from the CLI.

Companion: `docs/api_server_implementation_plan.md` covers the phased rollout.

# Motivation

Today, CLI-backing logic is scattered across domain modules
(`aaiclick/data/object/cli.py`, helpers inside `aaiclick/__main__.py`, etc.)
and mixes business logic with `print()` calls and `argparse` parsing. Adding a
REST API or MCP server would require re-implementing the same verbs against
the same domain entities — and keeping three implementations in sync.

A shared I/O layer lets us:

- Write each command **once**, in `aaiclick/internal_api/`, returning a typed
  view model.
- Render it three ways (CLI text, CLI `--json`, HTTP JSON, MCP tool result)
  without duplicating logic.
- Generate an OpenAPI spec directly from pydantic models.
- Keep database models (`SQLModel`) and wire models (`pydantic`) independent
  so DB schema changes don't ripple into REST clients.

# Architecture

```
         ┌───────── aaiclick/internal_api/ (typed domain functions) ─────────┐
         │   list_jobs(filter)         → Page[JobView]                       │
         │   get_job(ref)              → JobDetail                           │
         │   run_job(req)              → JobView                             │
         │   cancel_job(ref)           → JobView                             │
         │   list_workers(filter)      → Page[WorkerView]                    │
         │   list_objects(filter)      → Page[ObjectView]                    │
         │   get_object(ref)           → ObjectDetail                        │
         │   ...                                                             │
         └───────────────────────────────┬───────────────────────────────────┘
                                         │
             ┌───────────────────────────┼───────────────────────────┐
             ▼                           ▼                           ▼
        CLI renderer              FastAPI routers              FastMCP tools
      (text / --json)          (response_model=...)          (typed return)
      aaiclick/__main__.py     aaiclick/server/routers/      aaiclick/server/mcp.py
```

**Key property**: every command is written in exactly one place. The three
surfaces do nothing but parse input, call `internal_api.*`, and render the
result in their native format.

# Package Layout

```
aaiclick/
  view_models.py                   ← shared view models (cross-domain)
                                     Page[T], Problem, RefId, *Request, *Filter
  orchestration/
    view_models.py                 ← orchestration domain
                                     JobView, JobDetail, JobStatsView,
                                     TaskView, TaskDetail,
                                     WorkerView, RegisteredJobView
                                     + to_view() adapters (SQLModel → View)
  data/
    view_models.py                 ← data domain
                                     ObjectView, ObjectDetail,
                                     SchemaView, ColumnView
                                     + to_view() adapters
  internal_api/                    ← business logic relocated from per-module cli.py
    __init__.py                    (public re-exports)
    errors.py                      NotFound, Conflict, Invalid
    jobs.py                        list_jobs, get_job, job_stats, cancel_job, run_job
    registered_jobs.py             list_registered_jobs, register_job,
                                   enable_job, disable_job
    tasks.py                       get_task
    workers.py                     list_workers, start_worker, stop_worker
    objects.py                     list_objects, get_object, delete_object, purge_objects
    setup.py                       setup, migrate, bootstrap_ollama
  __main__.py                      ← argparse + text/JSON renderers only
                                     (zero business logic)
  server/                          ← FastAPI + FastMCP (optional extra)
    __init__.py
    app.py                         FastAPI app factory; mounts routers + MCP
    deps.py                        AsyncSession / ChClient dependency providers
    errors.py                      internal_api.errors.* → HTTP Problem mapper
    routers/
      jobs.py                      /jobs, /jobs/{id}, /jobs/{id}/stats,
                                   /jobs/{id}/cancel
      registered_jobs.py           /registered-jobs, enable/disable, run
      tasks.py                     /tasks/{id}
      workers.py                   /workers, /workers/{id}/stop
      objects.py                   /objects, /objects/{name}
    mcp.py                         FastMCP server; tools wrap internal_api.*
    __main__.py                    `python -m aaiclick.server` → uvicorn
```

All HTTP routes are mounted under a single versioned prefix —
**`/api/v0`** — declared once in `server/app.py` as `API_PREFIX` and passed to
`include_router(..., prefix=API_PREFIX)`. Individual router files declare
paths *relative* to the prefix (`/jobs`, `/workers`, ...) so the version lives
in exactly one place. The `v0` segment is deliberate: the schema is still
experimental and may break; the number advances to `v1` once the contract
stabilises.

# View Model Catalogue

## Shared (`aaiclick/view_models.py`)

| Model                  | Purpose                                                      |
|------------------------|--------------------------------------------------------------|
| `Page[T]`              | Generic paged list: `items`, `total`, `next_cursor`          |
| `Problem`              | Error shape: `title`, `status`, `detail`, `code`             |
| `RefId`                | `int \| str` — numeric id or human-readable name             |
| `RunJobRequest`        | `name`, `kwargs`, `preservation_mode`                        |
| `RegisterJobRequest`   | `entrypoint`, `schedule`, `defaults`                         |
| `JobListFilter`        | `status`, `name`, `since`, `limit`, `cursor`                 |
| `RegisteredJobFilter`  | `enabled`, `name`, `limit`, `cursor`                         |
| `WorkerFilter`         | `status`, `limit`                                            |
| `ObjectFilter`         | `prefix`, `scope`, `limit`, `cursor`                         |

## Orchestration (`aaiclick/orchestration/view_models.py`)

| Model                  | Populated fields                                                                 |
|------------------------|----------------------------------------------------------------------------------|
| `JobView`              | `id`, `name`, `status`, `created_at`, `started_at`, `completed_at`, `error`      |
| `JobDetail`            | everything in `JobView` + `tasks: list[TaskView]`, `duration_ms` (computed)      |
| `JobStatsView`         | `job_id`, `job_name`, `status_counts`, `wall_time_ms`, `exec_time_ms`, `tasks`   |
| `TaskView`             | `id`, `job_id`, `entrypoint`, `status`, `attempt`, `started_at`, `completed_at`  |
| `TaskDetail`           | everything in `TaskView` + `kwargs`, `result_ref`, `log_path`, `worker_id`       |
| `WorkerView`           | `id`, `status`, `started_at`, `last_heartbeat`, `tasks_completed`, `tasks_failed` |
| `RegisteredJobView`    | `name`, `entrypoint`, `schedule`, `enabled`, `defaults`                          |

## Data (`aaiclick/data/view_models.py`)

| Model          | Populated fields                                                                 |
|----------------|----------------------------------------------------------------------------------|
| `ColumnView`   | `name`, `type`, `nullable`, `array_depth`, `low_cardinality`                     |
| `SchemaView`   | `columns: list[ColumnView]`, `order_by`, `engine`                                |
| `ObjectView`   | `name`, `table`, `scope`, `persistent`, `row_count`, `size_bytes`, `created_at`  |
| `ObjectDetail` | everything in `ObjectView` + `table_schema: SchemaView`, `lineage_summary`       |

## Enums

Reuse existing enums from `aaiclick/orchestration/models.py`:
`JobStatus`, `TaskStatus`, `WorkerStatus`, `RunType`, `PreservationMode`.
View models import **enums only**, never SQLModel classes.

## View vs Detail

- **View** — the shape returned by *list* endpoints. Small, no nested
  collections, safe to render in a table.
- **Detail** — the shape returned by *get* endpoints. Extends the View with
  nested collections (`tasks`), derived fields (`duration_ms`), and any
  expensive lookups the list form omits.

Split keeps list payloads compact without forking into three-per-surface
model families.

# Internal API Contract

Every function in `aaiclick/internal_api/` follows one shape:

```python
async def list_jobs(filter: JobListFilter = JobListFilter()) -> Page[JobView]: ...

async def list_objects(filter: ObjectFilter = ObjectFilter()) -> Page[ObjectView]: ...
```

Rules:

- **Input**: primitives or `*Request` / `*Filter` view models.
- **Output**: a view model (`JobView`, `Page[JobView]`, `JobDetail`, ...).
- **Contexts arrive via ContextVars, not parameters**: every function runs
  inside an active `orch_context()` (orchestration) or `data_context()`
  (data) and reads SQL/CH resources through the getters
  (`get_sql_session()`, `get_ch_client()`). This matches the rest of the
  codebase — decorators, execution, CRUD helpers — so callers do not have
  to thread resources through. CLI wrappers, FastAPI request handlers, and
  MCP tools each establish the surrounding context once per invocation.
- **Errors** raise `internal_api.errors.*` (`NotFound`, `Conflict`, `Invalid`).
  CLI formats them; FastAPI maps them to `Problem` + HTTP status; FastMCP
  surfaces them as tool errors.
- **No side effects on I/O** — no `print`, no `sys.exit`, no argparse.

## CLI verb → internal_api → REST → MCP

All REST paths share a common `/api/v0` prefix — see
[REST Surface](#rest-surface) for the rationale.

| CLI today                  | Internal API                       | REST (under `/api/v0`)             | MCP tool                  |
|----------------------------|------------------------------------|------------------------------------|---------------------------|
| `job list`                 | `list_jobs(filter)`                | `GET /jobs`                        | `list_jobs`               |
| `job get <ref>`            | `get_job(ref)`                     | `GET /jobs/{ref}`                  | `get_job`                 |
| `job stats <ref>`          | `job_stats(ref)`                   | `GET /jobs/{ref}/stats`            | `job_stats`               |
| `job cancel <ref>`         | `cancel_job(ref)`                  | `POST /jobs/{ref}/cancel`          | `cancel_job`              |
| `run-job <name>`           | `run_job(RunJobRequest)`           | `POST /jobs:run`                   | `run_job`                 |
| `register-job <entry>`     | `register_job(RegisterJobRequest)` | `POST /registered-jobs`            | `register_job`            |
| `registered-job list`      | `list_registered_jobs(filter)`     | `GET /registered-jobs`             | `list_registered_jobs`    |
| `job enable <name>`        | `enable_job(name)`                 | `POST /registered-jobs/{n}/enable` | `enable_job`              |
| `job disable <name>`       | `disable_job(name)`                | `POST /registered-jobs/{n}/disable`| `disable_job`             |
| `worker list`              | `list_workers(filter)`             | `GET /workers`                     | `list_workers`            |
| `worker start`             | `start_worker()`                   | `POST /workers`                    | `start_worker`            |
| `worker stop <id>`         | `stop_worker(id)`                  | `POST /workers/{id}/stop`          | `stop_worker`             |
| `data list`                | `list_objects(filter)`             | `GET /objects`                     | `list_objects`            |
| `data get <name>`          | `get_object(name)`                 | `GET /objects/{name}`              | `get_object`              |
| `data delete <name>`       | `delete_object(name)`              | `DELETE /objects/{name}`           | `delete_object`           |
| `data purge`               | `purge_objects(filter)`            | `POST /objects:purge`              | `purge_objects`           |
| *(new)* task detail        | `get_task(id)`                     | `GET /tasks/{id}`                  | `get_task`                |

# CLI Rendering Contract

`aaiclick/__main__.py` holds argparse wiring and two renderers — nothing else:

```python
async def cmd_job_list(args):
    async with orch_context(with_ch=False):
        page = await internal_api.list_jobs(_filter_from_args(args))
    if args.json:
        print(page.model_dump_json())
    else:
        _render_jobs_table(page.items)
```

- **Default output**: the same human tables and single-line summaries the CLI
  prints today. The renderer reads fields off the view model — never from DB
  rows — so table columns and JSON fields cannot drift.
- **`--json` flag**: `print(model.model_dump_json())`. Available on every
  command group for symmetry with REST.
- **Exit codes**: owned by `__main__.py`. `internal_api` signals outcomes
  through return values and exceptions.

# REST Surface

`aaiclick/server/app.py` exposes a FastAPI app. All resource routes mount under
a single versioned prefix — declared once and reused by every router:

```python
# aaiclick/server/app.py
API_PREFIX = "/api/v0"                 # pre-1.0 — the contract may still churn

app = FastAPI(title="aaiclick")
app.include_router(jobs.router,             prefix=API_PREFIX)
app.include_router(registered_jobs.router,  prefix=API_PREFIX)
app.include_router(tasks.router,            prefix=API_PREFIX)
app.include_router(workers.router,          prefix=API_PREFIX)
app.include_router(objects.router,          prefix=API_PREFIX)
```

Individual routers declare paths **relative** to the prefix — `/jobs`,
`/registered-jobs`, etc. — so the version lives in exactly one place and can be
bumped to `/api/v1` with a single-line edit.

Each router is a thin wrapper that runs inside an `orch_context()` (or
`data_context()` for data routes) scoped to the request:

```python
# aaiclick/server/routers/jobs.py
router = APIRouter(prefix="/jobs", tags=["jobs"])

@router.get("", response_model=Page[JobView])
async def list_jobs(filter: JobListFilter = Depends(), _=Depends(orch_scope)):
    return await internal_api.list_jobs(filter)
```

The resulting route is `GET /api/v0/jobs`.

`orch_scope` is a FastAPI dependency that enters `orch_context(with_ch=False)`
on request start and exits on response — the contextvar getters inside
`internal_api` see the session/client for the duration of the call.

**Why `/api/v0`?** The shape of the view models, error envelope, and URL layout
are still evolving alongside Phase 3. The `v0` segment signals "experimental,
subject to breaking change" to downstream UIs / SDK generators; we graduate to
`/api/v1` once the schema has settled and external callers exist.

- **Error mapping**: one exception handler turns `internal_api.errors.NotFound`
  into `404 Problem`, `Conflict` into `409`, `Invalid` into `422`.
- **OpenAPI**: derived automatically from view models; served at
  `/api/v0/openapi.json` with Swagger UI at `/api/v0/docs`.
- **Logs**: out of scope. Task log files are served statically or streamed
  verbatim; no log envelope view model.

# MCP Surface

`aaiclick/server/mcp.py` mounts a FastMCP server on the same app. Each tool
is a direct wrapper that opens the surrounding context:

```python
@mcp.tool()
async def run_job(req: RunJobRequest) -> JobView:
    async with orch_context(with_ch=False):
        return await internal_api.run_job(req)
```

FastMCP generates tool schemas from the pydantic models — identical inputs
and outputs to the REST surface.

# Configuration

The server reuses the CLI's existing env vars and adds two new ones for its
own bind address:

| Variable               | Purpose                                    | Status                 |
|------------------------|--------------------------------------------|------------------------|
| `AAICLICK_CH_URL`      | ClickHouse connection URL                  | Existing (see `backend.py`) |
| `AAICLICK_SQL_URL`     | Orchestration SQL backend URL              | Existing (see `backend.py`) |
| `AAICLICK_SERVER_HOST` | Bind host for `python -m aaiclick.server`  | New (Phase 3)          |
| `AAICLICK_SERVER_PORT` | Bind port for `python -m aaiclick.server`  | New (Phase 3)          |

Auth is out of scope for v1 — the server is localhost-only. Token / OAuth
is added when the orchestration UI needs remote access.

# Non-Goals

- **Streaming log envelopes** — task logs stream as files; no `TaskLogLine`
  view model.
- **WebSockets** — the UI's live update channel is a follow-up once the REST
  surface stabilises.
- **Backwards-compatible shims for old CLI code paths** — during migration,
  the old `*_cmd` functions are deleted outright; no dual-path maintenance.
