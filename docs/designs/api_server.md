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

All of view models, `internal_api`, REST, MCP, JWT auth + RBAC (see
[Authentication](#authentication)), and `start_execution_worker` are implemented.

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
         │   list_execution_workers(filter)      → Page[ExecutionWorkerView]                    │
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
                                     ExecutionWorkerView, RegisteredJobView
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
    execution_workers.py                     list_execution_workers, start_execution_worker, stop_execution_worker
    objects.py                     list_objects, get_object, delete_object, purge_objects
    setup.py                       setup, migrate, bootstrap_ollama
  __main__.py                      ← argparse + text/JSON renderers only
                                     (zero business logic)
  server/                          ← FastAPI + FastMCP (optional extra)
    __init__.py
    app.py                         FastAPI app instance; mounts routers + MCP
    deps.py                        AsyncSession / ChClient dependency providers
    errors.py                      internal_api.errors.* → HTTP Problem mapper
    routers/
      jobs.py                      /jobs, /jobs/{id}, /jobs/{id}/stats,
                                   /jobs/{id}/cancel
      registered_jobs.py           /registered-jobs, enable/disable, run
      tasks.py                     /tasks/{id}
      execution_workers.py                   /execution-workers, /execution-workers/{id}/stop
      objects.py                   /objects, /objects/{name}
    mcp.py                         FastMCP server; tools wrap internal_api.*
```

All HTTP routes are mounted under a single versioned prefix —
**`/api/v0`** — declared once in `server/app.py` as `API_PREFIX` and passed to
`include_router(..., prefix=API_PREFIX)`. Individual router files declare
paths *relative* to the prefix (`/jobs`, `/execution-workers`, ...) so the version lives
in exactly one place. The `v0` segment is deliberate: the schema is still
experimental and may break; the number advances to `v1` once the contract
stabilises.

# View Model Catalogue

Auth + worker-spawn add `StartWorkerRequest` and expand `ProblemCode` — see
[Spawning workers](#spawning-workers--post-apiv0workers) and
[Authentication](#authentication).

## Shared (`aaiclick/view_models.py`)

| Model                  | Purpose                                                      |
|------------------------|--------------------------------------------------------------|
| `Page[T]`              | Generic paged list: `items`, `total`, `next_cursor`          |
| `Problem`              | Error shape: `title`, `status`, `detail`, `code`             |
| `RefId`                | `int \| str` — numeric id or human-readable name             |
| `SnowflakeId`          | `int` serialized as a JSON **string** (`when_used="json"`)   |

**Snowflake ids on the wire**: every 64-bit id field (`id`, `job_id`,
`execution_worker_id`, …) is typed `SnowflakeId`, so it serializes to a JSON *string*.
This keeps ids exact in JavaScript, which would otherwise round integers past
`Number.MAX_SAFE_INTEGER` (2^53-1). It is serialization-only — the Python
attribute and `model_dump()` stay `int`, and request paths/bodies still accept
the numeric string and coerce it back to `int`. The generated SPA types
(`src/api/schema.ts`) follow, declaring these fields `string`.
| `RunJobRequest`        | `name`, `kwargs`, `preservation_mode`                        |
| `RegisterJobRequest`   | `entrypoint`, `schedule`, `defaults`                         |
| `JobListFilter`        | `status`, `name`, `since`, `limit`, `cursor`                 |
| `RegisteredJobFilter`  | `enabled`, `name`, `limit`, `cursor`                         |
| `ExecutionWorkerFilter`         | `status`, `limit`                                            |
| `ObjectFilter`         | `prefix`, `scope`, `limit`, `cursor`                         |

## Orchestration (`aaiclick/orchestration/view_models.py`)

| Model                  | Populated fields                                                                 |
|------------------------|----------------------------------------------------------------------------------|
| `JobView`              | `id`, `name`, `status`, `created_at`, `started_at`, `completed_at`, `error`      |
| `JobDetail`            | everything in `JobView` + `tasks: list[TaskView]`, `duration_ms` (computed)      |
| `JobStatsView`         | `job_id`, `job_name`, `status_counts`, `wall_time_ms`, `exec_time_ms`, `tasks`   |
| `TaskView`             | `id`, `job_id`, `entrypoint`, `status`, `attempt`, `started_at`, `completed_at`  |
| `TaskDetail`           | everything in `TaskView` + `kwargs`, `result_ref`, `execution_worker_id`                   |
| `ExecutionWorkerView`           | `id`, `status`, `started_at`, `last_heartbeat`, `tasks_completed`, `tasks_failed` |
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
`JobStatus`, `TaskStatus`, `ExecutionWorkerStatus`, `RunType`, `PreservationMode`.
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
| `execution-worker list`              | `list_execution_workers(filter)`             | `GET /execution-workers`                     | `list_execution_workers`            |
| `execution-worker start`             | `start_execution_worker()`                   | `POST /execution-workers`                    | `start_execution_worker`            |
| `execution-worker stop <id>`         | `stop_execution_worker(id)`                  | `POST /execution-workers/{id}/stop`          | `stop_execution_worker`             |
| `data list`                | `list_objects(filter)`             | `GET /objects`                     | `list_objects`            |
| `data get <name>`          | `get_object(name)`                 | `GET /objects/{name}`              | `get_object`              |
| `data delete <name>`       | `delete_object(name)`              | `DELETE /objects/{name}`           | `delete_object`           |
| `data purge`               | `purge_objects(filter)`            | `POST /objects:purge`              | `purge_objects`           |
| *(new)* task detail        | `get_task(id)`                     | `GET /tasks/{id}`                  | `get_task`                |
| `explain <table> [q]`      | `lineage_ai.explain_lineage(...)`  | —                                  | —                         |
| `debug <table> "<q>"`      | `lineage_ai.debug_result(...)`     | —                                  | —                         |

`job wait <ref>` and `run-job --progress` have no row: they are CLI-only
compositions over `job_stats`. Blocking a request for up to 600s is not a
valid server shape — REST clients poll `GET /jobs/{ref}/stats` instead.

`explain` / `debug` need the `ai` extra, so their wrappers live in
`internal_api.lineage_ai`, imported on demand by the CLI and never from
`internal_api.__init__`. REST and MCP expose only the AI-independent
primitives in `internal_api.lineage`; the calling agent composes them itself.


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
are still evolving. The `v0` segment signals "experimental,
subject to breaking change" to downstream UIs / SDK generators; we graduate to
`/api/v1` once the schema has settled and external callers exist.

- **Error mapping**: one exception handler turns `internal_api.errors.NotFound`
  into `404 Problem`, `Conflict` into `409`, `Invalid` into `422`,
  `Unauthorized` into `401`.
- **OpenAPI**: derived automatically from view models; served at
  `/api/v0/openapi.json` with Swagger UI at `/api/v0/docs`.
- **Logs**: `GET /tasks/{id}/logs` returns a `TaskLogsView` whose `lines` are
  `LogLine` objects (`stream` = `stdout`/`stderr`, `text`) read from the
  ClickHouse `task_logs` stream (host-independent); optional `?tail=N` bounds the
  response to the last N lines.

## Spawning workers — `POST /api/v0/execution-workers`

**Implementation**: `internal_api.execution_workers.start_execution_worker`,
`aaiclick/server/routers/execution_workers.py`.

The CLI's `execution-worker start` is a blocking process loop that runs until
SIGTERM — it does not fit the request/response pattern. The REST
endpoint spawns a **detached subprocess** and returns `202 Accepted`
once the fork/exec has succeeded. The caller polls `GET /api/v0/execution-workers`
if it wants to see the new row:

```
POST /api/v0/execution-workers
Content-Type: application/json

{ "max_tasks": 100 }          # all fields optional → unlimited if omitted
```

Request body maps to `StartWorkerRequest` (new shared view model). The
handler flow:

1. `internal_api.execution_workers.start_execution_worker(request)` refuses in local mode
   (`is_local() → raise Invalid`) — same constraint as the CLI.
2. Spawn `python -m aaiclick execution-worker start [--max-tasks N]` with
   `asyncio.create_subprocess_exec(..., start_new_session=True)` so the
   child survives the HTTP request. POSIX-only, matching the project's
   Linux / macOS scope; Windows is not a supported deployment target.
3. If exec raises (`FileNotFoundError`, `PermissionError`), translate
   to `ExecutionWorkerSpawnFailed` (a `Conflict` subclass) → `503`. Otherwise
   return `None`.
4. Router returns `202 Accepted` with header
   `Location: /api/v0/execution-workers` and an empty body.

The caller polls `GET /api/v0/execution-workers` to observe the new worker row;
whether the child has finished registering (or has already crashed) is
an orchestration-layer concern, not an HTTP concern. This keeps the
endpoint idempotent in intent ("ensure one more worker is running"),
avoids a new DB column, and sidesteps the race where two concurrent
spawns would both claim "the next id."

Failure modes:

| Scenario                                | HTTP | `Problem.code`        |
|-----------------------------------------|------|-----------------------|
| Local mode (chdb + SQLite)              | 422  | `invalid`             |
| Subprocess exec raises (missing binary) | 503  | `execution_worker_spawn_failed` |
| Insufficient scope (post-scope rollout) | 403  | `forbidden`           |

The server does **not** track child PIDs — shutdown uses the existing
cooperative `stop_execution_worker` path, which writes a stop signal to SQL and
relies on the worker's own polling loop to exit. Orphan reaping remains
the orchestration layer's responsibility, identical to CLI-spawned
workers.

!!! warning "`start_execution_worker` requires distributed backends"
    The endpoint raises `422 Invalid` in local mode (chdb + SQLite),
    where every process shares one chdb data path and a spawned child
    would deadlock on the file lock. Use the CLI's `local start` verb in
    local mode — it runs worker + background in a single process.

## New / changed view models

| Model                 | Where                          | Purpose                                                       |
|-----------------------|--------------------------------|---------------------------------------------------------------|
| `StartWorkerRequest`  | `aaiclick/view_models.py`      | `max_tasks: int \| None`                                      |
| `Unauthorized`        | `aaiclick/internal_api/errors` | Missing / invalid bearer token                                |
| `Forbidden`           | `aaiclick/internal_api/errors` | Reserved for scope rollout; unused in v0                      |
| `ExecutionWorkerSpawnFailed`   | `aaiclick/internal_api/errors` | Detached worker exec failed; `Conflict` subclass → `503`      |
| `Problem.code`        | `aaiclick/view_models.py`      | Extend `ProblemCode` with `UNAUTHORIZED`, `FORBIDDEN`, `WORKER_SPAWN_FAILED` |

`Forbidden` ships in v0 so the error-mapping table is stable; no route
raises it until scopes land.

## Live updates — `GET /api/v0/events`

A `text/event-stream` of `changed` events (no payload) fed by Postgres
`LISTEN`/`NOTIFY` in distributed mode and an in-process bus in local mode.
Requires a principal and a tenant like every other resource route; the
signal itself carries nothing tenant-specific. Design and client behaviour:
`docs/designs/frontend.md` — Live updates. **Implementation**:
`aaiclick/server/events.py` — see `stream_events`, `live_events`.

# MCP Surface

`aaiclick/server/mcp.py` exposes a module-level `mcp = FastMCP("aaiclick")`
instance. Each tool is a direct wrapper that opens the surrounding context:

```python
@mcp.tool
async def run_job(request: RunJobRequest) -> JobView:
    async with orch_context(with_ch=True):
        return await internal_api.run_job(request)
```

The server mounts it on the main FastAPI app under `/mcp`:

```python
# aaiclick/server/app.py
_mcp_app = mcp.http_app(path="/")
app = FastAPI(..., lifespan=_mcp_app.lifespan)
app.mount("/mcp", _mcp_app)
```

FastMCP generates tool schemas from the pydantic types — identical inputs
and outputs to the REST surface.

**Testing**: use FastMCP's in-process client against the same module-level
`mcp` instance — no HTTP round-trip, no uvicorn:

```python
from fastmcp import Client
from aaiclick.server.mcp import mcp

async with Client(mcp) as client:
    result = await client.call_tool("list_jobs", {})
    page = Page[JobView].model_validate(result.structured_content)
```

Internal-API errors (`NotFound` / `Conflict` / `Invalid`) surface as
`fastmcp.exceptions.ToolError` on the client.

# Running the server

The app is exposed as a module-level `app = FastAPI(...)` in
`aaiclick/server/app.py` — no factory, no wrapper module. Run with
uvicorn directly:

```bash
pip install 'aaiclick[server]'
uvicorn aaiclick.server.app:app
```

In **local mode** (`chdb` + `sqlite`) the lifespan auto-starts the
`BackgroundWorker` and the execution worker — submitting a job via
the REST or MCP surface picks it up and runs it in the same process.
There is no separate worker process to launch.

For convenience, the CLI exposes the same flow:

```bash
python -m aaiclick local start            # workers + REST + MCP on 127.0.0.1:5255
python -m aaiclick local start --port 9000
python -m aaiclick local start --reload   # auto-restart on code change (dev)
```

In **distributed mode** (PostgreSQL + ClickHouse) the lifespan is a
no-op and the worker / background processes run separately:

```bash
uvicorn aaiclick.server.app:app           # serves REST + MCP
python -m aaiclick execution-worker start           # one or more worker processes
python -m aaiclick background start       # one cleanup process
```

Host, port, workers, reload, TLS, etc. are uvicorn's standard flags and
env vars (`UVICORN_HOST`, `UVICORN_PORT`, …); aaiclick does not invent a
parallel `AAICLICK_SERVER_*` namespace.

# Configuration

The server reuses the CLI's existing env vars and adds a single auth knob:

| Variable               | Purpose                                              | Status                 |
|------------------------|------------------------------------------------------|------------------------|
| `AAICLICK_CH_URL`      | ClickHouse connection URL                            | Existing (see `backend.py`) |
| `AAICLICK_SQL_URL`     | Orchestration SQL backend URL                        | Existing (see `backend.py`) |
| `AAICLICK_JWT_SECRET`  | HS256 signing secret (required in distributed mode)  | See `Authentication`   |
| `UVICORN_HOST`         | Bind host (uvicorn native)                           | Standard uvicorn       |
| `UVICORN_PORT`         | Bind port (uvicorn native)                           | Standard uvicorn       |

# Authentication

**Design**: `docs/designs/auth.md`. **Implementation**: `aaiclick/server/auth.py`
(principal resolution + RBAC), `aaiclick/auth/` (models, security, store),
`aaiclick/internal_api/auth.py` (login/refresh/logout), wired in
`aaiclick/server/app.py`.

Username/password users with two roles (`admin` / `viewer`),
authenticated by a short-lived access JWT + rotating refresh token. The CLI
runs `internal_api` in-process and never crosses this HTTP-transport layer.

- **Gating**: mode-derived, not a flag — open in local mode (synthetic admin +
  startup `WARNING`), enforced in distributed mode (requires
  `AAICLICK_JWT_SECRET`, else the server refuses to start).
- **Login**: `POST /api/v0/auth/login` `{username, password}` → access +
  refresh tokens; `POST /auth/refresh` rotates; `POST /auth/logout` revokes;
  `GET /auth/me` returns the current principal.
- **Enforcement**: `HTTPBearer` extracts the access JWT; `require_principal`
  guards every `/api/v0/*` router and `require_admin` guards every mutating
  endpoint and all of `/users`. Reads need only a valid principal.
- **MCP**: the `/mcp` mount is admin-only via an ASGI middleware (`Depends`
  does not propagate into mounted sub-apps).

Open paths (never 401): `GET /health`, `/api/v0/openapi.json`, `/docs`,
`/redoc`, and `/api/v0/auth/login|refresh`.

The error envelope is the standard `Problem` (`code="unauthorized"` / 401 with
`WWW-Authenticate: Bearer`, or `code="forbidden"` / 403).

## Future

Per-tool MCP RBAC, a user-management UI, long-lived API tokens / PATs with
scopes, OAuth 2.0 / OIDC, and a per-request audit log are tracked in
`docs/designs/future.md`.

# Non-Goals

- **Streaming log envelopes** — `GET /tasks/{id}/logs` returns the captured
  lines in one `TaskLogsView`; live per-line streaming (`TaskLogLine`) is a
  follow-up tracked in `docs/designs/future.md`.
- **WebSockets** — the UI's live update channel is a follow-up once the REST
  surface stabilises.
- **Backwards-compatible shims for old CLI code paths** — during migration,
  the old `*_cmd` functions are deleted outright; no dual-path maintenance.
