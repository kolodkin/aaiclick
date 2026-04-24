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
    app.py                         FastAPI app instance; mounts routers + MCP
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
```

All HTTP routes are mounted under a single versioned prefix —
**`/api/v0`** — declared once in `server/app.py` as `API_PREFIX` and passed to
`include_router(..., prefix=API_PREFIX)`. Individual router files declare
paths *relative* to the prefix (`/jobs`, `/workers`, ...) so the version lives
in exactly one place. The `v0` segment is deliberate: the schema is still
experimental and may break; the number advances to `v1` once the contract
stabilises.

# View Model Catalogue

Phase 5 adds `StartWorkerRequest` and expands `ProblemCode` — see
[Spawning workers](#spawning-workers--post-apiv0workers) and
[Authentication](#authentication) for the additions.

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
  into `404 Problem`, `Conflict` into `409`, `Invalid` into `422`,
  `Unauthorized` into `401`.
- **OpenAPI**: derived automatically from view models; served at
  `/api/v0/openapi.json` with Swagger UI at `/api/v0/docs`.
- **Logs**: out of scope. Task log files are served statically or streamed
  verbatim; no log envelope view model.

## Spawning workers — `POST /api/v0/workers`

The CLI's `worker start` is a blocking process loop that runs until
SIGTERM — it does not fit the request/response pattern. The REST
endpoint therefore spawns a **detached subprocess** and returns once the
spawned worker has registered itself in SQL:

```
POST /api/v0/workers
Content-Type: application/json

{ "max_tasks": 100 }          # all fields optional → unlimited if omitted
```

Request body maps to `StartWorkerRequest` (new shared view model). The
handler flow:

1. `internal_api.workers.start_worker(request)` refuses in local mode
   (`is_local() → raise Invalid`) — same constraint as the CLI.
2. Generate a one-shot correlation id. Pass it to the subprocess via
   `AAICLICK_WORKER_CORRELATION_ID=<uuid>`; the worker's registration
   path stamps it onto the `Worker` row at startup.
3. Spawn `python -m aaiclick worker start [--max-tasks N]` with
   `asyncio.create_subprocess_exec(..., start_new_session=True)` so the
   child survives the HTTP request. POSIX-only, matching the project's
   Linux / macOS scope; Windows is not a supported deployment target.
4. Poll `SELECT * FROM worker WHERE correlation_id = :id` with a
   bounded timeout (default 10s) until a row appears.
5. Return `WorkerView` for the registered row.

Failure modes:

| Scenario                                 | HTTP | `Problem.code`         |
|------------------------------------------|------|------------------------|
| Local mode (chdb + SQLite)               | 422  | `invalid`              |
| Subprocess exits before registering      | 503  | `worker_spawn_failed`  |
| Registration timeout (no row within 10s) | 503  | `worker_spawn_timeout` |
| Insufficient scope (post-scope rollout)  | 403  | `forbidden`            |

Once registered, the worker is autonomous. The server does **not** track
child PIDs — shutdown uses the existing cooperative
`stop_worker` path, which writes a stop signal to SQL and relies on the
worker's own polling loop to exit. Orphan reaping remains the
orchestration layer's responsibility, identical to CLI-spawned workers.

The correlation id is what ties an HTTP request to "its" worker row.
The simpler alternative — snapshot `max(worker.id)` before spawn, wait
for a higher id — races under concurrent spawns: two simultaneous
`POST /workers` calls both claim the first new row. A unique id per
request sidesteps coordination entirely.

!!! warning "`start_worker` requires distributed backends"
    The endpoint raises `422 Invalid` in local mode (chdb + SQLite),
    where every process shares one chdb data path and a spawned child
    would deadlock on the file lock. Use the CLI's `local start` verb in
    local mode — it runs worker + background in a single process.

## New / changed view models

| Model                 | Where                          | Purpose                                          |
|-----------------------|--------------------------------|--------------------------------------------------|
| `StartWorkerRequest`  | `aaiclick/view_models.py`      | `max_tasks: int \| None`                         |
| `Unauthorized`        | `aaiclick/internal_api/errors` | Missing / invalid bearer token                   |
| `Forbidden`           | `aaiclick/internal_api/errors` | Reserved for scope rollout; unused in v0         |
| `Problem.code`        | `aaiclick/view_models.py`      | Extend `ProblemCode` with `UNAUTHORIZED`, `FORBIDDEN`, `WORKER_SPAWN_FAILED`, `WORKER_SPAWN_TIMEOUT` |

`Forbidden` ships in v0 so the error-mapping table is stable; no route
raises it until scopes land.

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
# dev:
uvicorn aaiclick.server.app:app --reload
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
| `AAICLICK_API_TOKEN`   | Shared bearer token for `/api/v0/*` and `/mcp` (v0)  | See `Authentication`   |
| `UVICORN_HOST`         | Bind host (uvicorn native)                           | Standard uvicorn       |
| `UVICORN_PORT`         | Bind port (uvicorn native)                           | Standard uvicorn       |

# Authentication

The `/api/v0/*` REST surface and the `/mcp` mount share one bearer-token
check in v0. The CLI, in-process MCP client, and router-level tests all
bypass the check — authentication is an HTTP-transport concern, not an
internal-API concern.

## Static token (v0)

- **Token source**: the `AAICLICK_API_TOKEN` env var, read per-request
  via a module-level helper so tests can flip it with `monkeypatch`.
  No DB-backed token store, no rotation, no scopes.
- **Enforcement**: if the env var is set, every request to `/api/v0/*`
  and `/mcp/*` must carry `Authorization: Bearer <token>`. Mismatches
  return `401 Problem` (`code="unauthorized"`). Missing headers return
  `401` with a `WWW-Authenticate: Bearer` response header.
- **Unset token → open server**: when `AAICLICK_API_TOKEN` is unset, the
  check is a no-op and the server logs a `WARNING` at startup
  (`"AAICLICK_API_TOKEN unset — server is open"`). This preserves the
  "localhost-only, no config needed" onboarding path while making the
  exposure visible in logs.
- **Timing-safe compare**: the check uses `hmac.compare_digest`, not `==`.

!!! warning "Unset token ≠ safe in production"
    An unset `AAICLICK_API_TOKEN` means *any* network-reachable client
    can hit the API. Run behind a bind-to-localhost socket, a reverse
    proxy, or a firewall rule — or set the token.

## Wiring

One FastAPI dependency, attached once at the mount site — not at every
endpoint:

```python
# aaiclick/server/auth.py
async def require_bearer(authorization: str | None = Header(default=None)) -> None:
    token = os.environ.get("AAICLICK_API_TOKEN")
    if token is None:
        return  # open-server mode
    if authorization is None or not authorization.startswith("Bearer "):
        raise Unauthorized("missing bearer token")
    if not hmac.compare_digest(authorization.removeprefix("Bearer "), token):
        raise Unauthorized("invalid bearer token")

# aaiclick/server/app.py
for router in (jobs.router, registered_jobs.router, tasks.router,
               workers.router, objects.router):
    app.include_router(router, prefix=API_PREFIX,
                       dependencies=[Depends(require_bearer)])

app.mount(MCP_PATH, _mcp_app, ...)  # protected by ASGI middleware — see below
```

The `/mcp` mount is protected by a lightweight ASGI middleware that runs
the same check before delegating to the FastMCP sub-app. `Depends()` does
not propagate into mounted sub-apps, so a middleware is required at the
mount boundary.

## What stays open

| Path                       | Auth required? | Why                                                                  |
|----------------------------|----------------|----------------------------------------------------------------------|
| `GET /health`              | No             | Liveness / uptime probes must never 401                              |
| `GET /api/v0/openapi.json` | No             | FastAPI serves it at the app level; router-dependency does not cover it, and an info-leak isn't a v0 concern |
| `GET /api/v0/docs`         | No             | Same                                                                 |
| `GET /api/v0/redoc`        | No             | Same                                                                 |

Gating the schema / docs behind auth would need a middleware (like the
`/mcp` one below) or `openapi_url=None` + a hand-written authed route —
both are deferred to the DB-backed-tokens phase alongside scopes.

## Error envelope

```json
{
  "title": "Unauthorized",
  "status": 401,
  "detail": "missing bearer token",
  "code": "unauthorized"
}
```

`Unauthorized` is a new `internal_api.errors.*` subclass. The server-side
handler sets the `WWW-Authenticate: Bearer` response header; the CLI and
MCP paths never raise it because they bypass the bearer check.

## Future (tracked in `docs/future.md`)

- **DB-backed tokens with scopes** — `api_tokens` table, per-token
  `read` / `write` / `admin` scope, CRUD CLI (`aaiclick token issue`,
  `aaiclick token revoke`), rotation, expiry. Scopes gate mutating verbs
  (`cancel_job`, `delete_object`, `start_worker`, `setup`).
- **OAuth 2.0 / OIDC** — for the orchestration UI once a browser client
  exists. Delegated identity, not a concern of the v0 static token.
- **Per-request audit log** — who called what, when. Out of scope until
  token identity exists.

# Non-Goals

- **Streaming log envelopes** — task logs stream as files; no `TaskLogLine`
  view model.
- **WebSockets** — the UI's live update channel is a follow-up once the REST
  surface stabilises.
- **Backwards-compatible shims for old CLI code paths** — during migration,
  the old `*_cmd` functions are deleted outright; no dual-path maintenance.
