API Server: Shared Pydantic I/O Layer
---

One set of **input/output schemas** and one **internal API** power three surfaces: the **CLI** (`python -m aaiclick ...`), the **REST API** (FastAPI, backs the UI) and **MCP** (FastMCP, the AI agent tool surface).

# Motivation

Each surface is a thin renderer over one internal API typed with pydantic view models, so:

- Each command is written **once**, in `aaiclick/internal_api/`, returning a typed view model.
- It renders as CLI text, CLI `--json`, HTTP JSON or an MCP tool result without duplicated logic; the CLI gets `--json` for free.
- The OpenAPI spec, docs and client SDKs come straight from the pydantic models, so REST and MCP schemas cannot drift from the CLI.
- Database models (`SQLModel`) and wire models (`pydantic`) stay independent, so DB schema changes don't ripple into REST clients.

# Architecture

```
         ┌──────── aaiclick/internal_api/ (typed domain functions) ────────┐
         │   list_jobs(filter)  → Page[JobView]                            │
         │   get_job(ref)       → JobDetail                                │
         │   run_job(req)       → JobView                                  │
         │   ...                                                           │
         └───────────────────────────────┬─────────────────────────────────┘
                                         │
             ┌───────────────────────────┼───────────────────────────┐
             ▼                           ▼                           ▼
        CLI renderer              FastAPI routers              FastMCP tools
      (text / --json)          (response_model=...)          (typed return)
      aaiclick/__main__.py     aaiclick/server/routers/      aaiclick/server/mcp.py
```

**Key property**: every command lives in exactly one place. The surfaces only parse input, call `internal_api.*` and render the result natively.

# Package Layout

| Path                                    | Holds                                                                          |
|-----------------------------------------|--------------------------------------------------------------------------------|
| `aaiclick/view_models.py`               | Cross-domain wire models: `Page[T]`, `Problem`, `RefId`, `*Request`, `*Filter` |
| `aaiclick/orchestration/view_models.py` | Orchestration views + `to_view()` adapters (SQLModel → View)                   |
| `aaiclick/data/view_models.py`          | Data (Object) views + adapters                                                 |
| `aaiclick/internal_api/`                | Business logic, one module per resource; `errors.py` holds the error types     |
| `aaiclick/__main__.py`                  | argparse + text/JSON renderers only, zero business logic                       |
| `aaiclick/server/`                      | FastAPI app, `routers/`, `mcp.py`, error mapper (optional `server` extra)      |

# View Model Catalogue

## Shared (`aaiclick/view_models.py`)

| Model                     | Purpose                                                                                             |
|---------------------------|-----------------------------------------------------------------------------------------------------|
| `Page[T]`                 | Generic paged list: `items`, `total`, `next_cursor`                                                 |
| `Problem`                 | Error shape: `title`, `status`, `detail`, `code`                                                    |
| `RefId`                   | `int \| str` — numeric id or human-readable name                                                    |
| `SnowflakeId`             | `int` serialized as a JSON **string** (`when_used="json"`)                                          |
| `PageLimit`, `PageOffset` | Bounds on every `*Filter`: `1 ≤ limit ≤ MAX_PAGE_LIMIT` (1000), `offset ≥ 0`; out of range is `422` |
| `UtcDateTime`             | Request datetimes (`since`, `expires_at`, …) normalized to naive UTC                                |

Inputs are `*Request` models (`RunJobRequest`, `RegisterJobRequest`, …) and one `*Filter` per list endpoint. Viewer models (`ObjectQueryRequest`, `DashboardIn`, `RunQueriesRequest` and their results) live in `aaiclick/viewer/view_models.py`; see `docs/designs/viewer.md`.

**Snowflake ids on the wire**: every 64-bit id field (`id`, `job_id`, `execution_worker_id`, …) is a `SnowflakeId`, serialized as a JSON *string* so JavaScript keeps it exact instead of rounding past `Number.MAX_SAFE_INTEGER` (2^53-1). It is serialization-only: the Python attribute and `model_dump()` stay `int`, and request paths/bodies accept the numeric string and coerce it back. The generated SPA types (`src/api/schema.ts`) declare these fields `string`.

## Domain views

Orchestration (`aaiclick/orchestration/view_models.py`: jobs, tasks, workers, registered jobs) and data (`aaiclick/data/view_models.py`: Objects) define the resource views. Status fields reuse the `Literal` types from `aaiclick/orchestration/models.py`; views never embed SQLModel classes — the `to_view()` adapters convert rows.

## View vs Detail

- **View** — returned by *list* endpoints. Small, no nested collections, safe to render in a table.
- **Detail** — returned by *get* endpoints. Extends the View with nested collections (`tasks`), derived fields (`duration_ms`) and expensive lookups the list omits.

This keeps list payloads compact without a model family per surface.

## Required fields on the wire

**Implementation**: `aaiclick/api_model.py`

Every model in the OpenAPI schema extends `ApiModel`, not `BaseModel`. It sets `json_schema_serialization_defaults_required=True`, so a field with a default reads as optional going in and required coming out.

Pydantic builds two JSON schemas per model, and FastAPI picks one per role:

| Role                      | Schema mode   | Field with a default | Without `ApiModel` |
|---------------------------|---------------|----------------------|--------------------|
| Request body, query model | validation    | optional             | optional           |
| Response model            | serialization | required             | optional           |

The response side is true on the wire: the server always serializes every field (no route uses `exclude_none` / `exclude_unset`). The generated SPA types follow — `JobView.total_tasks` is `number`, not `number | undefined` — and `openapi-typescript --default-non-nullable false` keeps defaulted request fields optional (`RegisterJobRequest.replace?`).

A model used in both roles gets two schemas, `Name-Input` and `Name-Output` (today only `ObjectQuery`); code reading `components["schemas"]` must pick the right one.

The option affects schema generation only — validation and `model_dump()` behave as with `BaseModel`. Pydantic config is per class with no global default, so nested models need it too, hence the shared base.

!!! warning "New API models must extend `ApiModel`"
    A plain `BaseModel` still works at runtime, but its defaulted response
    fields regress to optional in `src/api/schema.ts`. A server test walks every
    response schema and fails on such a field.

# Internal API Contract

Every function in `aaiclick/internal_api/` has one shape:

```python
async def list_jobs(filter: JobListFilter = JobListFilter()) -> Page[JobView]: ...
```

- **Input**: primitives or `*Request` / `*Filter` view models.
- **Output**: a view model (`JobView`, `Page[JobView]`, `JobDetail`, ...).
- **Contexts arrive via ContextVars, not parameters**: every function runs inside an active `orch_context()` or `object_context()` and reads resources through the getters (`get_sql_session()`, `get_ch_client()`), like decorators, execution and CRUD helpers do, so callers don't thread resources through. CLI wrappers, request handlers and MCP tools each enter the context once per invocation.
- **Errors** raise `internal_api.errors.*`. The CLI formats them, FastAPI maps them to `Problem` + HTTP status (see [REST Surface](#rest-surface)), FastMCP surfaces them as tool errors.
- **No I/O side effects** — no `print`, no `sys.exit`, no argparse.

## CLI verb → internal_api → REST → MCP

| CLI                          | Internal API                       | REST (under `/api/v0`)               | MCP tool                 |
|------------------------------|------------------------------------|--------------------------------------|--------------------------|
| `job list`                   | `list_jobs(filter)`                | `GET /jobs`                          | `list_jobs`              |
| `job get <ref>`              | `get_job(ref)`                     | `GET /jobs/{ref}`                    | `get_job`                |
| `job stats <ref>`            | `job_stats(ref)`                   | `GET /jobs/{ref}/stats`              | `job_stats`              |
| `job cancel <ref>`           | `cancel_job(ref)`                  | `POST /jobs/{ref}/cancel`            | `cancel_job`             |
| `run-job <name>`             | `run_job(RunJobRequest)`           | `POST /jobs:run`                     | `run_job`                |
| `register-job <entry>`       | `register_job(RegisterJobRequest)` | `POST /registered-jobs`              | `register_job`           |
| `registered-job list`        | `list_registered_jobs(filter)`     | `GET /registered-jobs`               | `list_registered_jobs`   |
| `job enable <name>`          | `enable_job(name)`                 | `POST /registered-jobs/{n}/enable`   | `enable_job`             |
| `job disable <name>`         | `disable_job(name)`                | `POST /registered-jobs/{n}/disable`  | `disable_job`            |
| `execution-worker list`      | `list_execution_workers(filter)`   | `GET /execution-workers`             | `list_execution_workers` |
| `execution-worker start`     | — (blocking CLI loop)              | —                                    | —                        |
| `execution-worker stop <id>` | `stop_execution_worker(id)`        | —                                    | —                        |
| `data list`                  | `list_objects(filter)`             | `GET /objects`                       | `list_objects`           |
| `data get <name>`            | `get_object(name)`                 | `GET /objects/{name}`                | `get_object`             |
| `data delete <name>`         | `delete_object(name)`              | `DELETE /objects/{name}`             | `delete_object`          |
| `data purge`                 | `purge_objects(filter)`            | `POST /objects:purge`                | `purge_objects`          |
| `data query <object>`        | `query_object(request)`            | `POST /viewer/query`                 | `query_object`           |
| `view dashboards list`       | `list_dashboards()`                | `GET /viewer/dashboards`             | `list_dashboards`        |
| `view dashboards get`        | `get_dashboard(name)`              | `GET /viewer/dashboards/{name}`      | `get_dashboard`          |
| `view dashboards save`       | `save_dashboard(dashboard)`        | `PUT /viewer/dashboards/{name}`      | `save_dashboard`         |
| `view dashboards delete`     | `delete_dashboard(name)`           | `DELETE /viewer/dashboards/{name}`   | `delete_dashboard`       |
| `view dashboards run`        | `run_dashboard(name)`              | `POST /viewer/dashboards/{name}:run` | `run_dashboard`          |
| —                            | `run_queries(request)`             | `POST /viewer/runqueries`            | `run_queries`            |
| `task list`                  | `list_tasks(filter)`               | `GET /tasks`                         | `list_tasks`             |
| —                            | `get_task(id)`                     | `GET /tasks/{id}`                    | `get_task`               |
| —                            | `get_task_logs(id, tail, attempt)` | `GET /tasks/{id}/logs`               | `get_task_logs`          |

- `job wait <ref>` and `run-job --progress` have no row: they are CLI-only compositions over `job_stats`. Blocking a request for up to 600s is not a valid server shape, so REST clients poll `GET /jobs/{ref}/stats`.
- The lineage primitives (`internal_api.lineage`) have no CLI verb: MCP exposes them and the calling agent composes them — see `docs/user_guide/lineage.md`.

# CLI Rendering Contract

`aaiclick/__main__.py` holds argparse wiring and two renderers, nothing else:

```python
async def cmd_job_list(args):
    async with orch_context():
        page = await internal_api.list_jobs(_filter_from_args(args))
    if args.json:
        print(page.model_dump_json())
    else:
        _render_jobs_table(page.items)
```

- **Default output**: human tables and one-line summaries, read off the view model — never DB rows — so table columns and JSON fields cannot drift.
- **`--json`**: `print(model.model_dump_json())`, on every command group for symmetry with REST.
- **Exit codes** are owned by `__main__.py`; `internal_api` signals outcomes through return values and exceptions.

# REST Surface

**Implementation**: `aaiclick/server/app.py`

All resource routes mount under one versioned prefix, `API_PREFIX = f"/api/{API_VERSION}"` (`API_VERSION = "v0"` in `aaiclick/view_models.py`), built once in `app.py` and passed to every `include_router(..., prefix=API_PREFIX)`. Routers declare paths **relative** to it (`/jobs`, `/execution-workers`, …), so the version lives in one place and bumping to `/api/v1` is a one-line edit.

**Why `v0`?** View models, error envelope and URL layout are still evolving; `v0` tells downstream UIs and SDK generators "experimental, may break". It graduates to `v1` once the schema settles and external callers exist.

## Contract version

`API_VERSION` is the contract version of every surface, not the package build:
it is the REST prefix, the OpenAPI `info.version`, and the MCP `serverInfo.version`.
A breaking change to a route or a tool schema bumps all three together. Rejected:

- **Package version in `serverInfo`**: it names a build, not a contract, and no
  MCP client (Claude Code, Cursor, VS Code, Codex) displays or acts on the field.
- **`/mcp/v0`**: clients store the full URL, so a path change breaks every
  configured agent; shipped servers (GitHub, Cloudflare, Sentry, Linear, Notion,
  Slack, Supabase) all serve an unversioned `/mcp`.
- **Per-tool versions**: not in the MCP spec. Tool names stay stable; a replaced
  tool is deprecated in its description before removal.

Each router is a thin wrapper running inside a request-scoped `orch_context()` via the `orch_scope` dependency, so the contextvar getters inside `internal_api` see the session/client for the call:

```python
router = APIRouter(prefix="/jobs", tags=["jobs"], dependencies=[Depends(orch_scope)])

@router.get("", response_model=Page[JobView])
async def list_jobs(filter: JobListFilter = Depends()) -> Page[JobView]:
    return await jobs_api.list_jobs(filter)          # → GET /api/v0/jobs
```

**Error mapping** — one exception handler maps each `internal_api.errors` type to a `Problem` with a stable `ProblemCode`.

| Error                        | HTTP |
|------------------------------|------|
| `NotFound`                   | 404  |
| `Conflict`                   | 409  |
| `Invalid`                    | 422  |
| `Unauthorized`               | 401  |
| `Forbidden`                  | 403  |

- **OpenAPI** — derived from the view models; served at `/api/v0/openapi.json` with Swagger UI at `/api/v0/docs`.
- **Logs** — `GET /tasks/{id}/logs` returns a `TaskLogsView` whose `lines` carry `stream` (`stdout`/`stderr`) and `text`, read from the host-independent ClickHouse `task_logs` stream. `?tail=N` keeps the last N lines; `?attempt=N` (1-based, default latest, 404 if out of range) picks the run; `attempts` lists every run with its status.

## Worker lifecycle — CLI only

**Implementation**: `aaiclick/internal_api/execution_workers.py`

REST and MCP only list workers. Start and stop stay on the CLI and the deployment (Compose, Helm replicas):

- **Stop is one-way**: a stopped worker never resumes. In local mode it is the only worker, so a remote stop strands every job in `PENDING` until the server restarts.
- **Start belongs to the deployment**: a server-spawned worker is an untracked process on the API host.
- **No remote workflow needs either**, and the `admin` scope alone would not keep an agent away from them.

## Live updates — `GET /api/v0/events`

**Implementation**: `aaiclick/server/events.py`

A `text/event-stream` of payload-free `changed` events, fed by Postgres `LISTEN`/`NOTIFY` in distributed mode and an in-process bus in local mode. It requires a principal like every resource route, though the signal carries nothing per-user. Design and client behaviour: `docs/designs/frontend.md` — Live updates.

# MCP Surface

**Implementation**: `aaiclick/server/mcp.py`

A module-level `mcp = FastMCP("aaiclick", version=API_VERSION)` (see [Contract version](#contract-version)); each tool wraps one internal API call inside its context:

```python
@mcp.tool(tags={TAG_READ})
async def list_jobs(filter: JobListFilter | None = None) -> Page[JobView]:
    async with orch_context():
        return await jobs_api.list_jobs(filter)
```

The app mounts it at `/mcp` and chains `mcp.http_app()`'s lifespan into its own. FastMCP generates tool schemas from the pydantic types, so inputs and outputs match REST.

**Stack**: the standalone `fastmcp` package (4.x) on the official `mcp` SDK v2. Not the SDK's own `MCPServer` (v1's `mcp.server.fastmcp`): the RBAC gate needs FastMCP's typed `on_list_tools` / `on_call_tool` middleware and tool tags. The server negotiates the newest protocol (`2026-07-28`) and still answers the legacy `initialize` handshake.

**Skill**: the usage guide (every tool family, the lineage triage order) ships as an agent skill in `aaiclick/server/skills/aaiclick/SKILL.md`, served by FastMCP's `SkillProvider` as the resource `skill://aaiclick/SKILL.md`. The server `instructions` stay a two-sentence summary that ends by pointing at that resource, so clients that inline instructions into every prompt pay for two sentences, not the whole method.

**Testing**: FastMCP's in-process client against the same `mcp` instance — no HTTP, no uvicorn. Internal-API errors surface as `fastmcp.exceptions.ToolError`:

```python
async with Client(mcp) as client:
    result = await client.call_tool("list_jobs", {})
    page = Page[JobView].model_validate(result.structured_content)
```

# Running the server

`aaiclick/server/app.py` exposes a module-level `app` — no factory, no wrapper — run directly by uvicorn:

```bash
pip install aaiclick
uvicorn aaiclick.server.app:app
```

In **local mode** (chdb + SQLite) the lifespan starts the `BackgroundWorker` and the execution worker, so a job submitted over REST or MCP runs in the same process; there is no worker process to launch. Both workers and every request share the lifespan's `orch_context` (see `aaiclick/server/deps.py`), so a request nests into it instead of opening its own engine on `local.db`. The CLI wraps this:

```bash
python -m aaiclick local start            # workers + REST + MCP on 127.0.0.1:5255
python -m aaiclick local start --port 9000
python -m aaiclick local start --reload   # auto-restart on code change (dev)
```

In **distributed mode** (PostgreSQL + ClickHouse) the lifespan starts no workers; they run separately:

```bash
uvicorn aaiclick.server.app:app                   # serves REST + MCP
python -m aaiclick execution-worker start         # one or more worker processes
python -m aaiclick background start               # one cleanup process
```

Host, port, workers, reload, TLS etc. are uvicorn's own flags and env vars (`UVICORN_HOST`, `UVICORN_PORT`, …); aaiclick adds no parallel `AAICLICK_SERVER_*` namespace.

# Configuration

The server reuses the CLI's env vars plus the auth knobs in `docs/designs/auth.md` — Configuration:

| Variable              | Purpose                                             | Source                            |
|-----------------------|-----------------------------------------------------|-----------------------------------|
| `AAICLICK_CH_URL`     | ClickHouse connection URL                           | `aaiclick/backend.py`             |
| `AAICLICK_SQL_URL`    | Orchestration SQL backend URL                       | `aaiclick/backend.py`             |
| `AAICLICK_JWT_SECRET` | HS256 signing secret (required in distributed mode) | [Authentication](#authentication) |
| `UVICORN_HOST`        | Bind host                                           | uvicorn                           |
| `UVICORN_PORT`        | Bind port                                           | uvicorn                           |

# Authentication

**Design**: `docs/designs/auth.md`.

Username/password users with one role each (`viewer` / `member` / `admin`), authenticated by a short-lived access JWT + rotating refresh token, or by a scoped long-lived API token. The CLI runs `internal_api` in-process and never crosses this HTTP layer.

- **Gating** is mode-derived, not a flag: open in local mode (synthetic admin + startup `WARNING`), enforced in distributed mode (requires `AAICLICK_JWT_SECRET`, else the server refuses to start).
- **Login**: `POST /api/v0/auth/login` `{username, password[, totp_code]}` → access + refresh tokens; `POST /auth/refresh` rotates, `POST /auth/logout` revokes, `GET /auth/me` returns the principal. MFA and password reset are optional, configuration-driven extensions.
- **Enforcement**: `HTTPBearer` extracts the credential (access JWT or `aaic_` API token); `require_principal` guards every `/api/v0/*` router, then `require_scope(...)` gates on the level a route needs (`require_write` for a member's own mutations, `require_admin` for everything else — jobs, objects, users, audit, worker control). Authorization always compares scopes: a session resolves one through its role, a token carries its own.
- **MCP**: the `/mcp` mount requires any principal via ASGI middleware (`Depends` does not propagate into mounted sub-apps); each tool is gated by its `read` / `write` / `admin` tag (`aaiclick/server/mcp_rbac.py`).
- **Audit**: `aaiclick/server/audit.py` records requests to `audit_log` per `AAICLICK_AUDIT_LOG`.

Open paths (never 401): `GET /health`, `/api/v0/openapi.json`, `/api/v0/docs`, `/api/v0/redoc`, `/api/v0/auth/login|refresh` and `/api/v0/auth/password-reset*`.

Errors use the standard `Problem`: `code="unauthorized"` or `"mfa_required"` → 401 with `WWW-Authenticate: Bearer`; `code="forbidden"` → 403.

# Non-Goals

- **Streaming log envelopes** — `GET /tasks/{id}/logs` returns the captured lines in one `TaskLogsView`; the UI polls it while a task runs. No live per-line push — `docs/designs/frontend.md` (Task logs) says why.
- **WebSockets** — the UI's live channel is server-sent events (see [Live updates](#live-updates--get-apiv0events)).
