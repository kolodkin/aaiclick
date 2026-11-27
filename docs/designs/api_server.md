API Server: Shared Pydantic I/O Layer
---

Process layout and the wire contracts (view models, internal API, REST, MCP): `docs/architecture/topology.md`, `docs/architecture/contracts.md`.

One set of **input/output schemas** and one **internal API** power three surfaces: the **CLI** (`python -m aaiclick ...`), the **REST API** (FastAPI, backs the UI) and **MCP** (FastMCP, the AI agent tool surface).

# Motivation

Each surface is a thin renderer over one internal API typed with pydantic view models, so:

- Each command is written **once**, in `aaiclick/internal_api/`, returning a typed view model.
- It renders as CLI text, CLI `--json`, HTTP JSON or an MCP tool result without duplicated logic; the CLI gets `--json` for free.
- The OpenAPI spec, docs and client SDKs come straight from the pydantic models, so REST and MCP schemas cannot drift from the CLI.
- Database models (`SQLModel`) and wire models (`pydantic`) stay independent, so DB schema changes don't ripple into REST clients.

# Package Layout

| Path                                    | Holds                                                                          |
|-----------------------------------------|--------------------------------------------------------------------------------|
| `aaiclick/view_models.py`               | Cross-domain wire models: `Page[T]`, `Problem`, `RefId`, `*Request`, `*Filter` |
| `aaiclick/orchestration/view_models.py` | Orchestration views + `to_view()` adapters (SQLModel → View)                   |
| `aaiclick/data/view_models.py`          | Data (Object) views + adapters                                                 |
| `aaiclick/internal_api/`                | Business logic, one module per resource; `errors.py` holds the error types     |
| `aaiclick/__main__.py`                  | argparse + text/JSON renderers only, zero business logic                       |
| `aaiclick/server/`                      | FastAPI app, `routers/`, `mcp.py`, error mapper (optional `server` extra)      |

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

<!-- req: cli-rendering-contract -->
- **Default output**: human tables and one-line summaries, read off the view model — never DB rows — so table columns and JSON fields cannot drift.
- **`--json`**: `print(model.model_dump_json())`, on every command group for symmetry with REST.
- **Exit codes** are owned by `__main__.py`; `internal_api` signals outcomes through return values and exceptions.

# Running the server

`aaiclick/server/app.py` exposes a module-level `app` — no factory, no wrapper — run directly by uvicorn:

```bash
pip install aaiclick
uvicorn aaiclick.server.app:app
```

<!-- req: local-lifespan-workers -->
In **local mode** (chdb + SQLite) the lifespan starts the `BackgroundWorker` and the execution worker, so a job submitted over REST or MCP runs in the same process; there is no worker process to launch. Both workers and every request share the lifespan's `orch_context` (see `aaiclick/server/deps.py`), so a request nests into it instead of opening its own engine on `local.db`. In **distributed mode** (PostgreSQL + ClickHouse) the lifespan starts no workers; they run as separate processes. The commands for both modes are in the user guide: [Deployment](../user_guide/deployment.md) and [Managing Jobs — Workers](../user_guide/orchestration/managing_jobs.md#workers).

Host, port, workers, reload, TLS etc. are uvicorn's own flags and env vars (`UVICORN_HOST`, `UVICORN_PORT`, …); aaiclick adds no parallel `AAICLICK_SERVER_*` namespace.

# Configuration

The server reads the CLI's env vars ([Deployment](../user_guide/deployment.md)) plus the auth knobs in `docs/designs/auth.md` — Configuration. Host, port and TLS are uvicorn's own.

# Authentication

**Design**: `docs/designs/auth.md`.

Username/password users with one role each (`viewer` / `member` / `admin`), authenticated by a short-lived access JWT + rotating refresh token, or by a scoped long-lived API token. The CLI runs `internal_api` in-process and never crosses this HTTP layer.

- **Gating** is mode-derived, not a flag: open in local mode (synthetic admin + startup `WARNING`), enforced in distributed mode (requires `AAICLICK_JWT_SECRET`, else the server refuses to start).
- **Login**: `POST /api/v0/auth/login` `{username, password[, totp_code]}` → access + refresh tokens; `POST /auth/refresh` rotates, `POST /auth/logout` revokes, `GET /auth/me` returns the principal. MFA and password reset are optional, configuration-driven extensions.
<!-- req: route-guards -->
- **Enforcement**: `HTTPBearer` extracts the credential (access JWT or `aaic_` API token); `require_principal` guards every `/api/v0/*` router, then `require_scope(...)` gates on the level a route needs (`require_write` for a member's own mutations, `require_admin` for everything else — jobs, objects, users, audit, worker control). Authorization always compares scopes: a session resolves one through its role, a token carries its own.
- **MCP**: the `/mcp` mount requires any principal via ASGI middleware (`Depends` does not propagate into mounted sub-apps); each tool is gated by its `read` / `write` / `admin` tag (`aaiclick/server/mcp_rbac.py`).
- **Audit**: `aaiclick/server/audit.py` records requests to `audit_log` per `AAICLICK_AUDIT_LOG`.

<!-- req: open-paths -->
Open paths (never 401): `GET /health`, `/api/v0/openapi.json`, `/api/v0/docs`, `/api/v0/redoc`, `/api/v0/auth/login|refresh` and `/api/v0/auth/password-reset*`.

<!-- req: auth-problem-errors -->
Errors use the standard `Problem`: `code="unauthorized"` or `"mfa_required"` → 401 with `WWW-Authenticate: Bearer`; `code="forbidden"` → 403.

# Non-Goals

- **Streaming log envelopes** — `GET /tasks/{id}/logs` returns the captured lines in one `TaskLogsView`; the UI polls it while a task runs. No live per-line push — `docs/designs/frontend.md` (Task logs) says why.
- **WebSockets** — the UI's live channel is server-sent events (see [Live updates](../architecture/contracts.md#live-updates-get-apiv0events)).
