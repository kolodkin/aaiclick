API Server: Implementation Plan
---

Companion to `docs/api_server.md`. Phases 1–4 are done — their per-task
narratives have been removed as completed planning artifacts. The
status table below preserves the implementation map, and **Phase 5 —
Authentication + `start_worker`** remains the only outstanding work.

# Status

| Phase   | Component                                            | Status  | Implementation / Notes                      |
|---------|------------------------------------------------------|---------|---------------------------------------------|
| Phase 1 | Shared `aaiclick/view_models.py`                     | ✅      | `aaiclick/view_models.py`                   |
| Phase 1 | `aaiclick/orchestration/view_models.py` + adapters   | ✅      | `aaiclick/orchestration/view_models.py`     |
| Phase 1 | `aaiclick/data/view_models.py` + adapters            | ✅      | `aaiclick/data/view_models.py`              |
| Phase 2 | `aaiclick/internal_api/errors.py`                    | ✅      | `aaiclick/internal_api/errors.py`           |
| Phase 2 | `aaiclick/internal_api/jobs.py`                      | ✅      | `aaiclick/internal_api/jobs.py`             |
| Phase 2 | `--json` flag on the `job` + `run-job` verbs         | ✅      | `aaiclick/__main__.py` + `cli_renderers.py` |
| Phase 2 | `aaiclick/internal_api/registered_jobs.py`           | ✅      | `aaiclick/internal_api/registered_jobs.py`  |
| Phase 2 | `aaiclick/internal_api/workers.py`                   | ✅      | `aaiclick/internal_api/workers.py`          |
| Phase 2 | `aaiclick/internal_api/tasks.py`                     | ✅      | `aaiclick/internal_api/tasks.py`            |
| Phase 2 | `aaiclick/internal_api/objects.py`                   | ✅      | `aaiclick/internal_api/objects.py`          |
| Phase 2 | `aaiclick/internal_api/setup.py`                     | ✅      | `aaiclick/internal_api/setup.py`            |
| Phase 2 | `--json` flag on remaining CLI verbs                 | ✅      | Data `list`/`get`/`delete`/`purge`, plus `setup`/`migrate` now support `--json` |
| Phase 3 | `aaiclick[server]` optional extra                    | ✅      | `pyproject.toml`                            |
| Phase 3 | `aaiclick/server/app.py` + `/api/v0` prefix wiring   | ✅      | `aaiclick/server/app.py`                    |
| Phase 3 | `aaiclick/server/deps.py` (orch scope deps)          | ✅      | `aaiclick/server/deps.py`                   |
| Phase 3 | `aaiclick/server/errors.py` (internal_api → Problem) | ✅      | `aaiclick/server/errors.py`                 |
| Phase 3 | `aaiclick/server/routers/jobs.py`                    | ✅      | `aaiclick/server/routers/jobs.py`           |
| Phase 3 | `aaiclick/server/routers/registered_jobs.py`         | ✅      | `aaiclick/server/routers/registered_jobs.py`|
| Phase 3 | `aaiclick/server/routers/tasks.py`                   | ✅      | `aaiclick/server/routers/tasks.py`          |
| Phase 3 | `aaiclick/server/routers/workers.py`                 | ✅      | `aaiclick/server/routers/workers.py`        |
| Phase 3 | `aaiclick/server/routers/objects.py`                 | ✅      | `aaiclick/server/routers/objects.py`        |
| Phase 3 | `uvicorn aaiclick.server.app:app` invocation         | ✅      | Module-level `app` in `aaiclick/server/app.py` — no wrapper entrypoint |
| Phase 4 | `aaiclick[server]` extra includes `fastmcp`          | ✅      | `pyproject.toml`                            |
| Phase 4 | `aaiclick/server/mcp.py`                             | ✅      | `aaiclick/server/mcp.py`                    |
| Phase 4 | FastMCP mounted on FastAPI at `/mcp`                 | ✅      | `aaiclick/server/app.py` — `_mcp_app = mcp.http_app(path="/")` + `app.mount("/mcp", _mcp_app)`; the FastAPI `lifespan` chains FastMCP's startup with `local_runtime()` (workers in local mode) |
| Phase 4 | In-process `Client(mcp)` tool tests                  | ✅      | `aaiclick/server/test_mcp.py`               |
| Phase 5 | `Unauthorized` / `Forbidden` in `internal_api/errors`| ⚠️      | New subclasses + `ProblemCode` entries      |
| Phase 5 | `aaiclick/server/auth.py` — bearer-token dependency  | ⚠️      | New module                                  |
| Phase 5 | ASGI middleware protecting `/mcp` mount              | ⚠️      | `aaiclick/server/app.py`                    |
| Phase 5 | `AAICLICK_API_TOKEN` env var wiring + startup log    | ⚠️      | `aaiclick/server/app.py`                    |
| Phase 5 | `StartWorkerRequest` shared view model               | ⚠️      | `aaiclick/view_models.py`                   |
| Phase 5 | `internal_api.workers.start_worker(request)`         | ⚠️      | `aaiclick/internal_api/workers.py`          |
| Phase 5 | `POST /api/v0/workers` router wiring (202 Accepted)  | ⚠️      | `aaiclick/server/routers/workers.py`        |
| Phase 5 | `start_worker` MCP tool                              | ⚠️      | `aaiclick/server/mcp.py`                    |

---

# Phase 5 — Authentication + `start_worker`

**Objective**: close the two deferrals in Phase 3.

1. Gate the REST + MCP surfaces behind a shared bearer token so the
   server can be reached from a non-localhost UI without exposing every
   verb to the internet.
2. Ship `POST /api/v0/workers` — the only CLI verb that did not graduate
   to HTTP in Phase 3 because "spawn a worker from a request" needs a
   lifecycle design.

Full contract lives in `docs/api_server.md` — sections **Authentication**
and **Spawning workers — `POST /api/v0/workers`**. This plan breaks the
work into independently shippable PRs.

## Ordering

The two tracks are independent and can run in parallel. Inside a track,
PRs are sequential.

### Track A — Authentication

1. **PR A1 — `Unauthorized` / `Forbidden` errors** — add the subclasses
   to `aaiclick/internal_api/errors.py` and extend `ProblemCode` in
   `aaiclick/view_models.py` with `UNAUTHORIZED`, `FORBIDDEN`,
   `WORKER_SPAWN_FAILED`. Update `_PROBLEM_MAP` in
   `aaiclick/server/errors.py`.

2. **PR A2 — `aaiclick/server/auth.py`** — introduce the
   `require_bearer` FastAPI dependency:
   - Reads `AAICLICK_API_TOKEN` via a module-level helper that calls
     `os.environ.get` each request (monkeypatchable from tests).
   - Uses `hmac.compare_digest`.
   - Sets `WWW-Authenticate: Bearer` on 401 responses via a custom
     exception handler registered alongside the existing `Problem`
     handlers.
   - No router wiring in this PR — the dependency exists and is unit
     tested (`aaiclick/server/test_auth.py`) but is not yet mounted.

3. **PR A3 — Wire `require_bearer` on every router + middleware on
   `/mcp`** — attach `dependencies=[Depends(require_bearer)]` once per
   `include_router` call in `aaiclick/server/app.py`. Add an ASGI
   middleware that runs the same check before delegating to the
   mounted FastMCP sub-app (`Depends()` does not cross mount
   boundaries). Startup log: `WARNING` when the env var is unset.
   - `/health` stays outside `/api/v0` — no change.
   - `/api/v0/openapi.json`, `/api/v0/docs`, `/api/v0/redoc` remain
     open. FastAPI serves them at the app level, so a router-level
     `dependencies=` does **not** cover them. Gating the schema would
     need a middleware or `openapi_url=None` + a hand-written authed
     route — deferred to the DB-backed-tokens phase.

4. **PR A4 — Integration tests** — one sweep across the existing
   router test files (`aaiclick/server/routers/test_*.py`) asserting:
   - Token unset → existing tests pass unchanged (open-server mode).
   - Token set + no header → 401 with `code="unauthorized"`.
   - Token set + wrong token → 401.
   - Token set + correct token → existing happy paths still pass.
   - `/health` never 401s regardless of env var state.
   - `/mcp/*` HTTP endpoint enforces the same check (in-process MCP
     client tests continue to bypass it — they never hit HTTP).

### Track B — `start_worker`

1. **PR B1 — `StartWorkerRequest` + `internal_api.workers.start_worker`** —
   add `StartWorkerRequest(max_tasks: int | None = None)` to
   `aaiclick/view_models.py`. Implement
   `internal_api.workers.start_worker(request) -> None`:
   - Raise `Invalid` if `is_local()`.
   - Spawn `python -m aaiclick worker start [--max-tasks N]` via
     `asyncio.create_subprocess_exec` with `start_new_session=True`
     (POSIX only).
   - Catch `FileNotFoundError` / `PermissionError` from exec and raise
     `Conflict(code=WORKER_SPAWN_FAILED)`. Return `None` otherwise — no
     polling, no correlation id, no DB reads.
   - Unit tests monkey-patch `asyncio.create_subprocess_exec` with a
     fake that records the argv and env, then verify `is_local()`
     short-circuits and the exec path builds the expected command line.

2. **PR B2 — Router + MCP tool** — add `POST /workers` to
   `aaiclick/server/routers/workers.py` returning
   `Response(status_code=202, headers={"Location": "/api/v0/workers"})`
   (relative path; auth inherited). Add `start_worker` tool to
   `aaiclick/server/mcp.py` under the standard
   `async with orch_context(with_ch=False)` wrapper; the tool returns
   `None` on success. Router test asserts: 202 on happy path (with
   `Location` header), 422 in local mode, 503 with
   `code="worker_spawn_failed"` when exec raises. MCP tool test
   asserts the call completes without raising via in-process
   `Client(mcp)`.

## Exit Criteria

- `AAICLICK_API_TOKEN` set → every `/api/v0/*` and `/mcp/*` request
  without a matching bearer token returns `401 Problem` with
  `WWW-Authenticate: Bearer`. `/health` remains open.
- `AAICLICK_API_TOKEN` unset → server behaviour is identical to
  today's open server, with a single `WARNING` log line at startup.
- `POST /api/v0/workers` in distributed mode returns `202 Accepted`
  with a `Location: /api/v0/workers` header; the spawned process
  survives the HTTP handler (verified by asserting `proc.pid` is still
  alive after the response). Raises `422 Invalid` in local mode and
  `503 worker_spawn_failed` when exec raises.
- `docs/api_server.md` `CLI verb → internal_api → REST → MCP` table
  reflects `start_worker` as implemented (drop any "⚠️ deferred"
  marker once this phase lands).

## Non-Goals (Phase 5)

- **DB-backed tokens with scopes** — tracked in `docs/future.md`.
  Phase 5 ships a single static token; scopes arrive with the DB-
  backed token store.
- **Process supervision for HTTP-spawned workers** — parity with
  CLI-spawned workers only. The server does not track child PIDs,
  does not restart crashed children, and does not enforce concurrency
  caps on `POST /workers`. A supervision layer is a separate future
  doc when it's actually needed.
- **OAuth / OIDC** — Phase 5 is bearer tokens only. Browser-flow auth
  arrives with the orchestration UI.

---

# Rollout Notes

- No feature flags. Phase 5 replaces the open-server posture for the
  groups it covers; auth is opt-in via the env var.
- Pre-commit hooks may reformat migrated files; re-stage only the
  files originally staged (per `CLAUDE.md` commit guidelines).
