API Server: Implementation Plan
---

Companion to `docs/api_server.md`. Five phases, each independently
shippable, each leaves the tree green.

# Status

| Phase   | Component                                            | Status  | Implementation / Notes                      |
|---------|------------------------------------------------------|---------|---------------------------------------------|
| Phase 1 | Shared `aaiclick/view_models.py`                     | ✅      | `aaiclick/view_models.py`                   |
| Phase 1 | `aaiclick/orchestration/view_models.py` + adapters   | ✅      | `aaiclick/orchestration/view_models.py`     |
| Phase 1 | `aaiclick/data/view_models.py` + adapters            | ✅      | `aaiclick/data/view_models.py`              |
| Phase 2 | `aaiclick/internal_api/jobs.py`                      | Pending | Migrate from `__main__.py`                  |
| Phase 2 | `aaiclick/internal_api/registered_jobs.py`           | Pending | Migrate from `__main__.py`                  |
| Phase 2 | `aaiclick/internal_api/workers.py`                   | Pending | Migrate from `__main__.py`                  |
| Phase 2 | `aaiclick/internal_api/tasks.py`                     | Pending | Brand-new `task get`                        |
| Phase 2 | `aaiclick/internal_api/objects.py`                   | Pending | Migrate from `data/object/cli.py`           |
| Phase 2 | `aaiclick/internal_api/setup.py`                     | Pending | Migrate from `__main__.py`                  |
| Phase 2 | `--json` flag on every CLI verb                      | Pending |                                             |
| Phase 3 | `aaiclick[server]` optional extra                    | Pending | `pyproject.toml`                            |
| Phase 3 | `aaiclick/server/app.py` + routers                   | Pending |                                             |
| Phase 4 | `aaiclick/server/mcp.py`                             | Pending |                                             |
| Phase 5 | OpenAPI → UI SDK generator                           | Pending | Consumed by `docs/ui.md` frontend           |

---

# Phase 1 — View Models + Adapters

**Objective**: add the pydantic types and `to_view()` adapters. No caller
changes, no behaviour changes. After Phase 1 the type system exists and is
tested, nothing else is affected.

## Tasks

Fields and model signatures are defined in `api_server.md` "View Model
Catalogue" — do not restate them here.

1. **Create `aaiclick/view_models.py`** — shared models (`Page[T]`,
   `Problem`, `RefId`, `*Request`, `*Filter`).
2. **Create `aaiclick/orchestration/view_models.py`** — orchestration views
   plus `to_view()` adapters. `JobStatsView` rehomes
   `aaiclick/orchestration/jobs/stats.py::JobStats`. Adapters pull in
   SQLModel; view models do not (keeps imports one-directional).
3. **Create `aaiclick/data/view_models.py`** — data views plus `to_view()`
   adapters from `aaiclick.data.object.object.Object`.
4. **Tests** — round-trip each SQLModel / `Object` through `to_view()`
   and assert the dumped shape. Co-located per `CLAUDE.md`
   (`aaiclick/orchestration/test_view_models.py`,
   `aaiclick/data/test_view_models.py`). No decorator-only or
   default-only tests.

## Deliverables

- New modules exist, are imported by tests, and round-trip cleanly.
- No changes to CLI output, no new behaviour elsewhere.
- CI green.

---

# Phase 2 — Migrate CLI Internals to `aaiclick/internal_api/`

**Objective**: relocate business logic out of per-module `cli.py` files and
out of `aaiclick/__main__.py` into a single typed surface
(`aaiclick/internal_api/`). Every function returns a view model from
Phase 1. CLI gains `--json`.

## Migration Pattern (per command group)

1. Create `aaiclick/internal_api/<group>.py`.
2. Move each command's logic from its current home into a function that
   takes an explicit `AsyncSession` or `ChClient` (per the contract in
   `api_server.md`) plus a typed request/filter, and returns a view model.
3. Delete the old `*_cmd` / helper function — **no shim**, per the
   "no backwards-compat hacks" rule in `CLAUDE.md`.
4. Update `aaiclick/__main__.py`: the argparse handler does
   `parse args → call internal_api.<fn> → render`. Two renderers only:
   text table (current behaviour) and `--json` (`model.model_dump_json()`).
5. Move or rename the tests alongside the new module.

## Groups (suggested order)

| Order | Group             | Migrates from                                                | New file                                  |
|-------|-------------------|--------------------------------------------------------------|-------------------------------------------|
| 1     | `jobs`            | helpers in `__main__.py`, `orchestration/jobs/stats.py`      | `aaiclick/internal_api/jobs.py`           |
| 2     | `registered_jobs` | helpers in `__main__.py`, `orchestration/registered_jobs.py` | `aaiclick/internal_api/registered_jobs.py`|
| 3     | `workers`         | helpers in `__main__.py`                                     | `aaiclick/internal_api/workers.py`        |
| 4     | `tasks`           | *(new)* — add `task get <id>` verb                           | `aaiclick/internal_api/tasks.py`          |
| 5     | `objects`         | `aaiclick/data/object/cli.py`                                | `aaiclick/internal_api/objects.py`        |
| 6     | `setup`           | `setup` / `migrate` / ollama bootstrap in `__main__.py`      | `aaiclick/internal_api/setup.py`          |

One PR per group. Each PR:

- Leaves CLI output unchanged for the group's commands.
- Adds `--json` on the group's commands.
- Deletes the old home of the logic.
- Ships tests for the new `internal_api` module.

## Shared Errors

`aaiclick/internal_api/errors.py`:

- `NotFound` — the referenced entity does not exist.
- `Conflict` — state-transition violation (e.g. cancelling a finished job).
- `Invalid` — request/filter validation failed.

CLI handler maps these to exit code + human message. FastAPI maps them to
`Problem` + HTTP status in Phase 3.

## Exit Criteria

- `aaiclick/__main__.py` contains argparse wiring + renderers only. Zero
  business logic.
- No `cli.py` files remain in domain modules.
- Every CLI verb has a `--json` option.
- Every `internal_api` function is tested at the module level (no
  test-via-CLI).

---

# Phase 3 — `aaiclick/server/` FastAPI

**Objective**: HTTP surface over `internal_api`.

## Tasks

1. **Optional dependency extra** — add `aaiclick[server]` in
   `pyproject.toml` pulling in `fastapi`, `uvicorn`, `fastmcp`. Core CLI
   install stays slim.

2. **App factory** — `aaiclick/server/app.py::create_app()` returns a
   configured FastAPI. Registers routers, exception handlers, CORS, and
   lifecycle hooks for context setup/teardown.

3. **Dependency providers** — `aaiclick/server/deps.py::session_dep` /
   `ch_client_dep` yield an `AsyncSession` / `ChClient` per request,
   constructed from env-var config (`AAICLICK_SQL_URL`, `AAICLICK_CH_URL`).

4. **Error mapping** — `aaiclick/server/errors.py` registers handlers that
   turn `internal_api.errors.*` into `Problem` + HTTP status
   (404 / 409 / 422).

5. **Routers** — one file per command group under
   `aaiclick/server/routers/`. Each endpoint is three lines: parse, call
   `internal_api.*`, return. `response_model=` set from view models.

6. **Entrypoint** — `python -m aaiclick.server` launches uvicorn with
   host/port from env vars.

7. **Tests** — one integration test file per router, using
   `httpx.AsyncClient` against the in-process app. No live uvicorn. Assert
   status codes + response shapes against the view models.

## Exit Criteria

- `python -m aaiclick.server` boots and serves every endpoint in the
  CLI verb table.
- `/openapi.json` renders cleanly and reflects every view model.
- Integration tests cover each router.

---

# Phase 4 — FastMCP Mount

**Objective**: AI-agent tool surface over the same `internal_api`.

## Tasks

1. **MCP server** — `aaiclick/server/mcp.py` instantiates a FastMCP server
   and mounts it on the FastAPI app (or hosts standalone via the same
   app factory).
2. **Tools** — one `@mcp.tool()` per `internal_api` function. The function
   signature is the tool schema — no hand-written JSON Schema.
3. **Tests** — call each tool via FastMCP's in-process client; assert the
   response equals the equivalent REST response from Phase 3.

## Exit Criteria

- Every CLI verb is callable as an MCP tool.
- Tool schemas match the REST OpenAPI — both derived from the same view
  models.

---

# Phase 5 — UI SDK Generation

**Objective**: the orchestration UI (`docs/ui.md`) no longer maintains
its own DTOs.

## Tasks

1. Publish `openapi.json` from the FastAPI app.
2. Generate a typed client for the Preact UI directly from the spec
   (e.g. `openapi-typescript`).
3. UI imports the generated types; removes any hand-written DTOs.

## Exit Criteria

- UI SDK regenerates from `/openapi.json` in CI.
- No hand-written response types in the frontend.

---

# Rollout Notes

- Each phase is independent. Phases 3 / 4 / 5 depend only on Phase 2 being
  complete for the groups they touch — so a subset of routes can ship
  before every CLI group is migrated.
- No feature flags. Each phase replaces the previous surface outright for
  the groups it covers.
- Pre-commit hooks may reformat migrated files; re-stage only the files
  originally staged (per `CLAUDE.md` commit guidelines).
