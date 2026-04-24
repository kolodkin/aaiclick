API Server: Implementation Plan
---

Companion to `docs/api_server.md`. Four phases, each independently
shippable, each leaves the tree green.

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
| Phase 4 | `aaiclick/server/mcp.py`                             | Pending |                                             |

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

## Phase 1 follow-up (tracked for Phase 2)

- Delete the trivial passthrough "round-trip dump" tests once the Phase 2
  `internal_api` tests exercise each adapter end-to-end. They exist to
  satisfy the Phase 1 "assert the dumped shape" deliverable but are
  "assert Python assignment works" per `CLAUDE.md`.
  - `test_registered_job_to_view_round_trip` — removed (exercised by
    `aaiclick/internal_api/test_registered_jobs.py`).
  - `test_worker_to_view_round_trip` — removed (exercised by
    `aaiclick/internal_api/test_workers.py`).
  - `test_job_to_view_round_trip` — removed (exercised by
    `aaiclick/internal_api/test_jobs.py`).
  - `test_task_to_detail_includes_detail_fields` — removed (exercised by
    `aaiclick/internal_api/test_tasks.py`).
  - `test_column_info_to_view_*`, `test_schema_to_view_preserves_columns_and_metadata`,
    `test_object_to_view_*`, `test_object_to_detail_embeds_schema` — removed
    (adapters exercised by `aaiclick/internal_api/test_objects.py`;
    `scope_of` exercised by `aaiclick/data/test_scope.py`). The three
    `_object_name_from_table` parsing tests stay — that helper lives in
    `view_models.py` and has real branching logic.

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

## Phase 2 follow-up (cross-group cleanups)

Small refinements surfaced during migration PRs. Each is a separate PR, sized
to land alongside or just after the group that motivates it.

- **Paginator footer parity** — `cli_renderers.render_registered_jobs_page`
  lacks the `"Showing N-M of total"` footer that `render_jobs_page` emits.
  Thread `offset` through the CLI handler and add the footer. Apply the same
  check to every group that introduces a list view (workers, objects, tasks).

- **Apply typed-exception pattern across `internal_api/*`** — the
  registered-jobs migration moved error translation to typed subclasses of
  `ValueError` in the producer (`RegisteredJobAlreadyExists`,
  `RegisteredJobNotFound`). `internal_api/jobs.py` still pre-resolves via
  `_resolve_job` to avoid string matching. Unify on the typed-exception
  approach in `orchestration/execution/claiming.py` (`cancel_job`) and in
  any new producer that can fail with "not found" / "conflict" semantics.

- **Shared pagination helper** — when the third `list_*` call site lands
  (e.g. `list_workers`), extract a helper that takes a base `select`, a
  sequence of `WHERE` predicates, an ORDER BY column, and
  `limit`/`offset` — and returns `(total, rows)`. Premature with only two
  sites (per `CLAUDE.md`'s "three similar lines is better than a premature
  abstraction"); revisit at three.

---

# Phase 3 — `aaiclick/server/` FastAPI

**Objective**: HTTP surface over `internal_api`. Every CLI verb reachable
over HTTP under the **`/api/v0`** prefix with no new business logic and no
drift from the view models.

## Versioning decision — `/api/v0`

The REST surface mounts at **`/api/v0`**, not `/api/v1`. Rationale:

- The view-model schemas, error envelope, and URL shapes are still
  being tuned alongside Phase 3. `v0` signals "experimental, may break"
  to UI/SDK consumers.
- Bump to `/api/v1` once a downstream (the orchestration UI, external
  MCP client, or published SDK) commits to the contract.

The prefix is declared **once** in `aaiclick/server/app.py` as
`API_PREFIX = "/api/v0"` and threaded through every
`app.include_router(..., prefix=API_PREFIX)` call. Routers themselves
declare paths *relative* to the prefix (`/jobs`, `/workers`, ...) — the
version segment never appears in individual router files. This isolates
the "graduate to v1" change to a single line.

## Ordering

Each sub-task below is one PR. They can mostly run in parallel after
**PR 1–3** land (the scaffolding). Router PRs (5a–5e) are independent.

## Tasks

### PR 1 — `aaiclick[server]` optional extra

- Add `[project.optional-dependencies] server` to `pyproject.toml` with
  `fastapi>=0.115`, `uvicorn[standard]>=0.30`, `httpx>=0.27` (needed by
  tests). `fastmcp` arrives in Phase 4, not here — keep the Phase 3 PR
  focused on HTTP.
- Extend `all = ["aaiclick[distributed,ai,server]"]`.
- No new runtime imports of `fastapi` anywhere outside `aaiclick/server/`
  so the core CLI install stays slim.

### PR 2 — Module-level app + `/api/v0` prefix wiring

`aaiclick/server/app.py`:

- Module-level constant `API_PREFIX = "/api/v0"`.
- Module-level `app = FastAPI(...)` — **no factory**. Configuration
  does not depend on runtime arguments, and a module-level instance
  pairs naturally with `uvicorn aaiclick.server.app:app` (no `--factory`
  flag) and with a future process-wide `lifespan` that owns the
  engine / ch_client.
  - `docs_url=f"{API_PREFIX}/docs"`,
    `redoc_url=f"{API_PREFIX}/redoc"`,
    `openapi_url=f"{API_PREFIX}/openapi.json"` — OpenAPI lives under
    the versioned prefix too, so `v0` / `v1` specs can co-exist if we
    ever need to dual-publish.
  - Routers mount via `app.include_router(<group>.router, prefix=API_PREFIX)`.
  - Exception handlers from PR 4 register against the module-level `app`.
  - Liveness endpoint: `GET /health` (unversioned — for k8s / uptime
    probes; not part of the API contract).
- No CORS in v0 — the orchestration UI is same-origin. Add when a cross-
  origin consumer appears.

### PR 3 — Scope dependencies

`aaiclick/server/deps.py`:

- `orch_scope` — an `async` FastAPI dependency that wraps each request
  in `orch_context(with_ch=False)`. Read-only SQL routes (`jobs`,
  `registered_jobs`, `tasks`, `workers`) use this.
- `orch_scope_with_ch` — same, but `with_ch=True`. Required only by the
  `run-job` endpoint (task execution touches ClickHouse).
- `data_scope` — wraps requests in `data_context()` for `objects`
  routes.
- No per-request session/client factories: the `internal_api` functions
  already read resources through contextvar getters, so the dep only
  manages context entry/exit.

### PR 4 — Error mapping

`aaiclick/server/errors.py`:

- `problem_from(exc, status)` — build `Problem` from an
  `InternalApiError`, populating `title`, `status`, `detail`, and a
  short `code` (`"not_found"`, `"conflict"`, `"invalid"`).
- `register_exception_handlers(app)` — registers three handlers:
  - `NotFound` → 404
  - `Conflict` → 409
  - `Invalid` → 422
- Unhandled `InternalApiError` falls through to FastAPI's default 500 —
  we do **not** add a blanket handler, so bugs surface.

### PR 5a–5e — One router per command group

File: `aaiclick/server/routers/<group>.py`. Each file defines
`router = APIRouter(prefix="/<group>", tags=["<group>"])`. Paths are
relative — the `/api/v0` prefix comes from `include_router`. Each
endpoint body is parse → call `internal_api.*` → return.

- **5a `jobs.py`** — wraps `list_jobs`, `get_job`, `job_stats`,
  `cancel_job`, `run_job`. Uses `orch_scope` for reads, `orch_scope_with_ch`
  for `run_job`.
- **5b `registered_jobs.py`** — wraps `list_registered_jobs`,
  `register_job`, `enable_job`, `disable_job`.
- **5c `tasks.py`** — wraps `get_task` (read-only today; extend when
  the task-level verbs grow).
- **5d `workers.py`** — wraps `list_workers`, `stop_worker`. `start_worker`
  is deliberately omitted in v0 — spawning a worker from an HTTP request
  needs auth + lifecycle we have not designed yet. Tracked in
  `docs/future.md`.
- **5e `objects.py`** — wraps `list_objects`, `get_object`,
  `delete_object`, `purge_objects`. Uses `data_scope`.

Path conventions (match the REST column in `api_server.md`):

- Collection: `GET /<group>`, `POST /<group>` (where applicable).
- Item: `GET /<group>/{ref}`, `DELETE /<group>/{ref}`.
- Sub-resource: `GET /<group>/{ref}/stats`,
  `POST /<group>/{ref}/cancel`.
- Verb-actions that do not fit REST nouns use the `:verb` suffix:
  `POST /jobs:run`, `POST /objects:purge`.

Each router PR:

- Adds an integration test file (`aaiclick/server/routers/test_<group>.py`)
  using `httpx.AsyncClient` + `ASGITransport` against the in-process app
  — no live uvicorn.
- Asserts status codes **and** that the response body round-trips
  through the view model (`JobView.model_validate(resp.json())`).
- Covers happy path + each `internal_api` error path (404 / 409 / 422).

### PR 6 — Run via uvicorn directly — no wrapper module

No `__main__.py`; no factory. See `docs/api_server.md` — Running the
server — for the canonical invocation.

## Test Strategy

- **In-process only**: `httpx.AsyncClient(transport=ASGITransport(app))`.
  No uvicorn in the test suite — keeps CI hermetic.
- **Shared fixture**: one `conftest.py` fixture builds the app once per
  test module and wires it to the chdb + SQLite default backend (same
  fixtures the `internal_api` tests already use).
- **No CLI re-tests**: the `internal_api` tests already cover business
  logic. Router tests assert only HTTP plumbing — status codes, route
  registration under `/api/v0`, error envelope shape, and that the JSON
  deserialises into the declared `response_model`.
- **OpenAPI smoke test**: one test fetches `/api/v0/openapi.json` and
  asserts every view model from `api_server.md` appears under
  `components.schemas`.

## Exit Criteria

- `uvicorn aaiclick.server.app:app` boots and serves every route in the
  REST column of the CLI-verb table under `/api/v0`.
- `GET /api/v0/openapi.json` lists every view model and every route.
- Every `internal_api` error path has a router test asserting the
  correct HTTP status + `Problem` body.
- `pip install aaiclick` without the `[server]` extra still imports
  cleanly (no `fastapi` at module load time in the core package).

---

# Phase 4 — FastMCP Mount

**Objective**: AI-agent tool surface over the same `internal_api`.

## Tasks

1. **MCP server** — `aaiclick/server/mcp.py` instantiates a FastMCP server
   and mounts it on the FastAPI `app` (or hosts standalone by importing
   the same module-level `app`).
2. **Tools** — one `@mcp.tool()` per `internal_api` function. The function
   signature is the tool schema — no hand-written JSON Schema.
3. **Tests** — call each tool via FastMCP's in-process client; assert the
   response equals the equivalent REST response from Phase 3.

## Exit Criteria

- Every CLI verb is callable as an MCP tool.
- Tool schemas match the REST OpenAPI — both derived from the same view
  models.

---

# Rollout Notes

- Each phase is independent. Phases 3 / 4 depend only on Phase 2 being
  complete for the groups they touch — so a subset of routes can ship
  before every CLI group is migrated.
- No feature flags. Each phase replaces the previous surface outright for
  the groups it covers.
- Pre-commit hooks may reformat migrated files; re-stage only the files
  originally staged (per `CLAUDE.md` commit guidelines).
