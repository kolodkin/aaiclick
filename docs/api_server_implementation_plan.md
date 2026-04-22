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
| Phase 3 | `aaiclick[server]` optional extra                    | Pending | `pyproject.toml`                            |
| Phase 3 | `aaiclick/server/app.py` + routers                   | Pending |                                             |
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
  - Still pending: the equivalents in `aaiclick/data/test_view_models.py` —
    drop each alongside its internal_api migration PR.

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

# Rollout Notes

- Each phase is independent. Phases 3 / 4 depend only on Phase 2 being
  complete for the groups they touch — so a subset of routes can ship
  before every CLI group is migrated.
- No feature flags. Each phase replaces the previous surface outright for
  the groups it covers.
- Pre-commit hooks may reformat migrated files; re-stage only the files
  originally staged (per `CLAUDE.md` commit guidelines).
