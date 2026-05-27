Operator UI — Implementation Design
---

Turns the `docs/ui/index.html` mockup into a real React SPA, wired to the live
REST API, served by the FastAPI backend. Records the implementation decisions
and deltas from the existing specs — not a restatement of them:

- **UX, modes, wireframes**: `docs/ui.md`
- **Framework, build, data layer, real-time protocol**: `docs/frontend.md`

# Scope

First implementation covers the **full mockup feature set**:

| Area        | Views / actions                                                        |
|-------------|------------------------------------------------------------------------|
| Read-only   | Home, `@jobs`, `@job <name>`, `@task <id>`, `@registered`, `@all`       |
| Write       | run (confirm + params form), cancel, register, enable/disable          |
| Feedback    | toasts, confirm panels, status badges, progress bars                   |

Refresh is **REST polling** (TanStack Query `refetchInterval`). The SSE
`/events` endpoint and server-side fanout described in `docs/frontend.md` are
**deferred** — see [Deferred work](#deferred-work).

# Backend additions

Three small changes; everything else the mockup needs is already served by the
existing routers (`jobs`, `tasks`, `registered-jobs`).

## Task logs endpoint

The `@task` view is a log viewer, but `TaskDetail` exposes only `log_path` (a
filesystem path), not log content. Logs are captured to a `.log` file as raw
stdout/stderr.

- **Route**: `GET /api/v0/tasks/{id}/logs`
- **Response**: `TaskLogsView` — `{ available: bool, log_path: str | null, lines: list[str] }`
- **Behavior**: reads the file at `task.log_path` and returns its lines.
  Returns `available: false` (empty `lines`) when `log_path` is null or the
  file is not present on the server's filesystem.
- **Implementation**: new `get_task_logs(task_id)` in
  `aaiclick/internal_api/tasks.py`; route in `aaiclick/server/routers/tasks.py`;
  `TaskLogsView` in `aaiclick/orchestration/view_models.py`.

!!! warning "Distributed and docker logs are not on the server's filesystem"
    In those modes `log_path` may point at a path the FastAPI process can't
    read. The endpoint degrades to `available: false` rather than erroring;
    real cross-host log access is part of the deferred SSE work.

## Job progress in the list

The `@jobs` table shows a progress column, but `JobView` (the list-item model)
carries no task counts — only `JobDetail` has tasks.

- Add `total_tasks: int = 0` and `completed_tasks: int = 0` to `JobView`.
- `list_jobs` populates them with **one** grouped aggregate over the page's job
  ids (`GROUP BY job_id, status`) — no per-row N+1.
- Other endpoints that emit a `JobView` (`run_job`, `cancel_job`) leave the
  counts at `0`; the list is the only place that fills them.

**Implementation**: `aaiclick/internal_api/jobs.py` — see `list_jobs`;
`aaiclick/orchestration/view_models.py` — see `JobView` and `job_to_view`.

## SPA mount

FastAPI serves the built SPA and falls back to `index.html` for client-side
routes.

- Mount `aaiclick/server/static/` (Vite build output, gitignored).
- Serve `index.html` for any unknown route that is not under `/api` or `/mcp`.
- **Implementation**: `aaiclick/server/app.py` — static mount + SPA fallback,
  registered after the API routers and the MCP mount so they keep priority.

# Frontend

Layout, stack, build commands, and the data-layer contract are defined in
`docs/frontend.md`. This section records only the choices that doc leaves open.

## Prompt is the only state

The prompt string drives the view and **is** the application state. It syncs to
a URL query parameter so refresh, deep-link, and browser back/forward work. A
single `parsePrompt()` function maps the prompt to a view — the React port of
the mockup's `render()` switch:

| Prompt                | View            |
|-----------------------|-----------------|
| _(empty)_             | `Home`          |
| `@jobs`               | `Jobs`          |
| `@registered`         | `Registered`    |
| `@job <name>`         | `JobDetail`     |
| `@task <id>`          | `TaskDetail`    |
| `@all`                | `AllGallery`    |
| `run <name>`          | `RunConfirm`    |
| `run <name> ?`        | `RunForm`       |
| `register [<name>]`   | `RegisterForm`  |
| `cancel <ref>`        | `CancelConfirm` |

No Redux/Zustand. TanStack Query owns server state; component state owns UI
ephemera (input focus, scroll).

## API layer

`src/api/` holds a typed `fetchJSON` client plus one hook per endpoint:

| Hook                                    | Endpoint                                |
|-----------------------------------------|-----------------------------------------|
| `useJobs()`                             | `GET /jobs`                             |
| `useJob(ref)`                           | `GET /jobs/{ref}`                       |
| `useTask(id)`                           | `GET /tasks/{id}`                       |
| `useTaskLogs(id)`                       | `GET /tasks/{id}/logs`                  |
| `useRegisteredJobs()`                   | `GET /registered-jobs`                  |
| `useRunJob()`                           | `POST /jobs:run`                        |
| `useCancelJob()`                        | `POST /jobs/{ref}/cancel`               |
| `useRegisterJob()`                      | `POST /registered-jobs`                 |
| `useEnableJob()` / `useDisableJob()`    | `POST /registered-jobs/{name}/enable`/`disable` |

Live views set `refetchInterval` (~2 s). Mutations invalidate the relevant
query keys on success and surface a toast.

## Components and theme

- **Views** (`src/views/`): `Home`, `Jobs`, `JobDetail`, `TaskDetail`,
  `Registered`, `RunConfirm`, `RunForm`, `RegisterForm`, `CancelConfirm`,
  `AllGallery`.
- **Shared** (`src/components/`): `Header` (logo + prompt), `StatusBadge`,
  `JobsTable`, `TasksTable`, `ProgressBar`, `EnabledToggle`, `LogViewer`,
  `MetaGrid`, `Panel`, `Chips`, `Toast`.
- **Theme**: the mockup's liquid-glass CSS custom properties (backgrounds,
  panel/border tokens, status colors) port into a Tailwind theme + base layer
  so the rendered result matches the mockup 1:1.

Relative times ("2m ago") and durations are computed client-side from the ISO
timestamps the API returns.

# Testing

| Layer                | Tool                | Where / gate                              |
|----------------------|---------------------|-------------------------------------------|
| Static type check    | `tsc --noEmit`      | `npm run check`, CI gate                  |
| Backend plumbing     | pytest + httpx      | `routers/test_tasks.py`, `test_jobs.py`   |
| End-to-end (browser) | Playwright (Python) | `test_e2e/web/`, golden-path smoke        |

- Backend tests cover the new `/tasks/{id}/logs` route and the `JobView` count
  fields, asserting HTTP plumbing and response-model round-trips only (per
  `aaiclick/server/CLAUDE.md` — business logic is tested in `internal_api`).
- One Playwright smoke test drives the golden path `@jobs → @job → @task`
  against the built SPA + live server.
- Python tooling (`ruff`, `pyright`) excludes `src/` and `node_modules/`.

# Deferred work

Tracked in `docs/future.md`:

- SSE `GET /api/v0/events` + in-process bus + Postgres `LISTEN/NOTIFY` /
  SQLite-poll feeders (real-time updates replacing REST polling).
- Cross-host / docker task-log access.
- Vitest + React Testing Library unit tests.
- OpenAPI-generated client types (hand-written for v0).
- Auth for the SSE endpoint and the REST surface.
- Bundling `aaiclick/server/static/` into release wheels.
