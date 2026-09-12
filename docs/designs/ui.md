UI Specification
---

Single-screen, prompt-driven dashboard for aaiclick operators. SPA served by
the FastAPI backend; views refresh when an SSE `changed` signal invalidates the
query cache, falling back to 2 s polling only while that stream is down. Tech
stack, build details, and the live-update chain: `docs/designs/frontend.md`.

**Implementation**: `aaiclick/server/app.py` — see `STATIC_DIR` and the
`StaticFiles` mount (SPA served when `aaiclick/server/static/` exists);
`src/App.tsx` — prompt router; `src/prompt.ts` — `parsePrompt` + URL sync.

# Layout

All modes share a fixed layout:

```
┌──────────────────────────────────────────┐
│ [aaiclick]  [ prompt input.............. ]│  ← fixed header
├──────────────────────────────────────────┤
│                                          │
│   dynamic content area                   │  ← updates based on prompt
│                                          │
└──────────────────────────────────────────┘
```

- **Header**: always visible, contains app logo and editable prompt input
- **Content area**: fills remaining viewport, updates reactively based on prompt value

# Navigation

Clicking interactive elements updates the prompt, which drives what is displayed:

```
(empty prompt)  ──────▶  Help / command reference
@jobs           ──────▶  Jobs list
@job <name>     ──────▶  Job detail (tasks table)
@task <id>      ──────▶  Task detail (status + logs)
@data …         ──────▶  Objects of a scope and their rows
@query …        ──────▶  Query panel over one object
@dashboard …    ──────▶  Saved dashboard in a sandbox
```

```
@jobs  ──click job row──▶  @job <name>  ──click task row──▶  @task <id>
```

# Modes

## Home (empty prompt)

**Prompt**: _(empty)_

Displays a help/command reference showing available commands and their descriptions.

**Implementation**: `src/views/Home.tsx` — see `Home` component.

## Jobs List (`@jobs`)

**Prompt**: `@jobs`

Table of jobs sorted by `created_at` descending. Auto-refreshes on the SSE `changed` signal.

**Implementation**: `src/views/Jobs.tsx` — see `Jobs` component; `aaiclick/server/routers/jobs.py` — see `list_jobs`; `aaiclick/orchestration/view_models.py` — see `JobView` (`total_tasks`, `completed_tasks`).

| Column     | Source field    | Notes                           |
|------------|----------------|---------------------------------|
| Name       | `name`         | Clickable — sets prompt to `@job <name>` |
| Status     | `status`       | Colored badge                   |
| Progress   | computed       | `completed_tasks / total_tasks` |
| Created    | `created_at`   | Relative time (e.g., "2m ago")  |
| Duration   | computed       | `started_at` to `completed_at` or now |

### Status badges

| Status      | Color  |
|-------------|--------|
| `PENDING`   | gray   |
| `RUNNING`   | blue   |
| `COMPLETED` | green  |
| `FAILED`    | red    |
| `CANCELLED` | yellow |

## Job Detail (`@job <name>`)

**Prompt**: `@job <name>`

Header with job info, followed by a table of tasks. Auto-refreshes on the SSE `changed` signal.

**Implementation**: `src/views/JobDetail.tsx` — see `JobDetail` component; `aaiclick/server/routers/jobs.py` — see `get_job`.

**Job header**: name, status badge, created/started/completed times, error (if any).

**Tasks table**:

| Column     | Source field    | Notes                           |
|------------|----------------|---------------------------------|
| Name       | `name`         | Clickable — sets prompt to `@task <id>` |
| Status     | `status`       | Colored badge                   |
| Entrypoint | `entrypoint`   |                                 |
| Attempt    | `attempt`      | `attempt / max_retries`         |
| Started    | `started_at`   | Relative time                   |
| Duration   | computed       | `started_at` to `completed_at` or now |

A Table/Graph toggle switches the body between the tasks table and the
dependency graph. The prompt carries the mode — `@job <name> graph` — so the
view stays shareable as a URL.

Edges are task-level: the server resolves `Group` dependencies onto member
tasks, so the client receives task-to-task edges only. Node colour follows task
status, and an image-build task and its outgoing edges are styled distinctly.

Groups render as nested containers around their members: `"group"` nodes with
a status rolled up server-side from every task beneath them (activity outranks
outcome — see `rollup_status`) and timing spanning the earliest member start
to the latest finish. Empty groups are omitted. Containers are dagre clusters
drawn as React Flow subflows; clicking one does nothing, since only tasks have
a detail view.

**Implementation**: `src/components/graph/JobGraph.tsx` — see `JobGraph`;
`src/components/graph/GroupNode.tsx` — see `GroupNode`;
`aaiclick/orchestration/graph.py` — see `build_graph_edges`, `rollup_status`;
`aaiclick/orchestration/view_models.py` — see `build_job_graph_view`;
`aaiclick/server/routers/jobs.py` — see `job_graph`.

Task statuses use the same color scheme as job statuses, plus:

| Status    | Color  |
|-----------|--------|
| `CLAIMED` | purple |

## Task Detail (`@task <id>`)

**Prompt**: `@task <id>`

**Top section**: status bar with task metadata — name, status badge, entrypoint, job name, worker ID, attempt info, timestamps, error (if any).

**Main section**: log viewer filling the remaining screen with vertical scroll. Logs refresh on the same `changed` signal as every other view. Lines come from the ClickHouse `task_logs` stream for the task's latest run, so they resolve regardless of which host ran the task. Returns `available=false` when the task has not run yet or its latest run captured no output. Lines are colored by `level` (`lvl-*` classes) and an opt-in "Show timestamps" toggle reveals each line's `created_at`.

**Implementation**: `src/views/TaskDetail.tsx` — see `TaskDetail` component; `src/components/LogViewer.tsx` — see `LogViewer`; `aaiclick/server/routers/tasks.py` — see `get_task_logs`; `aaiclick/internal_api/tasks.py` — see `get_task_logs`.

## Data (`@data [job <ref>] [<object>]`)

**Prompt**: `@data`, `@data job <ref>`, `@data [job <ref>] <object>`

Left: a scope tree — Persistent, then the jobs (newest first, name filter).
Right: the objects of the selected scope (name, rows, size, created, a
Query button), or, with an object named, its header and first page of rows.
Rows render through the QueryView kernel's `ResultsTable` and default cell
views, straight from `POST /viewer/query` (see `docs/designs/viewer.md`).

**Implementation**: `src/views/Data.tsx` — see `Data`, `ObjectPreview`;
`src/components/ScopeTree.tsx` — see `ScopeTree`;
`src/components/ObjectsTable.tsx` — see `ObjectsTable`;
`src/queryview-core/results/ResultsTable.tsx`.

## Query (`@query [job <ref>] [<object>]`)

**Prompt**: `@query`, `@query job <ref>`, `@query [job <ref>] <object>`

The scope tree plus an object picker; with an object chosen, the query
panel: a `where` expression, limit / offset paging, the kernel's field and
order-by pickers (fed from `GET /objects/{name}[?job=…]`), static `params:` dropdowns,
the cell-view YAML modal, a saved-query dropdown (Save / Delete), and CSV
download. Saved queries persist through `PUT /viewer/queries/{name}`.

**Implementation**: `src/views/Query.tsx` — see `Query`;
`src/components/QueryPanel.tsx` — see `QueryPanel`;
`src/queryview-core/presentation/FieldPickers.tsx`,
`src/queryview-core/cells/CellViewModal.tsx`; `src/lib/viewer.ts` — see
`orderColsToPairs`, `fieldsFromSchema`.

## Dashboard (`@dashboard [name]`)

**Prompt**: `@dashboard`, `@dashboard <name>`

A dashboard picker, a Refresh button, and the dashboard's HTML in a
sandboxed iframe with the panel results exposed as `window.queries`
(`POST /viewer/dashboards/{name}:run`). Authoring stays with agents and the
CLI (`view dashboards save`).

**Implementation**: `src/views/Dashboard.tsx` — see `Dashboard`;
`src/queryview-core/dashboard/DashboardFrame.tsx`.

## Account & Administration

Prompt-driven like the rest of the UI; flows and the role matrix are in
`docs/designs/auth.md` — SPA.

| Prompt          | View                                          | Implementation                 |
|-----------------|-----------------------------------------------|--------------------------------|
| `@account`      | Change password, MFA setup / disable          | `src/views/Account.tsx`        |
| `@tokens`       | List / create / revoke the caller's API tokens | `src/views/Tokens.tsx`        |
| `@users`        | Superadmin user table                         | `src/views/Users.tsx`          |
| `@audit`        | Superadmin audit-log table with filters       | `src/views/Audit.tsx`          |
| `reset <token>` | New-password form from a reset link (no session) | `src/views/ResetPassword.tsx` |

`src/components/Header.tsx` shows the signed-in username (opens `@account`)
and sign-out; `src/views/Login.tsx` adds the SSO button and the MFA code field.
