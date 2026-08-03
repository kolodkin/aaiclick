UI Specification
---

Single-screen, prompt-driven dashboard for aaiclick operators. SPA served by
the FastAPI backend with 2 s REST polling (v0); SSE is deferred to
`docs/designs/future.md`. Tech stack and build details:
`docs/designs/frontend.md`.

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

Table of jobs sorted by `created_at` descending. Auto-refreshes via REST polling (2 s).

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

Header with job info, followed by a table of tasks. Auto-refreshes via REST polling (2 s).

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

Nodes are task-level: the server resolves `Group` dependencies onto member
tasks, so the client receives plain task nodes and task-to-task edges. Node
colour follows task status, and an image-build task and its outgoing edges are
styled distinctly.

**Implementation**: `src/components/graph/JobGraph.tsx` — see `JobGraph`;
`aaiclick/orchestration/graph.py` — see `build_graph_edges`;
`aaiclick/server/routers/jobs.py` — see `job_graph`.

Task statuses use the same color scheme as job statuses, plus:

| Status    | Color  |
|-----------|--------|
| `CLAIMED` | purple |

## Task Detail (`@task <id>`)

**Prompt**: `@task <id>`

**Top section**: status bar with task metadata — name, status badge, entrypoint, job name, worker ID, attempt info, timestamps, error (if any).

**Main section**: log viewer filling the remaining screen with vertical scroll. Logs poll every 2 s in v0; real-time SSE is deferred. Lines come from the ClickHouse `task_logs` stream for the task's latest run, so they resolve regardless of which host ran the task. Returns `available=false` when the task has not run yet or its latest run captured no output. Lines are colored by `level` (`lvl-*` classes) and an opt-in "Show timestamps" toggle reveals each line's `created_at`.

**Implementation**: `src/views/TaskDetail.tsx` — see `TaskDetail` component; `src/components/LogViewer.tsx` — see `LogViewer`; `aaiclick/server/routers/tasks.py` — see `get_task_logs`; `aaiclick/internal_api/tasks.py` — see `get_task_logs`.
