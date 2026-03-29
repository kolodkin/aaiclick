UI Specification
---

Single-screen, prompt-driven dashboard for aaiclick operators. Built with **Preact + TailwindCSS** (SPA) and a **FastAPI** backend with WebSocket for real-time updates.

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

- **Header**: always visible, contains app logo and prompt input
- **Content area**: fills remaining viewport, updates reactively based on prompt value
- Prompt is always editable — user can type directly or click interactive elements to update it

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

**Wireframe**: `docs/ui/home.excalidraw.svg`

## Jobs List (`@jobs`)

**Prompt**: `@jobs`

Table of jobs sorted by `created_at` descending. Auto-refreshes via WebSocket.

| Column     | Source field    | Notes                           |
|------------|----------------|---------------------------------|
| Name       | `name`         | Clickable — sets prompt to `@job <name>` |
| Status     | `status`       | Colored badge                   |
| Progress   | computed       | `completed_tasks / total_tasks` |
| Created    | `created_at`   | Relative time (e.g., "2m ago")  |
| Duration   | computed       | `started_at` to `completed_at` or now |

**Wireframe**: `docs/ui/jobs.excalidraw.svg`

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

Header with job info, followed by a table of tasks. Auto-refreshes via WebSocket.

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

Task statuses use the same color scheme as job statuses, plus:

| Status    | Color  |
|-----------|--------|
| `CLAIMED` | purple |

**Wireframe**: `docs/ui/job_detail.excalidraw.svg`

## Task Detail (`@task <id>`)

**Prompt**: `@task <id>`

**Top section**: status bar with task metadata — name, status badge, entrypoint, job name, worker ID, attempt info, timestamps, error (if any).

**Main section**: log viewer filling the remaining screen with vertical scroll. Logs stream in real-time via WebSocket when task is running. Log source is `log_path` field on the task.

**Wireframe**: `docs/ui/task_detail.excalidraw.svg`

# Tech Stack

| Layer    | Technology         |
|----------|--------------------|
| Frontend | Preact + TailwindCSS (SPA) |
| Backend  | FastAPI            |
| Real-time | WebSocket         |
| Bundler  | TBD (vite likely)  |

# Real-time Updates

- WebSocket connection from frontend to FastAPI backend
- Backend pushes updates when job/task status changes
- Frontend re-renders content area on update
- Connection auto-reconnects on disconnect
