UI Specification
---

**Implementation**: `src/App.tsx`

Single-screen, prompt-driven dashboard for aaiclick operators, served by the
FastAPI backend. Every view refreshes when an SSE `changed` signal invalidates
the query cache, falling back to 2 s polling only while that stream is down.
Tech stack, build, and the live-update chain: `docs/designs/frontend.md`.

# Layout

```
┌──────────────────────────────────────────┐
│ [aaiclick]  [ prompt input.............. ]│  ← fixed header
├──────────────────────────────────────────┤
│                                          │
│   dynamic content area                   │  ← updates based on prompt
│                                          │
└──────────────────────────────────────────┘
```

# Navigation

Clicks update the prompt, and the prompt drives the view:

```
(empty prompt)  ──────▶  Help / command reference
@jobs           ──────▶  Jobs list
@job <id>       ──────▶  Job detail (tasks, graph, timings, data)
@data …         ──────▶  Persistent objects and their rows
@dashboard …    ──────▶  Saved dashboard in a sandbox
```

```
@jobs  ──click job row──▶  @job <id>  ──click task row──▶  @task <id>
```

Home lists no `@task` command — tasks are reached through a job, or by typing `@task <id>`.

## Prompt dropdown

**Implementation**: `src/components/Header.tsx`

The input edits a draft; the view follows only the committed prompt.

- **Commit**: picking a row, Enter, or typing a whole argument-less command (`@jobs`, `@data`, `help`). Clicks inside views commit directly.
- **Esc**: drops the draft back to the committed prompt.
- **No command yet**: the dropdown lists matching commands.
- **After `<command> `**: it lists that command's values; the rest of the draft narrows them.

| Prefix                                          | Lists                | Narrowing     | Picked row inserts  |
|-------------------------------------------------|----------------------|---------------|---------------------|
| `@task`                                         | tasks, newest first  | name contains | `@task <id>`        |
| `@job`, `cancel`                                | jobs, newest first   | name contains | `<cmd> <id>`        |
| `run`, `enable`, `disable`                      | registered jobs      | name contains | `<cmd> <name>`      |
| `@data` (+ `persistent`), `@job <id> data`      | objects of the scope | name prefix   | `<cmd> … <name>`    |
| `@dashboard`                                    | dashboards           | name contains | `@dashboard <name>` |
| `@sandbox`                                      | sandbox files        | —             | `@sandbox <id>`     |

- Paged sources load 50 rows at a time as the list scrolls; objects and dashboards come in one page.
- Job and task rows read `name (short id)` — the id's last 6 digits — and insert the full id.
- The UI links jobs and tasks by id. A typed name stands for the latest: `@job <name>` is the latest run, `@job <ref> task <name>` its latest task of that name, `@job <name> data <object>` an object of that run.
- `@job <ref> task ` lists that job's tasks.
- Tasks and jobs also match a whole id, so a pasted id finds its row.
- `enable` / `disable` open a confirm view, like `cancel`, then return to `@registered`.

# Modes

## Home (empty prompt)

**Implementation**: `src/views/Home.tsx`

**Prompt**: _(empty)_ or `help`

Command reference. In local mode (auth disabled) the Account and Administration groups are greyed out and marked "(Distributed mode only)"; the dropdown labels those commands the same way.

One registry, `src/components/commands.ts`, feeds Home, the prompt dropdown and the "Unknown command" chips.

## Jobs List (`@jobs`)

**Implementation**: `src/views/Jobs.tsx`

Jobs, newest `created_at` first.

| Column   | Source field | Notes                                    |
|----------|--------------|------------------------------------------|
| Name     | `name`       | Clickable — sets prompt to `@job <id>`   |
| Status   | `status`     | Colored badge                            |
| Progress | computed     | `completed_tasks / total_tasks`          |
| Created  | `created_at` | Relative time (e.g., "2m ago")           |
| Duration | computed     | `started_at` to `completed_at` or now    |

Status badges, shared by jobs and tasks: `PENDING` gray, `RUNNING` blue,
`COMPLETED` green, `FAILED` red, `CANCELLED` yellow.

## Job Detail (`@job <id>`)

**Implementation**: `src/views/JobDetail.tsx`

**Job header**: name, status badge, created/started/completed times, error (if any).

**Tasks table**:

| Column     | Source field | Notes                                   |
|------------|--------------|-----------------------------------------|
| Name       | `name`       | Clickable — sets prompt to `@task <id>` |
| Status     | `status`     | Colored badge                           |
| Entrypoint | `entrypoint` |                                         |
| Attempt    | `attempt`    | `attempt / max_retries`                 |
| Started    | `started_at` | Relative time                           |
| Duration   | computed     | `started_at` to `completed_at` or now   |

A Table / Graph / Timings / Duration / Data toggle switches the body. The
prompt carries the mode — `@job <id> graph`, `timings`, `duration`, `data` —
so the view stays shareable as a URL.

**Data** (`@job <id> data [<item>]`) is the job's task inputs, results and
objects — see "Data" below for the listing. A row opens `@job <id> data
<item>`: the object view, or a task value by its address, under the same job
header.

**Graph**: one edge per dependency, so a `Group` dependency is a single edge
on the group's container — every member waits, as the scheduler enforces.
dagre cannot anchor an edge on a container, so the response's `layout_edges`
carry each group edge expanded onto its direct members, for positioning
only. Node colour follows task status; image-build tasks and
their edges are styled distinctly.

Groups render as nested containers around their members: `"group"` nodes with
a status rolled up server-side from every task beneath them (activity outranks
outcome) and timing spanning the earliest member start
to the latest finish. Empty groups are omitted. Containers are dagre clusters
drawn as React Flow subflows; clicking one does nothing, since only tasks have
a detail view.

**Timings** (`src/components/timings/`) is a Gantt view from
`GET /jobs/{ref}/timings`: one row per task in start order, one bar per
attempt (`task_runs`), earlier attempts faded. Before the first bar, grey dots
mark time **blocked** on upstream tasks and yellow hatching time **queued**
with no free worker; hatching also marks the wait between attempts. A task's
`ready_at` — the latest upstream `completed_at`, groups expanded to direct
members as the scheduler does — is computed on read. A strip under the rows
counts attempts running at once; Total runs from `ready_at` to the last
attempt's end.

**Duration** reads the same endpoint and ranks every attempt by run time,
longest first, labelled `name #n` when a task ran more than once. Both tabs
tick every second while the job is active, so running bars grow.

## Task Detail (`@task <id>`)

**Implementation**: `src/views/TaskDetail.tsx`

**Top section**: status bar with task metadata — name, status badge, entrypoint, job name, worker ID, attempt info, timestamps, error (if any).

A Log / Data toggle switches the main section; `@task <id> data` lists the
task's own inputs and result, each row opening in the job's Data tab.

**Main section**: scrolling log viewer filling the rest of the screen. Lines come from ClickHouse `task_logs` for one run of the task, so they resolve whichever host ran it. A retried task gets an Airflow-style "Task tries" row: one button per run, colored by how it ended; the latest is selected and polled. A failed run's log ends with its traceback on stderr (shell: `error: exit N`); the header shows only the latest error and clears on success. Returns `available=false` when the task has not run yet or the selected run captured no output. Lines are colored by keyword (`src/lib/logTone.ts`), stderr lines carry a marker bar, and an opt-in "Show timestamps" toggle reveals each line's `created_at`.

## Data (`@data [<object>]`)

**Implementation**: `src/views/Data.tsx`, `src/components/ScopeData.tsx`

**Prompt**: `@data` (or `@data persistent`) lists the persistent objects —
name, rows, size, created; `@data <object>` opens one. A job's data is on its
own page (`@job <id> data`).

`ScopeData` is the body shared with the job and task Data tabs: a scope's
listing, or one item of it. A job scope lists its tasks' inputs and results
before its objects (`viewer.md` — "Task Values"):

- **Columns**: Kind (`object` / `value` / `ref`) and Value / Target. Value /
  Target is a value's JSON preview, or a ref's type tag and what it points
  at — a link when it can be opened. On a phone the column is hidden and the
  value sits under the name.
- **Chips** filter the rows: job (its own kwargs and result), tasks,
  objects, all.
- **A cut page** says "Showing N of M rows".
- **Opening a task row** shows its header (kind, task, direction, points-to)
  and the stored JSON as one cell of the same `ResultsTable`, with no paging
  or pickers.

The object view is QueryView's explorer over one object: a header, then
Refresh, Limit (applied on blur or Enter) and Previous / Next, the kernel's
Fields and Order by pickers (fed from `GET /objects/{name}[?job=…]`), and
the rows in its `ResultsTable`. Fields only hide columns; an order-by change
re-runs `POST /viewer/query` at once, the previous page staying dimmed
meanwhile. A value past the cell width opens in the kernel's cell popup.

## Dashboard (`@dashboard [name]`)

**Implementation**: `src/views/Dashboard.tsx`

A dashboard picker, a Refresh button, and the dashboard's HTML in a
sandboxed iframe with the panel results as `window.queries` and its
selectors as `window.params` (`POST /viewer/runqueries`). The page calls
`window.setParams({…})`; the view substitutes the values into the panels'
`where` and posts the new results back without reloading the frame.
Authoring stays with agents and the CLI (`view dashboards save`).

## Account & Administration

Prompt-driven like the rest of the UI; flows and the role matrix are in
`docs/designs/auth.md` — SPA.

| Prompt          | View                                             | Implementation                |
|-----------------|--------------------------------------------------|-------------------------------|
| `@account`      | Change password, MFA setup / disable             | `src/views/Account.tsx`       |
| `@tokens`       | List / create / revoke the caller's API tokens   | `src/views/Tokens.tsx`        |
| `@users`        | Admin user table                                 | `src/views/Users.tsx`         |
| `@invite`       | Invite a user — admin only                       | `src/views/Invite.tsx`        |
| `@audit`        | Admin audit-log table with filters               | `src/views/Audit.tsx`         |
| `reset <token>` | New-password form from a reset link (no session) | `src/views/ResetPassword.tsx` |

`src/components/Header.tsx` shows the signed-in username (opens `@account`)
and sign-out; `src/views/Login.tsx` adds the MFA code field.
