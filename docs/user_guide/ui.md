Web UI
---

`local start` and the API server serve a single-screen dashboard at the server
root (`http://127.0.0.1:5255` by default). One prompt drives every view; views
refresh on their own as jobs change.

# Layout

```
┌──────────────────────────────────────────┐
│ [aaiclick]  [ prompt input.............. ]│  ← fixed header
├──────────────────────────────────────────┤
│                                          │
│   dynamic content area                   │  ← follows the prompt
│                                          │
└──────────────────────────────────────────┘
```

<!-- req: prompt-drives-the-view -->
Clicks update the prompt, and the prompt drives the view; the URL carries the
prompt, so any view is shareable and Back restores the previous one.

```
(empty prompt)  ──────▶  Help / command reference
@jobs           ──────▶  Jobs list
@registered     ──────▶  Registered jobs
@job <id>       ──────▶  Job detail (tasks, graph, timings, data)
@task <id>      ──────▶  Task detail (log, data)
@data …         ──────▶  Persistent objects and their rows
@dashboard …    ──────▶  Saved dashboard
@sandbox        ──────▶  Sandbox submissions
```

# The prompt

<!-- req: prompt-dropdown -->
The input edits a draft; the view follows only the committed prompt.

- **Commit**: pick a row, press Enter, or type a whole argument-less command (`@jobs`, `@data`, `help`).
- **Esc** drops the draft back to the committed prompt.
- **Before a command** the dropdown lists matching commands; **after `<command> `** it lists that command's values, narrowed by the rest of the draft. Lists load more rows as you scroll.

| Prefix                                     | Lists                | Picked row inserts  |
|--------------------------------------------|----------------------|---------------------|
| `@task`                                    | tasks, newest first  | `@task <id>`        |
| `@job`, `cancel`                           | jobs, newest first   | `<cmd> <id>`        |
| `run`, `enable`, `disable`, `unregister`   | registered jobs      | `<cmd> <name>`      |
| `@data`, `@job <id> data`                  | objects of the scope | `<cmd> … <name>`    |
| `@dashboard`                               | dashboards           | `@dashboard <name>` |
| `@sandbox`                                 | sandbox files        | `@sandbox <id>`     |

<!-- req: ids-in-links-names-when-typed -->
A link the UI makes always carries an id; a typed name means "the latest". So
`@job <name>` and `cancel <name>` open the latest run of that name, and
`@job <ref> task <name>` that run's latest task of that name. Dropdown rows
read `name (short id)` so same-named runs can be told apart. Registered jobs,
objects and dashboards have no id: their name is their identity.

# Views

## Home

<!-- req: home -->
The empty prompt (or `help`) shows the command reference; clicking a row runs
it. In local mode the Account and Administration commands are greyed out and
marked "(Distributed mode only)".

## Jobs and registered jobs

<!-- req: jobs-list -->
`@jobs` lists jobs newest first: name, status, progress (`completed / total`
tasks), created and duration. Status badges, shared by jobs and tasks:
`PENDING` gray, `RUNNING` blue, `COMPLETED` green, `FAILED` red, `CANCELLED`
yellow.

`@registered` lists registered jobs. `run <name>` runs one (add `?` to edit
its params first), `register` opens the registration form, and `enable`,
`disable`, `unregister` and `cancel <id>` ask for confirmation, then return to
the list. Past runs survive `unregister`.

## Job detail

<!-- req: job-detail -->
`@job <id>` shows the job header (name, status, times, error) and its tasks:
name, status, entrypoint, attempt (`attempt / max_retries`), started and
duration. A Table / Graph / Timings / Duration / Data toggle switches the
body; the prompt carries the mode (`@job <id> graph`, `timings`, `duration`,
`data`).

<!-- req: job-graph -->
- **Graph**: one node per task, coloured by status, one edge per dependency.
  A group is drawn as a container around its members, its status rolled up
  from them; a `Group` dependency is a single edge into the container.
  Image-build dependencies show as badges on the task; a toggle reveals their
  edges. Clicking a task opens it; clicking a group does nothing.

<!-- req: job-timings -->
- **Timings**: a Gantt chart, one row per task in start order and one bar
  per attempt. Grey dots mark time blocked on upstream tasks, yellow hatching
  time queued with no free worker. **Duration** ranks every attempt by run
  time, longest first. Both tick every second while the job runs.

- **Data**: the job's task inputs, results and objects (see [Data](#data)).

## Task detail

<!-- req: task-detail -->
`@task <id>` shows the task's metadata (name, status, entrypoint, job,
worker, attempt, times, error) above its log. The log streams while the task
runs, whichever host ran it. A retried task gets one button per run, coloured
by how it ended; the latest is selected. Lines are coloured by keyword, stderr
lines carry a marker bar, and "Show timestamps" reveals each line's time. A
task that has not started says so. `@task <id> data` lists its own inputs and
result.

## Data

<!-- req: data-view -->
`@data` lists persistent objects (name, rows, size, created); `@data <object>`
opens one. A job's Data tab lists its tasks' inputs and results before its
objects, with chips to filter job / tasks / objects / all; a cut page says
"Showing N of M rows". An object opens as a table with Refresh, Limit,
Previous / Next, a Fields picker (hides columns) and an Order by picker (runs
the query again).

## Dashboard

<!-- req: dashboard -->
`@dashboard <name>` renders a saved dashboard: its HTML with live panel
results, and its selectors re-running the panels without a reload. Dashboards
are authored by agents and the CLI (`view dashboards save`).

## Sandbox

`@sandbox` submits a single-file job and lists submissions; `@sandbox <id>`
opens one. See [Sandbox](sandbox.md).

## Account and administration

<!-- req: role-gated-controls -->
In distributed mode the header shows the signed-in user (opens `@account`)
and a sign-out control. Admin-only actions (run, cancel, register, enable / disable) are shown to
viewers and members **disabled with a tooltip**, not hidden. Account
and admin views: [Access](access.md#account-pages).
