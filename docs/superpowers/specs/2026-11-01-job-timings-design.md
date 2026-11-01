# Job Timings and Duration Views

Two new tabs on the job detail page, beside **Table** and **Graph**:

- **Timings** — every task as horizontal bars on a shared time axis, Gantt-style.
- **Duration** — tasks ranked longest to shortest.

## Intent

Answer, at a glance, why a job took as long as it did:

- **Queueing** — a task was ready but no worker picked it up.
- **Idle gaps** — stretches where nothing in the job ran.
- **Parallelism** — how many tasks ran at once; a mostly-sequential job is obvious.
- **Retries** — earlier failed attempts and the backoff between them.
- **Which task took longest** — the Duration tab.

Non-goals: zoom/pan, dependency arrows (Graph covers those), group nesting,
CLI or MCP exposure.

Anti-goal: a view that silently hides retries — a task that failed twice then
succeeded must not read as one short bar.

## Approaches

| Option | Trade-off |
|--------|-----------|
| **`GET /jobs/{ref}/timings`** (picked) | Mirrors `/graph`: fetched only while the tab is open; `JobDetail` unchanged for CLI/MCP/Table |
| Always add `attempts` to every task in `JobDetail` | Least code, but every job poll and MCP `get_job` carries data only this view uses |
| `include_retries` flag on `GET /jobs/{ref}` | One endpoint, two response shapes |

Picked the dedicated endpoint for parity with Graph.

## Data

Per-attempt timing already exists on main: `task_runs` (`TaskRun`, one row per
attempt — `run_id`, `task_id`, `status`, `started_at`, `completed_at`). Attempt
N is the Nth row by `run_id`, same rule as `TaskAttemptView` in
`internal_api/tasks.py`.

### API

`GET /jobs/{ref}/timings` → `JobTimingsView`, in `orchestration/view_models.py`:

```python
class TaskTimingView(BaseModel):
    id: SnowflakeId
    name: str
    status: TaskStatus
    created_at: datetime
    ready_at: datetime | None  # last upstream finished; created_at if no deps; None while blocked
    attempts: list[TaskAttemptView] = Field(default_factory=list)  # reused, ordered by run_id

class JobTimingsView(BaseModel):
    job_id: SnowflakeId
    tasks: list[TaskTimingView] = Field(default_factory=list)
```

- `internal_api.jobs.get_job_timings(ref)`: load job + tasks with
  `_load_job_and_tasks`, then **one** query for all `task_runs` of the job,
  matched by subquery (`task_id IN (SELECT id FROM tasks WHERE job_id = …)`) —
  same constant-size-SQL reasoning as `get_job_graph`.
- Router: `@router.get("/{ref}/timings")` next to `/{ref}/graph`, same auth.
- Never-run tasks appear with `attempts=[]`.
- `ready_at` = latest `completed_at` among the task's upstream tasks, with
  group dependencies expanded onto member tasks the way `get_job_graph` does
  (reuse that expansion, don't re-derive it). No upstream → `created_at`. Any
  upstream unfinished → `None`. Computed on read; no migration.
- No server-side ordering beyond attempts-by-`run_id`; the client sorts.
- Runs from before a `clear_task` stay in `attempts` — the clear's
  `CANCELLED` run marks the boundary. Gaps between attempts are labelled
  "waiting", not "backoff", since a gap may be a clear-and-rerun.
  *(builder call — `TaskRun` has no epoch to split on)*

## UI

Routes: `@job <name> timings` → `view: "timings"`, `@job <name> duration` →
`view: "duration"`. `JobViewMode` widens to
`"table" | "graph" | "timings" | "duration"`; links stay shareable.

Both tabs read the same `useJobTimings` query.

Hook: `useJobTimings(ref)`, query key `["job-timings", ref]`, added to
`LIVE_KEYS` in `src/api/events.ts` — `TaskRun` is already a watched model, so
live updates arrive like Graph's.

### Timings tab

```
+----------------------------------------------------------------+
| [Table] [Graph] [Timings] [Duration]                           |
|                                                                |
| NAME                0s      1s      2s      3s      4s      5s |
| extract           ..[======]                                   |
| flaky_load            [xx]...[xx]....[=========]               |
| report                                         [=====>         |
| not_yet                                        (not started)   |
| parallel  11112222221111111111111111111111111111111            |
+----------------------------------------------------------------+
```

- Axis: 0 = earliest task `created_at`; right edge = latest
  `completed_at`, or now while anything is running.
- Rows: sorted by first attempt's `started_at`; never-run tasks last, no bar.
- Per row, left to right:
  - **Blocked** (grey dots): `created_at` → `ready_at`, waiting on upstream.
  - **Queued** (yellow hatch): `ready_at` → first attempt start, ready but no
    worker.
  - Each attempt as a bar, coloured by its status (reuse `b-<STATUS>`
    classes). Earlier attempts faded.
  - **Waiting** (yellow hatch): previous attempt end → next attempt start.
  - A `RUNNING` attempt extends to now.
  - A gap still open (task `PENDING` after a failed attempt) is drawn to now.
    *(builder call)*
- Hover a row: blocked Xs · queued Ys, then per attempt "#n STATUS Zs · waited Ws".
- **Total** column: `ready_at` → last attempt end (or now).
- Never-run row: "not started" in place of a bar.
- Click a row: `@task <id>`, same as Table.
- **parallel** strip: count of attempts running at each instant, as a step
  bar/heat strip under the rows.

### Duration tab

```
+----------------------------------------------------------------+
| [Table] [Graph] [Timings] [Duration]                           |
|                                                                |
| NAME                 RUN TIME                                  |
| flaky_load #3        [=================]          1.9s         |
| extract              [==============]             1.8s         |
| report               [========]                   1.0s         |
| flaky_load #2        [xx]                         0.6s         |
| flaky_load #1        [xx]                         0.5s         |
+----------------------------------------------------------------+
```

- One row per **attempt**, all bars starting at 0, sorted longest first, with
  the run time beside it. Bar coloured by the attempt's status.
- Name: task name, plus ` #n` when the task has more than one attempt.
- Run time = attempt `started_at` → `completed_at` (or now while `RUNNING`).
- Never-run tasks are omitted; a footer counts them.
- Click: `@task <id>`. Hover: status and start time.

Rendering: plain divs positioned with CSS percentages; no chart dependency.
Re-render on a 1s tick while the job is `RUNNING` or `PENDING`, so live bars
and open gaps grow. *(builder call — ticking on job status, not task status,
keeps backoff gaps live)*

## Testing

- `internal_api/test_jobs_timings.py`: retried task returns attempts in order
  with timings; never-run task has `attempts=[]`; unknown ref → 404 like `/graph`.
- Pure layout helper `src/lib/timings.ts` (bar offsets, sort, parallel counts)
  with a vitest file; `prompt.test.ts` covers the new routes.

## Docs

`docs/designs/ui.md` "Job Detail" section: Table/Graph/Timings toggle and
the two modes.
