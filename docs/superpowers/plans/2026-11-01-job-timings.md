# Job Timings and Duration Views Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add **Timings** (Gantt) and **Duration** (per-attempt ranking) tabs to the job detail page, fed by a new `GET /jobs/{ref}/timings` endpoint.

**Architecture:** A pure upstream-resolver in `orchestration/graph.py` and a pure view builder in `view_models.py` turn tasks, `task_runs` rows and dependencies into `JobTimingsView`. `internal_api.jobs.get_job_timings` loads the rows (sharing the loader with `get_job_graph`); a router exposes it. The SPA renders both tabs from one `useJobTimings` query with a pure layout module `src/lib/timings.ts`.

**Tech Stack:** FastAPI, SQLModel, pytest (async); React, TanStack Query, vitest; Playwright e2e.

**Spec:** `docs/superpowers/specs/2026-11-01-job-timings-design.md`

## Global Constraints

- No migration: `ready_at` is computed on read.
- No new frontend dependency: bars are divs positioned with CSS percentages.
- Routes: `@job <name> timings` → `view: "timings"`; `@job <name> duration` → `view: "duration"`.
- Query key `["job-timings", ref]`, added to `LIVE_KEYS` in `src/api/events.ts`.
- Bar colours reuse `b-<STATUS>` badge classes (`statusClass` from `src/lib/status.ts`).
- Run time of an attempt = `started_at` → `completed_at`, or now while `RUNNING`.
- `src/api/schema.ts` is generated: `npm run gen-types`, never hand-edited.
- All imports at file top; no `Any`; `Literal`, not enums.

## Review Focus

- Job with zero tasks → endpoint returns `tasks=[]`; both tabs render an empty state, no division by zero on the axis.
- Task depending on a **group** (incl. nested group) → `ready_at` waits for every task beneath it, not just direct members.
- Upstream task still running or never started → `ready_at is None`, row shows blocked to "now", not a negative span.
- Task with attempts from before a `clear_task` → all attempts listed in `run_id` order; Duration labels `#1..#n`.
- Clock skew: an attempt whose `completed_at < started_at` → clamp run time to 0, never a negative bar width.

---

### Task 1: Upstream resolver

**Files:**
- Modify: `aaiclick/orchestration/graph.py`
- Test: `aaiclick/orchestration/test_graph.py`

**Interfaces:**
- Produces: `upstream_task_ids(dependencies: Sequence[DependencyRow], group_members: Mapping[int, set[int]], group_children: Mapping[int, set[int]]) -> dict[int, set[int]]` — task id → ids of tasks it waits on. Group endpoints expand to **every** task beneath (`group_member_tasks`), then reuse `build_graph_edges(...).layout` with that full-membership mapping.

- [ ] **Step 1: Write failing tests** in `test_graph.py`:
  - `test_upstream_task_ids_task_to_task`: rows `a→b`, `b→c` (task type) → `{b: {a}, c: {b}}`.
  - `test_upstream_task_ids_nested_group`: group `g` with child group `h`; task `x` in `g`, `y` in `h`; row `g→z` → `{z: {x, y}}`.
- [ ] **Step 2:** `pytest aaiclick/orchestration/test_graph.py -k upstream -v` → FAIL (import error).
- [ ] **Step 3:** Implement `upstream_task_ids` per Interfaces.
- [ ] **Step 4:** Re-run → PASS.
- [ ] **Step 5:** Commit `feat: resolve upstream tasks for timing`.

### Task 2: Timings view model and endpoint

**Files:**
- Modify: `aaiclick/orchestration/view_models.py`, `aaiclick/internal_api/jobs.py`, `aaiclick/server/routers/jobs.py`
- Create: `aaiclick/internal_api/test_jobs_timings.py`
- Modify: `src/api/schema.ts` (generated), `src/api/types.ts`

**Interfaces:**
- Consumes: `upstream_task_ids` (Task 1); `TaskAttemptView` (existing).
- Produces:
  - `TaskTimingView(id: SnowflakeId, name: str, status: TaskStatus, created_at: datetime, ready_at: datetime | None, attempts: list[TaskAttemptView])`
  - `JobTimingsView(job_id: SnowflakeId, tasks: list[TaskTimingView])` — docstring: served by ``GET /jobs/{ref}/timings``.
  - `build_job_timings_view(job: Job, tasks: list[Task], runs: list[TaskRun], upstream: Mapping[int, set[int]]) -> JobTimingsView` in `view_models.py`. `ready_at`: no upstream → `created_at`; any upstream task missing or without `completed_at` → `None`; else max upstream `completed_at`. Attempts numbered from 1 in `run_id` order.
  - `async def get_job_timings(ref: RefId) -> JobTimingsView` in `internal_api/jobs.py`. Extract `_load_graph_rows(ref, session) -> tuple[Job, list[Task], list[Group], list[Dependency]]` from `get_job_graph` and use it in both. Runs: one query `TaskRun.task_id IN (select Task.id where job_id = …)`, ordered by `run_id`.
  - Route `GET /jobs/{ref}/timings`, `response_model=JobTimingsView`, `responses=problem_responses(404)`, beside `/graph`.
  - TS: `export type JobTimingsView = S["JobTimingsView"]; export type TaskTimingView = S["TaskTimingView"];`

- [ ] **Step 1: Write failing tests** in `test_jobs_timings.py` (pattern from `test_jobs_graph.py`, `set_task_runs` from `aaiclick.testing`):
  - `test_timings_lists_attempts_in_order`: task with runs `[FAILED, COMPLETED]` → `[a.attempt for a in t.attempts] == [1, 2]`, statuses match.
  - `test_timings_never_run_task`: `attempts == []`.
  - `test_timings_ready_at`: `raw >> report`; raw `completed_at` set, report → `report.ready_at == raw.completed_at`; raw → `ready_at == raw.created_at`.
  - `test_timings_ready_at_none_while_upstream_running`: upstream without `completed_at` → `None`.
  - `test_timings_ready_at_through_group`: group of two tasks `>> z`; one member unfinished → `None`; both finished → later `completed_at`.
  - `test_timings_unknown_ref`: raises `errors.NotFound`.
- [ ] **Step 2:** `pytest aaiclick/internal_api/test_jobs_timings.py -v` → FAIL.
- [ ] **Step 3:** Implement view models, builder, loader refactor, `get_job_timings`, route.
- [ ] **Step 4:** Re-run, plus `pytest aaiclick/internal_api/test_jobs_graph.py aaiclick/orchestration -q` → PASS (loader refactor unbroken).
- [ ] **Step 5:** `npm run gen-types`; add TS aliases; `npx tsc -b` → clean.
- [ ] **Step 6:** Commit `feat: GET /jobs/{ref}/timings`.

### Task 3: Layout module

**Files:**
- Create: `src/lib/timings.ts`, `src/lib/timings.test.ts`

**Interfaces:**
- Consumes: `TaskTimingView` (Task 2), `parseUtc` from `src/lib/format.ts`.
- Produces (all times in ms, `now` injected):
  - `type Span = { start: number; end: number }`
  - `type TimelineRow = { task: TaskTimingView; blocked: Span | null; queued: Span | null; attempts: { span: Span; status: TaskStatus; attempt: number }[]; waits: Span[]; total: number | null }`
  - `timelineRows(tasks: TaskTimingView[], now: number): { origin: number; end: number; rows: TimelineRow[] }` — origin = min `created_at`; end = max of all span ends (min `origin + 1000` to avoid a zero-width axis); rows sorted by first attempt start, never-run last. `blocked` = created→(`ready_at` ?? now); `queued` = `ready_at`→first start (null if `ready_at` null or no attempt); `waits` between attempts; open attempt or open wait ends at `now`; `total` = `ready_at`→last end.
  - `parallelCounts(rows: TimelineRow[], origin: number, end: number, buckets: number): number[]`
  - `type DurationRow = { taskId: string; label: string; status: TaskStatus; ms: number }`
  - `durationRows(tasks: TaskTimingView[], now: number): DurationRow[]` — one per attempt, `label` = name + `` ` #${n}` `` only when the task has >1 attempt, sorted by `ms` desc, `ms` clamped ≥ 0.

- [ ] **Step 1: Write failing tests** in `timings.test.ts` with a fixture of three tasks (root completed, retried `[FAILED, COMPLETED]`, never-run):
  - rows order: root, retried, never-run; never-run has `attempts.length === 0`.
  - retried row: `waits.length === 1`, `attempts[0].status === "FAILED"`.
  - running attempt with `completed_at: null` → span end `=== now`.
  - `durationRows` labels `["retried #2", "root", "retried #1"]` for the fixture's durations; skewed attempt → `ms === 0`.
  - `parallelCounts` with two overlapping attempts → max bucket `2`.
  - empty input → `rows: []`, `end > origin`.
- [ ] **Step 2:** `npx vitest run src/lib/timings.test.ts` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Re-run → PASS.
- [ ] **Step 5:** Commit `feat: timings layout helpers`.

### Task 4: Routes, tabs and components

**Files:**
- Modify: `src/prompt.ts`, `src/prompt.test.ts`, `src/api/hooks.ts`, `src/api/events.ts`, `src/views/JobDetail.tsx`, `src/styles/globals.css`, `docs/designs/ui.md`
- Create: `src/components/JobTimings.tsx`, `src/components/JobDuration.tsx`

**Interfaces:**
- Consumes: Task 2 types, Task 3 functions.
- Produces: `JobViewMode = "table" | "graph" | "timings" | "duration"`; `useJobTimings(ref: string)`; `<JobTimings refId onPrompt />`, `<JobDuration refId onPrompt />`.

- [ ] **Step 1: Failing test** in `prompt.test.ts`: `parsePrompt("@job x timings")` → `{kind:"job",name:"x",view:"timings"}`; same for `duration`.
- [ ] **Step 2:** `npx vitest run src/prompt.test.ts` → FAIL.
- [ ] **Step 3:** Parse the trailing word against `graph | timings | duration` in `parsePrompt`.
- [ ] **Step 4:** Re-run → PASS.
- [ ] **Step 5:** Hook `useJobTimings` (mirrors `useJobGraph`); add `["job-timings"]` to `LIVE_KEYS`.
- [ ] **Step 6:** Components, matching the mockups in the spec:
  - Each owns its `LiveStatus` badge (`queryKey="job-timings"`) like `JobGraph`; `JobDetail` hides its own badge for every non-table view.
  - 1s re-render tick (`setInterval` in `useEffect`) only while job status is `RUNNING`/`PENDING` — pass `jobStatus` prop from `JobDetail`.
  - Timings: axis ticks, per-row blocked (dotted), queued/waits (hatched), attempt bars (earlier faded), Total column, "not started" row, `parallel` strip, `title` hover text per spec; row click → `onPrompt(\`@task ${id}\`)`.
  - Duration: one row per `DurationRow`, footer "N task(s) not started" when any.
  - Empty job: `<p className="sub">no tasks</p>`.
- [ ] **Step 7:** Chips: add `Timings` → `@job <name> timings`, `Duration` → `@job <name> duration`.
- [ ] **Step 8:** CSS classes `.tl-*` in `globals.css` (reuse tokens; no new colours).
- [ ] **Step 9:** `docs/designs/ui.md` Job Detail: four-tab toggle, both views, `/timings` endpoint (follow `markdown-style`).
- [ ] **Step 10:** `npx tsc -b && npx vitest run && npm run build` → clean.
- [ ] **Step 11:** Commit `feat: Timings and Duration tabs`.

### Task 5: E2E and cleanup

**Files:**
- Modify: `test_e2e/web/seed.py` (write one `TaskRun` per seeded task that has `started_offset_s`, with matching times/status)
- Create: `test_e2e/web/test_timings_ui.py`
- Delete: this plan and the spec; fix references.

- [ ] **Step 1:** Seed `TaskRun` rows.
- [ ] **Step 2:** `test_timings_tab_renders_rows`: open `@job <id> timings` → one `.tl-row` per seeded task; `test_duration_tab_ranks_longest_first`: first row's time ≥ second's.
- [ ] **Step 3:** `npm run build && pytest test_e2e/web/test_timings_ui.py test_e2e/web/test_graph_ui.py -v -p no:cov` → PASS.
- [ ] **Step 4:** Remove plan/spec (`git rm`), grep for references, commit `test: timings e2e; drop landed spec`.
- [ ] **Step 5:** Push; run `devpowers:check-pr`.
