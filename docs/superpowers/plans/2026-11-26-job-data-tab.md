# Job Data Tab Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A job's task values and objects live on `@job <ref> data`, a task's on `@task <id> data`, and `@data` lists persistent objects only.

**Architecture:** Frontend only. The body of today's `@data` page (listing, object explorer, value view) moves into one shared component keyed by scope; the data, job and task pages each mount it. The prompt parser and `dataPrompt` are the only places that know the prompt forms.

**Tech Stack:** React + TypeScript, TanStack Query, vitest, Playwright (pytest) e2e.

**Spec:** `docs/superpowers/specs/2026-11-26-job-data-tab-design.md`

## Global Constraints

- No backend or API changes.
- No redirect for `@data job …`; the form is removed.
- No "All scopes" listing; delete `ALL`, `ScopeTree`, `ObjectsMenu`.
- Job data queries use the job **id**, never the name, so a rerun's page shows its own data.
- Delete this plan and the spec in the last task (CLAUDE.md).

## Review Focus

- `@job <name> data` on a job with no values and no objects: an empty message, not a blank body.
- `@task <id> data` on a task with no kwargs and a null result: "No inputs or result".
- A job past the 1000-row page: the server drops task rows first, so the task tab can come up short — leave a `shortcut:` comment at the filter.
- `@data persistent` still lists persistent objects (the word stays an accepted alias).
- A job item whose name is a view word (`@job etl data graph`) parses as the Data tab with item `graph`.

---

### Task 1: Prompt forms

**Files:**
- Modify: `src/prompt.ts`, `src/lib/viewer.ts`, `src/components/promptSources.ts`, `src/components/commands.ts`
- Test: `src/prompt.test.ts`, `src/lib/viewer.test.ts`, `src/components/promptSources.test.ts`

**Interfaces — Produces:**
- `type JobViewMode = "table" | "graph" | "timings" | "duration" | "data"`
- `type TaskViewMode = "log" | "data"`
- `Route`: `{ kind: "job"; name: string; view: JobViewMode; item: string | null }`, `{ kind: "task"; id: string; view: TaskViewMode }`, `{ kind: "data"; object: string | null }`
- `JOB_VIEW_SUFFIX` matches a trailing `graph|timings|duration|data` or `data <item>`
- `dataPrompt(scope: string, object?: string): string` — `@job <id> data [<object>]` for a job scope, `@data [<object>]` otherwise
- `ALL` and the "All scopes" label removed from `viewer.ts`

- [ ] **Step 1: Failing tests**

`prompt.test.ts` cases:

```ts
["@data", { kind: "data", object: null }],
["@data persistent", { kind: "data", object: null }],
["@data persistent orders", { kind: "data", object: "orders" }],
["@data orders", { kind: "data", object: "orders" }],
["@job etl", { kind: "job", name: "etl", view: "table", item: null }],
["@job etl graph", { kind: "job", name: "etl", view: "graph", item: null }],
["@job etl data", { kind: "job", name: "etl", view: "data", item: null }],
["@job 123 data sales", { kind: "job", name: "123", view: "data", item: "sales" }],
["@job etl data graph", { kind: "job", name: "etl", view: "data", item: "graph" }],
["@task 7", { kind: "task", id: "7", view: "log" }],
["@task 7 data", { kind: "task", id: "7", view: "data" }],
```

`viewer.test.ts`: `dataPrompt("persistent")` → `@data`, `("persistent", "orders")` → `@data orders`, `("job:7")` → `@job 7 data`, `("job:7", "sales")` → `@job 7 data sales`; drop the `all` cases.

`promptSources.test.ts`: `"@job etl data "` → key `objects:@job etl data`, text `""`; `"@job etl data ord"` inserts `@job etl data orders`; `"@job et"` still completes job names; drop every `@data job` case.

- [ ] **Step 2:** `npx vitest run src/prompt.test.ts src/lib/viewer.test.ts src/components/promptSources.test.ts` — FAIL.
- [ ] **Step 3:** Implement. `commands.ts` help reads `@data [<object>]`; the `@job` help line names `data`.
- [ ] **Step 4:** Same command — PASS.
- [ ] **Step 5:** Commit.

### Task 2: Shared scope body, persistent-only Data page

**Files:**
- Create: `src/components/ScopeData.tsx`
- Modify: `src/views/Data.tsx`, `src/components/ObjectsTable.tsx`, `src/api/hooks.ts`, `src/styles/globals.css`, `src/App.tsx`
- Delete: `src/components/ScopeTree.tsx`

**Interfaces — Produces:**
- `ScopeData({ scope, object, taskId?, onPrompt })` — the listing (cut line + `ObjectsTable`), or `ValueView` / `ObjectExplorer` when `object` is set. `taskId` keeps only rows with that `task_id`.
- `ObjectsTable` gains `filters?: boolean` (default true) and `emptyText?: string` (default "No objects in this scope").
- `Data({ object, onPrompt })`.

- [ ] **Step 1:** Move `ValueView` and `ObjectExplorer` into `ScopeData.tsx` unchanged, except each has one back chip: `← <scope label>` → `dataPrompt(scope)`.
- [ ] **Step 2:** `Data.tsx` keeps its header and chips and mounts `<ScopeData scope={PERSISTENT} …>`; no scope tree, no two-column layout.
- [ ] **Step 3:** `hooks.ts` `jobParam` loses the `ALL` branch; remove scope-tree CSS.
- [ ] **Step 4:** Commit.

### Task 3: Job Data tab

**Files:**
- Modify: `src/views/JobDetail.tsx`, `src/App.tsx`
- Delete: `src/components/ObjectsMenu.tsx`

- [ ] **Step 1:** `JobDetail` takes `item: string | null`; a fifth chip `Data` → `@job <job.id> data`; body `<ScopeData scope={scopeKey(job.id)} object={item} …>` when `view === "data"`. The objects query refetches when `job.completed_tasks` changes, as the menu did.
- [ ] **Step 2:** Remove `ObjectsMenu` from the job page and delete the file once Task 4 drops its other use.
- [ ] **Step 3:** Commit.

### Task 4: Task Log / Data

**Files:**
- Modify: `src/views/TaskDetail.tsx`, `src/App.tsx`

- [ ] **Step 1:** `TaskDetail` takes `view: TaskViewMode`; chips `Log` → `@task <id>`, `Data` → `@task <id> data`. Data mounts `<ScopeData scope={scopeKey(task.job_id)} object={null} taskId={task.id} …>` with `filters={false}` and empty text "No inputs or result".
- [ ] **Step 2:** `npm run check && npm run test && npm run format:check` — all pass.
- [ ] **Step 3:** Commit.

### Task 5: e2e, docs, cleanup

**Files:**
- Modify: `test_e2e/web/test_viewer.py` (and any e2e test using the Objects menu), `docs/designs/ui.md`, `docs/designs/viewer.md`
- Delete: this plan, the spec

- [ ] **Step 1:** e2e: job-scope cases open `@job <id> data`; add `test_task_data_tab` (a seeded task's input row shows; a task with none shows "No inputs or result"); drop scope-tree and Objects-menu cases.
- [ ] **Step 2:** Run `pytest test_e2e/web/test_viewer.py` — PASS.
- [ ] **Step 3:** Rewrite the Job Detail, Task Detail and Data sections of `ui.md`; fix the prompt in `viewer.md`.
- [ ] **Step 4:** Delete plan and spec; commit; push.
