# Prompt Argument Dropdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A custom prompt dropdown that lists real argument values (tasks, jobs, registered jobs, objects, dashboards, sandbox files) with infinite scroll, a view that waits for commit, `enable`/`disable` prompt commands, and a task list from internal API to CLI.

**Architecture:** Backend gains `list_tasks` (internal API → REST → MCP → CLI). Frontend splits the prompt into draft (Header) and committed (App); a source table maps a draft prefix to a paged fetcher and row renderer; `PromptDropdown` renders it with `useInfiniteQuery` and an `IntersectionObserver` sentinel.

**Tech Stack:** Python / FastAPI / SQLModel / FastMCP / argparse; React 19, @tanstack/react-query 5, vitest, Playwright (`test_e2e/web`).

**Spec:** `docs/superpowers/specs/2026-11-03-prompt-dropdown-design.md`

## Global Constraints

- All imports at file top; no `TYPE_CHECKING`; no `__all__`; `Literal` over enums (CLAUDE.md).
- No new npm or Python dependency.
- Page size 50 (the existing `PageLimit` default); narrowing debounce ~200 ms.
- Snowflake ids stay strings in the frontend.
- After changing REST models, regenerate `src/api/schema.ts` with `npm run gen-types`.

## Review Focus

- `@task ` on an empty install → dropdown shows "no matches", not a spinner forever. Test in Task 4.
- Narrowing text containing `%` or `_` → treated literally, not as a SQL wildcard, for the new task filter. Test in Task 1.
- Draft typed then browser Back → view and input both follow the URL. Test in Task 5.
- Enter on a draft with no dropdown row highlighted → commits the draft as typed (unknown command still reachable). Test in Task 5.
- Last page reached → sentinel stops fetching (no request loop when `items.length < limit`). Test in Task 4.

---

### Task 1: `list_tasks` internal API

**Files:**
- Modify: `aaiclick/orchestration/view_models.py` (add `TaskListFilter` beside `TaskView`)
- Modify: `aaiclick/internal_api/tasks.py`, `aaiclick/internal_api/__init__.py`
- Test: `aaiclick/internal_api/test_tasks.py`

**Interfaces:**
- Produces: `TaskListFilter(ApiModel)` with `name: str | None = None`, `job: RefId | None = None`, `status: TaskStatus | None = None`, `limit: PageLimit = 50`, `offset: PageOffset = 0`.
- Produces: `async def list_tasks(filter: TaskListFilter | None = None) -> Page[TaskView]`, exported from `aaiclick.internal_api`.

- [ ] **Step 1: Write failing tests** in `test_tasks.py`:
  - `test_list_tasks_newest_first` — create two jobs; `[t.id for t in page.items]` is descending; `page.total == 2`.
  - `test_list_tasks_filters` (parametrized over `name` substring, `job` by name, `status`) — only matching tasks returned.
  - `test_list_tasks_name_is_literal` — `name="a%b"` matches nothing when no task contains `a%b`.
  - `test_list_tasks_paging` — `limit=1, offset=1` returns the second-newest; `total` unchanged.
  - `test_list_tasks_unknown_job_raises` — `job="nope"` → `errors.NotFound`.
- [ ] **Step 2:** `uv run pytest aaiclick/internal_api/test_tasks.py -k list_tasks -v` → FAIL (import error).
- [ ] **Step 3: Implement.** Order by `col(Task.id).desc()` (snowflake ids are time-ordered and the PK index serves it; `created_at` has none). `name` → `col(Task.name).contains(name, autoescape=True)`. `job` → `jobs_api.resolve_job(ref).id`. Use `paginate`; map rows with the existing task→`TaskView` helper used by `JobDetail`.
- [ ] **Step 4:** re-run → PASS.
- [ ] **Step 5:** commit `feat(internal_api): list_tasks`.

### Task 2: REST, MCP and CLI for task list

**Files:**
- Modify: `aaiclick/server/routers/tasks.py`, `aaiclick/server/mcp.py`, `aaiclick/__main__.py`, `aaiclick/cli_renderers.py`
- Modify: `src/api/schema.ts` (regenerated), `src/api/types.ts` (export `TaskListFilter` if types are re-exported there)
- Test: `aaiclick/server/routers/test_tasks.py`, `aaiclick/server/test_mcp.py`, `aaiclick/test_cli.py`, `aaiclick/test_cli_renderers.py`

**Interfaces:**
- Consumes: `list_tasks`, `TaskListFilter` (Task 1).
- Produces: `GET /api/v0/tasks?name=&job=&status=&limit=&offset=` → `Page[TaskView]`; MCP tool `list_tasks`; CLI `aaiclick task list [--like] [--job] [--status] [--limit] [--offset] [--json]`; `render_tasks_page(page: Page[TaskView], offset: int) -> None`.

- [ ] **Step 1: Failing tests:** `test_list_tasks` (REST: 200, items newest first, `?name=` narrows); add `"list_tasks"` to `EXPECTED_TOOLS` and `test_list_tasks_returns_page_view`; CLI `task list --json` parses to `Page[TaskView]`; renderer prints one row per task with id, name, job id, status.
- [ ] **Step 2:** run those files → FAIL.
- [ ] **Step 3: Implement.** Route `@router.get("", response_model=Page[TaskView])` declared before `/{task_id}`. MCP tool mirrors `list_jobs` (`TAG_READ`). CLI mirrors `job list` / `_run_job_list`; `--like` maps to `name`. Renderer mirrors `render_jobs_page`.
- [ ] **Step 4:** run tests → PASS; `npm ci && npm run gen-types`; `uv run pytest aaiclick/server/test_dump_openapi.py` → PASS.
- [ ] **Step 5:** commit `feat: task list via REST, MCP and CLI`.

### Task 3: `enable` / `disable` prompt commands

**Files:**
- Modify: `src/prompt.ts`, `src/prompt.test.ts`, `src/components/commands.ts`, `src/App.tsx`
- Create: `src/views/ToggleConfirm.tsx`

**Interfaces:**
- Produces: `Route` members `{ kind: "toggle-confirm"; name: string; enabled: boolean }`.

- [ ] **Step 1: Failing tests** in `prompt.test.ts`: `parsePrompt("enable etl")` → `{ kind: "toggle-confirm", name: "etl", enabled: true }`; `"disable etl"` → `enabled: false`. `commands.test.ts` already guards new registry rows.
- [ ] **Step 2:** `npx vitest run src/prompt.test.ts` → FAIL.
- [ ] **Step 3: Implement.** `ToggleConfirm` mirrors `CancelConfirm`, calls `useToggleRegisteredJob`, toasts, then `onPrompt("@registered")`. Replace the "enable / disable <name>" pointer row with two rows: `{ code: "enable <name>", cmd: "@registered", complete: "enable " }` and the `disable` twin.
- [ ] **Step 4:** `npx vitest run` → PASS; `npx tsc --noEmit` clean.
- [ ] **Step 5:** commit `feat(ui): enable/disable prompt commands`.

### Task 4: Source table and dropdown

**Files:**
- Create: `src/components/promptSources.ts`, `src/components/promptSources.test.ts`, `src/components/PromptDropdown.tsx`
- Modify: `src/styles.css` (or the stylesheet Header uses)

**Interfaces:**
- Produces:
  ```ts
  interface SourceMatch { key: string; command: string; text: string; source: Source }
  interface Source {
    fetchPage(text: string, offset: number): Promise<{ items: SourceItem[]; more: boolean }>;
  }
  interface SourceItem { id: string; label: string; meta?: ReactNode; insert: string }
  function matchSource(draft: string): SourceMatch | null;
  function PromptDropdown(props: { draft: string; onPick(value: string): void; active: number; onActive(i: number): void }): JSX.Element | null;
  ```
- `matchSource` resolves the longest prefix in the spec's table: `@task `, `@job `, `cancel `, `run `, `enable `, `disable `, `@data job `, `@query job `, `@data job <ref> `, `@query job <ref> `, `@data `, `@query `, `@dashboard `, `@sandbox ` (only when sandbox is enabled). No prefix → command list from `useCompletions`.
- `insert` per the spec's row table (`cancel` inserts the job id, `@job` the name).
- `more` = `items.length === limit` for offset sources; `false` for single-page sources (objects, dashboards). Dashboards filter by `text` client-side.

- [ ] **Step 1: Failing tests** in `promptSources.test.ts`: table of `draft → { key, text }` covering every prefix above plus `"@task load"` → text `"load"`, `"@data job etl "` → objects of `etl`, `"@jobs"` → `null`; `fetchPage` for tasks builds `/tasks?name=load&limit=50&offset=50` (mock `fetchJSON`); short page → `more: false`.
- [ ] **Step 2:** `npx vitest run src/components/promptSources.test.ts` → FAIL.
- [ ] **Step 3: Implement.** `PromptDropdown` uses `useInfiniteQuery({ queryKey: [match.key, debouncedText], getNextPageParam })`, a bottom sentinel observed with `IntersectionObserver` calling `fetchNextPage` only when `hasNextPage && !isFetchingNextPage`, an empty state "no matches", rows ≥ 40 px tall, `role="listbox"` / `role="option"` / `aria-selected`. Task rows: name · job id · `StatusBadge` · `relativeTime(created_at)`.
- [ ] **Step 4:** `npx vitest run` → PASS; `npx tsc --noEmit` clean.
- [ ] **Step 5:** commit `feat(ui): prompt dropdown sources`.

### Task 5: Draft vs. commit in Header

**Files:**
- Modify: `src/components/Header.tsx`, `src/prompt.ts` (add `isCommitOnType`), `src/prompt.test.ts`, `docs/designs/ui.md`
- Test: `test_e2e/web/test_prompt_dropdown.py`

**Interfaces:**
- Consumes: `PromptDropdown`, `matchSource` (Task 4).
- Produces: `function isCommitOnType(draft: string): boolean` — true iff `draft` exactly equals an argument-less command (`complete` with no trailing space).

- [ ] **Step 1: Failing tests:** vitest — `isCommitOnType("@jobs") === true`, `("@task ") === false`, `("@jo") === false`. Playwright — type `@task ` → `#content` text unchanged and dropdown rows visible; scroll to bottom → row count grows (seed > 50 tasks); click a row → URL `?p=@task <id>` and task detail renders; Esc → input resets to committed prompt; Enter on `@foo` → "Unknown command"; Back after commit → input and view follow URL.
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement.** Header keeps `draft` state synced from `prompt` prop on change (covers Back). `onChange` sets draft; calls `onPrompt` only when `isCommitOnType`. ↑/↓ move `active`; Enter picks the active row or commits the draft; Esc resets. Remove `<datalist>`. Update `docs/designs/ui.md` Navigation section per the `markdown-style` skill.
- [ ] **Step 4:** `npx vitest run`, `npx tsc --noEmit`, `uv run pytest test_e2e/web/test_prompt_dropdown.py` → PASS.
- [ ] **Step 5:** commit `feat(ui): prompt waits for commit, dropdown wired in`.

### Task 6: Cleanup

- [ ] Delete this plan and the spec; fix references (CLAUDE.md "Remove superpowers plan and spec once implemented").
- [ ] Push; run `devpowers:check-pr`.
