# Prompt argument dropdown

## Intent

Typing a command that takes an argument (`@task `, `@job `, `run `, …) opens a dropdown of the real values to pick from — newest first, infinite scroll — so nobody has to know a snowflake id or a job name by heart. Built for the phone as much as the desktop.

## Goals

- One custom dropdown under the prompt, replacing the native `<datalist>`.
- With no command yet, it lists the commands (today's autocomplete).
- After `<command> `, it lists that command's values; text after the space narrows the list server-side where the API supports it.
- Scrolling to the bottom loads the next page.
- The view does **not** follow keystrokes while the dropdown is open (see "View switching").
- New `enable <name>` / `disable <name>` prompt commands.
- New task list — internal API, REST, MCP, CLI.

## Non-goals

- Fuzzy matching, recent-history ranking, keyboard shortcuts beyond ↑/↓/Enter/Esc.
- Dropdowns for admin/account commands (`@users`, `@tokens`, …) — they take no argument.
- A `@tasks` page. Tasks stay reachable via a job, or `@task <id>` with this dropdown.

## Approaches

- **A — custom dropdown + per-command source table (picked).** No new dependency, react-query `useInfiniteQuery` for paging, one table row per command.
- B — a combobox library (cmdk/downshift). Rejected: adds a dependency, paging logic is still ours.
- C — keep `<datalist>`, fill with page one. Rejected: no infinite scroll, poor on mobile.

## Command → source

| Prompt prefix              | Lists                         | Endpoint                          | Narrowing           | Paging |
|----------------------------|-------------------------------|-----------------------------------|---------------------|--------|
| `@task `                   | tasks, newest first           | `GET /tasks` (new)                | `name` contains     | offset |
| `@job `, `cancel `         | jobs, newest first            | `GET /jobs`                       | `name` LIKE `%x%`   | offset |
| `run `, `enable `, `disable ` | registered jobs            | `GET /registered-jobs`            | `name` LIKE `%x%`   | offset |
| `@data `, `@query `        | persistent objects            | `GET /objects`                    | `prefix`            | single page |
| `@data job `, `@query job ` | jobs                         | `GET /jobs`                       | as `@job`           | offset |
| `@dashboard `              | dashboards                    | `GET /viewer/dashboards`          | client-side contains | single page |
| `@sandbox `                | sandbox files (if enabled)    | `GET /sandbox`                    | none                | offset |

`@data job <ref> ` (object after a job scope) lists that job's objects (`GET /objects?job=<ref>`).

Row content and the value inserted on pick:

| Source          | Row shows                                    | Inserts            |
|-----------------|----------------------------------------------|--------------------|
| task            | name · job id · status badge · relative time | `@task <id>`       |
| job             | name · status badge · relative time          | `@job <name>` (id for `cancel`, so the exact run) |
| registered job  | name · enabled/disabled                      | `<cmd> <name>`     |
| object          | name                                         | `<cmd> … <name>`   |
| dashboard / sandbox file | name                                | `<cmd> <name>` / `@sandbox <id>` |

## View switching

- The prompt keeps a **draft** (input text) separate from the **committed** prompt (what `parsePrompt` routes on and what goes in the URL).
- Typing edits only the draft and opens the dropdown. The view stays put.
- Commit happens on: picking a row, Enter, or a draft that exactly equals an argument-less command (`@jobs`, `@registered`, `@data`, …).
- Esc closes the dropdown and resets the draft to the committed prompt.
- Clicks inside views call `onPrompt` and commit immediately, as today.

## Backend: task list

- `TaskListFilter(ApiModel)` in `aaiclick/orchestration/view_models.py` (next to `TaskView`): `name: str | None` (substring, matched with `LIKE %name%`), `job: RefId | None`, `status: TaskStatus | None`, `limit: PageLimit = 50`, `offset: PageOffset = 0`.
- `internal_api.tasks.list_tasks(filter) -> Page[TaskView]`, newest first by snowflake `id` desc (PK-indexed; `created_at` is not), via `paginate`.
- REST: `GET /tasks` on the existing tasks router, `filter: TaskListFilter = Depends()`.
- MCP: `list_tasks` tool beside `list_jobs` in `aaiclick/server/mcp.py`.
- CLI: `aaiclick task list [--like] [--job] [--status] [--limit] [--offset] [--json]`, rendered like `job list`.

## Prompt commands: enable / disable

- `parsePrompt`: `enable <name>` / `disable <name>` → routes that call the existing `useToggleRegisteredJob`, then land on `@registered`. Mirror `CancelConfirm`: show a confirm view, act on button press.
- `commands.ts`: replace the "enable / disable <name>" pointer row with two real rows with `complete: "enable "` / `"disable "`.

## Frontend shape

- `src/components/PromptDropdown.tsx` — the list, scroll sentinel (`IntersectionObserver`), ↑/↓/Enter/Esc, touch-friendly row height.
- `src/components/promptSources.ts` — the table above: prefix match → `{ queryKey, fetchPage(text, offset), row renderer, insert }`.
- `Header.tsx` owns draft state; `App` keeps owning the committed prompt.
- Debounce narrowing text ~200 ms before refetch.

## Testing

- Python: `list_tasks` ordering, name/job/status filters, paging; CLI `task list`; REST route.
- Vitest: source resolution per prefix, draft vs. commit rules.
- Playwright: type `@task `, see rows, scroll loads more, pick commits and routes.

## Left to the builder

- Exact styling and max dropdown height.
- Whether `cancel ` filters to running/pending jobs client-side.
