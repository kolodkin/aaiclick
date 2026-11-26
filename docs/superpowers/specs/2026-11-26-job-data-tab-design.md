Job Data Tab
---

# Intent

A job's data — its tables and its tasks' inputs and results — lives under
`@data job <ref>`, a page the `@job` page does not link to. Someone looking at
a job or a task cannot find its values. Make the job the only way in to job
data, and leave `@data` for objects that belong to no job.

# Goals

- A job's inputs, results and objects are one tap from `@job`.
- A task's own inputs and result are one tap from `@task`.
- `@data` lists persistent objects only.
- Links to `@data job …` keep working.

Non-goals: backend or API changes, paging task values past the current
1000-row page, input/result columns in the tasks table.

Anti-goal: two pages that both list a job's data.

# Approaches

| Approach                                                 | Trade-off                                                    |
|----------------------------------------------------------|--------------------------------------------------------------|
| **Data tab on `@job`, `@data` persistent-only** (picked) | Removes the duplicate; drops the cross-job "All" listing     |
| Keep `@data` as is, link to it from `@job`               | Smallest diff, but the same data stays reachable two ways    |
| Inputs/results as a section under the tasks table        | No new tab, but a long page on a phone and objects stay away |

Picked by the user: the job is the entry point, so the data sits on it.

# Prompts

| Prompt                         | Shows                                                        |
|--------------------------------|--------------------------------------------------------------|
| `@job <ref> data`              | The job's task values and objects                            |
| `@job <ref> data <name>`       | One job object (explorer) or one task value, by its address  |
| `@task <id> data`              | That task's inputs and result                                |
| `@data`, `@data persistent`    | Persistent objects                                           |
| `@data [persistent] <object>`  | One persistent object                                        |
| `@data job <ref> [<name>]`     | Routed to `@job <ref> data [<name>]`                         |

The parser maps the old form to the job route; the prompt text is left as
typed.

# Job Page

- A fifth chip, **Data**, after Table / Graph / Timings / Duration.
- Body: what `@data job <ref>` shows today, minus the scope tree — the
  job / tasks / objects / all chips, the "Showing N of M rows" line, and
  `ObjectsTable`. It queries by `job.id`, so a name resolves to the latest
  run like the rest of the page.
- A row opens `@job <id> data <name>`: `ObjectExplorer` for a table,
  `ValueView` for a task value. Both render under the job header with the
  Data chip active; their back chip returns to `@job <id> data`.
- The Objects dropdown is removed from the job and task pages.

# Task Page

- A **Log / Data** chip pair under the header; Log is the default.
- Data lists the rows of the job listing whose `task_id` is this task,
  filtered client-side. No chips, no objects.
- A row opens `@job <job_id> data <address>`.

# Data Page

- No scope tree and no "All scopes"; Scope and Job columns go.
- Bare `@data` now means persistent, so the Dashboard chip keeps working.

# Code

- **Delete**: `ScopeTree`, `ObjectsMenu`, the `ALL` scope and its label.
- **Move**: `ObjectExplorer` and `ValueView` out of `Data.tsx` into a shared
  component file, used by both the job and data pages.
- **Change**: `dataPrompt` emits `@job <id> data [<name>]` for a job scope;
  `JOB_VIEW_SUFFIX` and `JobViewMode` gain `data` with an optional name;
  `Route` for `task` gains a view.
- **Prompt dropdown**: the object completion for `@data job <ref> ` moves to
  `@job <ref> data `; the `@data job` job-name completion goes.

# Tests and Docs

- Update `prompt.test.ts`, `viewer.test.ts`, `promptSources.test.ts`.
- `test_e2e/web/test_viewer.py`: job-scope cases open `@job <id> data`; add
  one case for the task Data tab and one for an old `@data job` link.
- `docs/designs/ui.md`: rewrite Job Detail, Task Detail and Data;
  `docs/designs/viewer.md` where it names the `@data job` prompt.

# Left to the Builder

- Whether the Data chip shows a row count.
- Layout of the task Data list on a phone.
