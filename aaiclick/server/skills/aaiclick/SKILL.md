---
name: aaiclick
description: How to use the aaiclick MCP tools — jobs, tasks, workers, objects, the viewer, lineage triage and admin.
---

# Basics

Tools mirror aaiclick's CLI verbs (the lineage tools have none) and run against
the same backends as the REST surface under `/api/v0`, with the same pydantic
schemas.

- **Access**: each tool needs `read`, `write` or `admin`; `tools/list` shows
  only what the caller's token allows. A refused call is a tool error.
- **References**: `ref` takes a numeric ID or a name; a job name means its latest run.
- **Pages**: `list_*` tools return `items`, `total`, `next_cursor`. Pass
  `next_cursor` back as `filter.cursor` for the next page.

# Jobs

- `list_jobs` (filter by `status`: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`,
  `CANCELLED`; `name`; `since`), `get_job` for every task, `job_stats` for timings.
- `run_job` runs a job now and registers it if needed. Set
  `preservation_mode: "FULL"` to keep intermediate tables for lineage.
- There is no tool that waits for a job. Re-check `get_job` on your own schedule.
- `cancel_job` stops a job and its unfinished tasks.
- Catalog: `list_registered_jobs`, `register_job` (`entrypoint`, optional cron
  `schedule`, `default_kwargs`), `enable_job` / `disable_job`.

# Tasks

- `list_tasks` across jobs, newest first; `name` is a `LIKE` pattern (`%` only).
- `get_task` for detail, `get_task_logs` for the last `tail` lines of one
  `attempt` (latest by default). Read logs before guessing why a task failed.
- `clear_task` resets a task and everything downstream to `PENDING` for a re-run.

# Workers

`list_execution_workers` is read-only. Starting and stopping workers is not
exposed here: it belongs to the CLI and the deployment. If no worker is
`ACTIVE`, jobs stay `PENDING` — report it instead of retrying.

# Objects

- `list_objects` (filter by `prefix`, `scope`, or `job` for one run's objects),
  `get_object` for the schema.
- `query_object` reads a page: `object`, optional `fields`, `where` (SQL boolean
  over the object's columns), `order_by`, `limit`. `scope` is `"persistent"` or
  `"job:<id|name>"`.
- `delete_object` drops one global object; `purge_objects` drops global objects
  by `after` / `before` creation time (one is required). Both are irreversible.

# Viewer

- Saved queries: `list_saved_queries`, `save_query` (create or replace by
  `name`), `delete_saved_query`.
- Dashboards: `list_dashboards`, `get_dashboard`, `save_dashboard` (HTML plus
  named object queries), `delete_dashboard`, `run_dashboard` to run every panel.

# Lineage triage

For "why does table X look wrong?", use the lineage tools in this order:

1. `lineage_graph` — read each node's `sql_template` and form a hypothesis before querying.
2. `lineage_nodes` — a table with `live=false` cannot be queried: say so and stop,
   or re-run the job with `preservation_mode: "FULL"` (`run_job`) and retry.
3. `lineage_schema` — before querying any table; use only the column names it returns.
4. `lineage_query` — for evidence; cite the rows it returns. It accepts one
   read-only `SELECT` over tables in the graph: write a CTE as a subquery in
   `FROM`, and use `has(column, value)` rather than `IN` for an array column.

All four take `direction` (`"backward"` default, or `"forward"`) and `max_depth`.

# Admin

`setup` runs filesystem and SQL migrations; `migrate` runs an alembic
subcommand (`action`, optional `revision`). Run them only when asked.
