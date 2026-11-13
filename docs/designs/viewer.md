Viewer: Object and Dashboard Modes
---

Two prompt-driven SPA modes (`@data`, `@dashboard`) and the
`internal_api` verbs behind them, exposed through REST, MCP and CLI like every
other command. The rendering kernel comes from QueryView; everything else is
aaiclick's own. UX per mode: `docs/designs/ui.md`.

# QueryView Coupling

The only shared code is QueryView's frontend kernel, `frontend/src/core`
(React 19, Tailwind 4, `js-yaml`): result rows, cell views, complex-type
cells, field pickers, and the dashboard sandbox with its params. No Python
dependency, no runtime coupling — once aaiclick owns catalog, storage, auth
and MCP, the kernel is all that is left to reuse.

## What is copied

`src/queryview-core/` is a verbatim copy of `frontend/src/core` at the commit
recorded in its `README.md`, imported as `@qv/core` (tsconfig `paths` + Vite
and vitest `resolve.alias`); its `glass-*` classes are ported to
`src/styles/queryview-core.css`. The folder is never edited here: a change
goes to QueryView first (its lint rules keep the kernel free of routing,
fetching and app state), then the copy is refreshed and `npm run check` /
`npm test` re-run.

## The contract the kernel expects

| Input             | Shape                                                                                                                                    | aaiclick source                                           |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------|
| result rows       | `{meta: [{name, type}], data: [[…]]}` — ClickHouse `JSONCompact`, 64-bit integers / decimals / denormals quoted, named tuples as objects | `POST /viewer/query` returns ClickHouse's output verbatim |
| column types      | `meta[].type`, ClickHouse type strings                                                                                                   | same response                                             |
| presentation      | `order_by: [{name, dir}]`, `fields: [col]`                                                                                               | the object view's pickers; `order_by` goes to the server  |
| dashboard results | `{query: {column: values}}` for `window.queries`                                                                                         | `run_queries`                                             |
| dashboard params  | `[{name, kind, options, default}]`; `{name}` placeholders substitute into a string per panel                                             | `viewer_dashboards.params`, each panel's `where`          |

These are QueryView's `/api/db/query`, dashboard and `/api/runqueries`
shapes; keeping them identical is what lets the copy stay verbatim. The one
adapter, `src/lib/viewer.ts`, converts aaiclick's wire order-by pair
(`[name, dir]`) and object schema to the kernel's types.

# Scope Model

**Implementation**: `aaiclick/viewer/scope.py`

Objects are shown under a scope key — a string, so it fits the prompt, the
URL, dashboards and CLI flags:

| Scope            | Tables listed                                    |
|------------------|--------------------------------------------------|
| `all`            | `persistent` plus every job's `j_*` rows         |
| `persistent`     | `p_*` rows in `table_registry`                   |
| `job:<id\|name>` | `table_registry` rows with the resolved `job_id` |

A job scope resolves like `get_job`: an id names one run, a name the latest
run with that name. Dashboards store the key as given, so
`job:nightly_etl` follows each new run while `job:123` stays pinned. Temp and
oplog tables never appear. `all` is a listing scope only
(`GET /objects?scope=all`); each item carries its own `job_id` / `job_name`,
and opening one switches to that item's scope.

Every surface identifies an object as `(scope, name)`, which maps to
`p_<name>` or `j_<job_id>_<name>`.

## Queries name an object, not a table

A query request carries a scope, an object name and constraints — never SQL
with a `FROM`. `query_object` opens the object and builds the SELECT with the
Object API, so the table name is produced inside `aaiclick/data` like for
every other consumer; the SPA, MCP and CLI never see one.

The `where` expression is the one free-text input. Its guard
(`aaiclick/data/sql_utils.py`, shared with the lineage sandbox) rejects
comments, unbalanced parentheses, statement separators, DDL/DML keywords and
`SELECT` / `FROM` / `JOIN` / `UNION` / `WITH`, so it cannot leave its
`WHERE (...)`. The built SELECT then passes the lineage sandbox's scope check
against the object's own table (catching `x IN other_table` and table
functions) and runs with `readonly=2`. Columns come from the object's
registered schema, so there is no `DESCRIBE` step. Free-form SQL across
objects: `future.md`.

# Backend

The viewer builds on three additions to `aaiclick/data` and
`aaiclick/internal_api`:

- Object listing and lookup filtered by job, and opening a job's object from
  outside a task.
- `Object.select_sql()`: the SELECT an object reads itself with; `View` and
  `LazyOperator` inherit it.
- A raw-bytes query on the ClickHouse client returning ClickHouse's own
  output for a named format. `JSONCompact` with the `output_format_json_*`
  quoting QueryView's driver sends means the kernel renders `{meta, data}`
  unchanged and aaiclick formats no values; CSV uses `CSVWithNames`.

## Internal API

**Implementation**: `aaiclick/internal_api/viewer.py`

Every function runs under `orch_context()`. Models:
`aaiclick/viewer/view_models.py`.

| Function                                    | Returns                                | Notes                                                                                                                 |
|---------------------------------------------|----------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| `query_object(ObjectQueryRequest)`          | `ObjectQueryResult`                    | `limit ≤ 1000`, `max_execution_time` 30 s; `fmt="json"` fills `meta` + `data`, `"csv"` fills `text` (MCP, CLI)        |
| `query_object_bytes(ObjectQueryRequest)`    | `bytes`                                | the page as ClickHouse sent it — what REST returns verbatim (`application/json` / `text/csv`), no decode or re-encode |
| `list_dashboards()` / `get_dashboard(name)` | `Page[DashboardSummary]` / `Dashboard` |                                                                                                                       |
| `save_dashboard(DashboardIn)`               | `Dashboard`                            | upsert on `name`; `queries` is `dict[panel, ObjectQuery]`, `params` the selector specs                                |
| `delete_dashboard(name)`                    | `Deleted`                              |                                                                                                                       |
| `run_queries(RunQueriesRequest)`            | `DashboardResults`                     | runs the panels concurrently under one scope; column-oriented, the `window.queries` contract                          |
| `run_dashboard(name)`                       | `DashboardResults`                     | `run_queries` over the dashboard's panels as stored — no param substitution                                           |

`ObjectQuery` (object, fields, where, order_by) is a dashboard panel; a
request adds `scope`, paging and `fmt`.

## Dashboard params

A dashboard's `params` are QueryView's selector specs (`name`, `kind`,
`options`, `default: none`), stored verbatim and interpreted by the kernel in
the browser. A `{name}` placeholder in a panel's `where` takes the chosen
value: a quoted literal, a backtick-quoted identifier for `kind: identifier`.
The SPA substitutes, then sends the panels to `run_queries`, so the result
passes the same `where` guard as any query; a panel whose selector is still
unchosen is held back. `options_sql`: `future.md`.

## Storage

**Implementation**: `aaiclick/viewer/models.py`

`viewer_dashboards`, unique on `name`, migrated through the Alembic chain.
`queries` and `params` are JSON text, stored verbatim; the server validates
each panel's `where` with the rules above, the kernel interprets `params`.

## Surfaces

- **REST** (`/viewer` prefix, the objects router's dependencies):
  `POST /query` (raw ClickHouse text, no parse / re-serialise on the server),
  `GET|PUT|DELETE /dashboards[/{name}]`, `POST /dashboards/{name}:run`,
  `POST /runqueries`. Any user may read and run; members and
  admins save; nothing here drops data.
- **MCP**: one tool per verb, each opening `orch_context()`. The
  lineage-scoped `lineage_query` stays as is.
- **CLI**: `data query <object> [--scope job:<ref>] [--where EXPR] [--fields
  a,b] [--order-by col:desc]`, `view dashboards list|get|save|delete|run`.

# Frontend

The object view in `@data` is QueryView's explorer: `query_object` with an
`order_by` and a page, the Fields picker fed from the object's schema.
`@dashboard` resolves the dashboard's params, substitutes them and answers
the frame's `setParams` messages. UX: `ui.md`.

Local mode (`python -m aaiclick local start`) runs the API and workers in one
process, so the viewer's ClickHouse access nests into the runtime context and
the chdb lock is not an issue; distributed mode runs it inside the API server
over clickhouse-connect. Nothing in the viewer is per-process.

# Testing

- chdb: `query_object` over persistent and job objects — fields, `where`,
  ordering, paging, CSV, the `where` guard, unknown objects and jobs;
  dashboard round-trips; `run_dashboard` / `run_queries` column orientation.
- Routers: routes, 404 / 422 problems, 401 without a token in distributed
  mode.
- Vitest (`npm test`): the kernel's own tests, prompt parsing, the adapter.
- Playwright (seeded before the server starts): `@data` lists, orders and
  pages the seeded object, `@dashboard` renders the saved dashboard and
  re-runs it through `setParams`.
