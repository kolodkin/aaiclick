Viewer: Objects, Query, and Dashboard Modes
---

Three prompt-driven SPA modes (`@data`, `@query`, `@dashboard`) and the
`internal_api` verbs behind them, exposed through REST, MCP, and CLI like every
other command. The rendering kernel comes from QueryView; everything else is
aaiclick's own. UX per mode: `docs/designs/ui.md`.

# QueryView Coupling

The only shared code is QueryView's frontend kernel, `frontend/src/core`
(React 19, Tailwind 4, `js-yaml`): result rows, cell views, complex-type
cells, query params, field pickers, and the dashboard sandbox. No Python
dependency, no runtime coupling — once aaiclick owns catalog, storage, auth,
and MCP, the kernel is all that is left to reuse.

## What is copied

`src/queryview-core/` is a verbatim copy of `frontend/src/core` at the commit
recorded in `src/queryview-core/README.md`, imported as `@qv/core` (tsconfig
`paths` + Vite and vitest `resolve.alias`); its `glass-*` classes are ported
to `src/styles/queryview-core.css`. The folder is never edited here: a change
goes to QueryView first (its lint rules keep the kernel free of routing,
fetching, and app state), then the copy is refreshed and `npm run check` /
`npm test` re-run.

## The contract the kernel expects

| Input             | Shape                                                                                                             | aaiclick source                                      |
|-------------------|-------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|
| result rows       | `{meta: [{name, type}], data: [[…]]}` — ClickHouse `JSONCompact`, 64-bit integers / decimals / denormals quoted, named tuples as objects | `POST /viewer/query` returns ClickHouse's output verbatim |
| column types      | `meta[].type`, ClickHouse type strings                                                                            | same response                                        |
| cell views        | raw YAML (`link` / `custom`, `params:` block); `{name}` params substitute into the `where` expression             | `viewer_queries.cell_view`, validated server-side    |
| presentation      | `order_by: [{name, dir}]`, `fields: [col]`                                                                        | `viewer_queries.order_by` / `fields`                 |
| dashboard results | `{query: {column: values}}` for `window.queries`                                                                  | `viewer.run_dashboard`                               |
| CSV download      | text from a separate request                                                                                      | `viewer.query_object(fmt="csv")`                     |

These are QueryView's `/api/db/query`, predefined-query, and `/api/runqueries`
shapes; keeping them identical is what lets the copy stay verbatim. The one
adapter, `src/lib/viewer.ts`, converts aaiclick's wire `OrderBy` pair
(`[name, dir]`) to the kernel's `OrderCol` and the object schema to `Field`.

# Scope Model

Objects are shown under a scope key — a string, so it fits the prompt, the
URL, saved queries, and CLI flags:

| Scope            | Tables listed                                    |
|------------------|--------------------------------------------------|
| `persistent`     | tenant's `p_*` rows in `table_registry`          |
| `job:<id\|name>` | `table_registry` rows with the resolved `job_id` |

A job scope resolves through `resolve_job` like `get_job` does: an id names
one run, a name the latest run with that name. Saved queries and dashboards
store the key as given, so `job:nightly_etl` follows each new run while
`job:123` stays pinned. Temp and oplog tables never appear.

**Implementation**: `aaiclick/viewer/scope.py` — see `parse_scope`;
`aaiclick/internal_api/objects.py` — see `open_scoped` (also behind
`get_object(name, job)` and `list_objects(ObjectFilter(job=…))`).

Every surface identifies an object as `(scope, name)` — the tenant is never one
of the two. Each resolves it once, outside the verb: REST from `X-Tenant-Id`
via `require_tenant`, the CLI from the global `--tenant <slug>` flag applied in
`_run_internal_api`, MCP from the default tenant (it has no tenant selector).
`make_scoped_table_name` then maps the pair to `p_<tenant_id>_<name>` or
`j_<job_id>_<name>`, with the job checked against the active tenant.

## Queries name an object, not a table

`ObjectQueryRequest` (`aaiclick/viewer/view_models.py`) carries a scope, an
object name, and constraints — never SQL with a `FROM`. `query_object`
resolves the object with `open_object(name, scope)` and builds the SELECT
with the Object API (`obj.view(...)` + `select_sql(columns=...)`), so the
table name is produced inside `aaiclick/data` like for every other consumer;
the SPA, MCP, and CLI never see one. The `where` expression is the one
free-text input: `validate_where_expression` (`aaiclick/data/sql_utils.py`,
shared with the lineage agent's SQL guards) rejects statement separators,
DDL/DML keywords, and `SELECT` / `FROM` / `JOIN` / `UNION` / `WITH`, so it has
no table position and cannot reach another object. Columns come from the
object's registered schema, so there is no `DESCRIBE` step. Free-form SQL
across objects is in `future.md`.

# Backend

The viewer builds on three additions to `aaiclick/data` and
`aaiclick/internal_api`:

- `list_objects(ObjectFilter(job=…))` and `get_object(name, job=…)`
  (`aaiclick/internal_api/objects.py`); `open_object(job_id=)` and
  `list_job_tables` (`aaiclick/data/data_context/data_context.py`) for callers
  outside a task.
- `Object.select_sql()` (`aaiclick/data/object/object.py`): the SELECT an
  object reads itself with; `View` and `LazyOperator` inherit it.
- `query_text(sql, fmt, settings)` (`aaiclick/data/data_context/ch_client.py`,
  over the clients' shared `raw_query`): ClickHouse's own output for a named
  format. `JSONCompact` with `JSON_COMPACT_SETTINGS` (the `output_format_json_*`
  quoting QueryView's driver sends) means the kernel renders `{meta, data}`
  unchanged and aaiclick formats no values; CSV uses `CSVWithNames`.

## Internal API

`aaiclick/internal_api/viewer.py`; every function runs under
`orch_context(with_ch=True)` and the active tenant, like `objects.py`.

| Function                                    | Returns                                | Notes                                                                                   |
|---------------------------------------------|----------------------------------------|-----------------------------------------------------------------------------------------|
| `query_object(ObjectQueryRequest)`          | `ObjectQueryResult`                    | `limit ≤ 1000`, `max_execution_time` 30 s; `fmt="json"` fills `meta` + `data`, `"csv"` fills `text` (MCP, CLI) |
| `query_object_text(ObjectQueryRequest)`     | `str`                                  | the same page as ClickHouse's own `JSONCompact` / `CSVWithNames` text — what REST returns verbatim (`application/json` / `text/csv`) |
| `list_saved_queries(SavedQueryFilter)`      | `Page[SavedQuery]`                     | by `scope` and `object`; a query saved with `scope=None` matches every scope            |
| `save_query(SavedQueryIn)`                  | `SavedQuery`                           | upsert on `(tenant, name)`; validates `where` and the `cell_view` YAML shape            |
| `delete_saved_query(name)`                  | `Deleted`                              |                                                                                         |
| `list_dashboards()` / `get_dashboard(name)` | `Page[DashboardSummary]` / `Dashboard` |                                                                                         |
| `save_dashboard(DashboardIn)`               | `Dashboard`                            | upsert on `(tenant, name)`; `queries` is `dict[panel, ObjectQuery]`                      |
| `delete_dashboard(name)`                    | `Deleted`                              |                                                                                         |
| `run_dashboard(name)`                       | `DashboardResults`                     | runs the panels concurrently under the dashboard's scope; column-oriented, the `window.queries` contract |

Models: `aaiclick/viewer/view_models.py`. `ObjectQuery` (object, fields,
where, order_by) is a dashboard panel; a saved query adds `name`, `scope`, and
`cell_view`; a request adds `scope`, paging, and `fmt`. Code-declared queries need no separate
mechanism: job code calls `save_query` through the same `internal_api`.

## Storage

`viewer_queries` and `viewer_dashboards` (`aaiclick/viewer/models.py`, unique
on `(tenant_id, name)`), migrated through the Alembic chain. `tenant_id` is a
plain `BigInteger` without FK, per `aaiclick/orchestration/models.py`.
`cell_view` is raw YAML and `order_by` / `fields` / `queries` are JSON text,
stored verbatim and interpreted by the kernel; the server validates shape,
and `where` with the rules above.

## Surfaces

- **REST** `aaiclick/server/routers/viewer.py`, prefix `/viewer`, the objects
  router's dependencies (`orch_scope_with_ch`, tenant): `POST /query` (raw
  ClickHouse text, no parse / re-serialise on the server),
  `GET|PUT|DELETE /queries[/{name}]`, `GET|PUT|DELETE /dashboards[/{name}]`,
  `POST /dashboards/{name}:run`. Any tenant member may read, run, and save;
  nothing here drops data.
- **MCP** `aaiclick/server/mcp.py` (viewer section): one tool per verb, each
  opening `orch_context(with_ch=True)`. The lineage-scoped `query_table`
  stays as is.
- **CLI** `data query <object> [--scope job:<ref>] [--where EXPR] [--fields
  a,b] [--order-by col:desc]`, `view queries list|save|delete`, `view
  dashboards list|get|save|delete|run` — `aaiclick/__main__.py`
  (`_run_data_query`, `_run_view_*`), `aaiclick/cli_renderers.py`.

# Frontend

Modes, prompts, and per-view implementation references: `docs/designs/ui.md`.
Hooks: `src/api/hooks.ts` (`useObjects`, `useObject`, `useQueryObject`,
`useObjectRows`, `useSavedQueries`, `useSaveQuery`, `useDeleteSavedQuery`,
`useDashboards`, `useDashboard`, `useRunDashboard`); routes:
`src/prompt.ts` (`parseScoped`). Object rows in `@data` come from the same
`query_object` with no constraints, so `@data` and `@query` share one hook
and one result shape. The Fields picker is fed from `useObject`'s schema;
only static `params:` options render (`future.md`).

Local mode (`python -m aaiclick local start`) runs the API and workers in one
process, so the viewer's ClickHouse access nests into the runtime context and
the chdb lock is not an issue; distributed mode runs it inside the API server
over clickhouse-connect. Nothing in the viewer is per-process.

# Testing

- `aaiclick/internal_api/test_viewer.py` (chdb): `query_object` over
  persistent and job objects — fields, `where`, ordering, paging, CSV, the
  `where` guard, unknown objects and jobs; saved-query and dashboard
  round-trips with tenant isolation; `run_dashboard` column orientation.
- `aaiclick/server/routers/test_viewer.py`: routes, 404 / 422 problems, 401
  without a token in distributed mode.
- Vitest (`npm test`): the kernel's own tests, `src/prompt.test.ts`,
  `src/lib/viewer.test.ts`.
- Playwright (`test_e2e/web/test_viewer.py`, seeded by `seed.py` before the
  server starts): `@data` previews the seeded object, `@query` runs and
  pages, `@dashboard` renders the saved dashboard.
