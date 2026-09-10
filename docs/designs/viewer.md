Viewer: Objects, Query, and Dashboard Modes
---

aaiclick has no UI for the rows of an Object. This design adds three
prompt-driven modes to the existing SPA (`@data`, `@query`, `@dashboard`) and
the `internal_api` verbs behind them, exposed through REST, MCP, and CLI like
every other command. The rendering kernel comes from QueryView; everything
else is aaiclick's own.

# QueryView Coupling

The only shared code is QueryView's frontend kernel, `frontend/src/core`
(React 19, Tailwind 4, `js-yaml`): result rows, cell views, complex-type
cells, query params, field pickers, and the dashboard sandbox. There is no
Python dependency and no runtime coupling. Earlier iterations explored
embedding QueryView as a Python plugin; once aaiclick owns catalog, storage,
auth, and MCP, the kernel is all that is left to reuse.

## What is copied

`src/queryview-core/` is a verbatim copy of `frontend/src/core` at the
QueryView commit recorded in `src/queryview-core/README.md`, imported through
the alias `@qv/core` (tsconfig `paths` + Vite and vitest `resolve.alias`); its
`glass-*` classes are ported to `src/styles/queryview-core.css`. The folder is never
edited in aaiclick: a needed change goes to QueryView first (its lint rules
keep the kernel free of routing, fetching, and app state), then the copy is
refreshed. Its own vitest tests run with the rest of the SPA.

## The contract the kernel expects

| Input                 | Shape                                                     | aaiclick source                                   |
|-----------------------|-----------------------------------------------------------|---------------------------------------------------|
| result rows           | `{meta: [{name, type}], data: [[…]]}` — ClickHouse `JSONCompact` with 64-bit integers, decimals, and denormals quoted and named tuples as objects | `viewer.query_object` passes ClickHouse's output through |
| column types          | `meta[].type`, ClickHouse type strings                    | same response                                     |
| cell views            | raw YAML (`link` / `custom`, `params:` block); `{name}` params substitute into the `where` expression | `viewer_queries.cell_view`, validated server-side |
| presentation          | `order_by: [{name, dir}]`, `fields: [col]`                | `viewer_queries.order_by` / `fields`              |
| dashboard results     | `{query: {column: values}}` for `window.queries`          | `viewer.run_dashboard`                            |
| CSV download          | text from a separate request                              | `viewer.query_object(fmt="csv")`                  |

These are QueryView's `/api/db/query`, predefined-query, and `/api/runqueries`
shapes; keeping them identical is what lets the copy stay verbatim.

## Keeping in sync

Bump the copy when QueryView's `core/index.ts` surface changes: copy the
folder, update the commit in `README.md`, run `npm run check` and vitest.
Behavioural changes to the kernel (new cell-view types, new complex-type
rules) land in QueryView with its e2e coverage before aaiclick picks them up.

# Scope Model

Objects are shown under a scope. Scope keys are strings so they fit in the
prompt, the URL, saved queries, and CLI flags.

| Scope             | Tables listed                                   | Query allowlist                            |
|-------------------|-------------------------------------------------|--------------------------------------------|
| `persistent`      | tenant's `p_*` rows in `table_registry`         | the persistent tables                      |
| `job:<id\|name>`  | `table_registry` rows with the resolved `job_id` | that job's tables plus the persistent ones |

`ScopeRef` (`aaiclick/viewer/scope.py`) parses and formats the key:
`kind: Literal["persistent", "job"]`, `job: RefId | None`. A job scope resolves
through `resolve_job` like `get_job` does: an id names one run, a name the
latest run with that name. Saved queries and dashboards store the key as
given, so `job:nightly_etl` follows each new run while `job:123` stays
pinned; responses carry the resolved id for display (`nightly_etl #123`).
Temp tables and the oplog internal tables never appear. Display names come
from `name_from_table` in `aaiclick/data/scope.py`; row counts, sizes, and
creation times from `_fetch_table_metadata` in
`aaiclick/internal_api/objects.py`.

**Implementation**: `aaiclick/viewer/scope.py` (`parse_scope`, `scope_key`);
`aaiclick/internal_api/viewer.py` (`resolve_scope`, `open_scoped`)

## Identity and tenancy

Every surface identifies an object as `(scope, name)`; the tenant is never a
parameter. REST resolves it from `X-Tenant-Id` through `require_tenant`,
which pins `active_tenant`; MCP and CLI act on the default tenant; inside
`internal_api` everything reads `get_active_tenant_id()`. The API maps the
pair to a table with `make_scoped_table_name`, so a persistent `orders` is
`p_orders` for the default tenant and `p_7_orders` otherwise, and a job
object is `j_<job_id>_<name>` with the job checked against the active
tenant. Callers never pass or see a tenant id.

## Queries name an object, not a table

The query API takes an object and constraints, never SQL with a `FROM`:

```python
class ObjectQueryRequest(BaseModel):
    scope: str                       # "persistent" | "job:<id|name>"
    object: str
    fields: list[str] | None = None  # None = every column
    where: str | None = None         # SQL boolean expression over the object's columns
    order_by: list[OrderBy] = []
    limit: int = 100                 # ≤ 1000
    offset: int = 0
    fmt: Literal["json", "csv"] = "json"
```

`query_object` resolves the object with `open_object(name, scope)` and builds
the query with the Object API — `obj.view(where=…, order_by=…, limit=…,
offset=…)` plus the field selection — so the SELECT and its table name are
produced inside `aaiclick/data`, the same way every other consumer queries an
Object. The viewer, the SPA, MCP, and the CLI never see a table name. The
`where` expression is the one free-text input: `validate_where_expression`
(`aaiclick/ai/agents/lineage_tools.py`) rejects statement separators, DDL/DML
keywords, and the subquery keywords `SELECT`, `FROM`, `JOIN`, `UNION`, `WITH`,
so the expression has no table position and cannot reach another object. An
unknown object is `NotFound`, as in `get_object`.

Columns come from the object's registered schema (`get_object`), so there is
no `DESCRIBE` step. Free-form SQL across several objects is in `future.md`.

# Backend

## Prerequisites

Landed in `aaiclick/data` and `aaiclick/internal_api`; the viewer builds on:

- `ObjectFilter.job` and `list_objects(scope="job")`
  (`aaiclick/internal_api/objects.py`), resolving the job through
  `jobs_api.resolve_job`; `open_object(job_id=)` and `list_job_tables`
  (`aaiclick/data/data_context/data_context.py`) for callers outside a task.
- `Object.select_sql()` (`aaiclick/data/object/object.py`): the SELECT an
  object reads itself with; `View` and `LazyOperator` inherit it.
- `query_text(sql, fmt, settings)` (`aaiclick/data/data_context/ch_client.py`):
  ClickHouse's own output for a named format on chdb and clickhouse-connect.
  Results use `JSONCompact` with the `output_format_json_*` quoting settings
  QueryView's driver sends (`JSON_SETTINGS` in `aaiclick/internal_api/viewer.py`),
  so the kernel renders `{meta, data}` unchanged and aaiclick formats no
  values. CSV uses `CSVWithNames`.

## Internal API

`aaiclick/internal_api/viewer.py`. Every function runs under
`orch_context(with_ch=True)` and the active tenant, like `objects.py`.

**Implementation**: `aaiclick/internal_api/viewer.py` (`query_object`,
`save_query`, `run_dashboard`)

| Function                                     | Returns             | Notes                                                                 |
|----------------------------------------------|---------------------|-----------------------------------------------------------------------|
| `query_object(ObjectQueryRequest)`           | `ObjectQueryResult` | validate `where`, then `open_object(...).view(...)` with `fields`, `limit ≤ 1000`, `max_execution_time` 30 s; `fmt="json"` returns `meta` + `data`, `fmt="csv"` returns `text` |
| `list_saved_queries(SavedQueryFilter)`       | `Page[SavedQuery]`  | filter by `scope` and `object`; a saved query with `scope=None` matches every scope |
| `save_query(SavedQueryIn)`                   | `SavedQuery`        | upsert on `(tenant, name)`; validates `where`, `cell_view` YAML shape, and `order_by` / `fields` |
| `delete_saved_query(name)`                   | `Deleted`           |                                                                       |
| `list_dashboards()` / `get_dashboard(name)`  | `Page[DashboardSummary]` / `Dashboard` |                                                    |
| `save_dashboard(DashboardIn)`                | `Dashboard`         | upsert on `(tenant, name)`; `queries` is `dict[panel, ObjectQuery]`   |
| `delete_dashboard(name)`                     | `Deleted`           |                                                                       |
| `run_dashboard(name)`                        | `DashboardResults`  | runs each panel's object query under the dashboard's scope; column-oriented `{query: {column: values}}`, the `window.queries` contract of the kernel's `DashboardFrame` |

Request and response models live in `aaiclick/viewer/view_models.py`.
`ObjectQueryResult`: `meta: list[ColumnSchema]` and `data: list[list[Any]]`
for JSON, `text` for CSV. `OrderBy` is a `NamedTuple(name, dir)`. A saved
query is an `ObjectQueryRequest` minus paging, plus `name` and `cell_view`;
a dashboard panel is the same minus `cell_view`.

Code-declared queries need no separate mechanism: job code calls
`viewer_api.save_query` through the same `internal_api`, so a registered job
can install its default queries when it runs.

## Storage

Two SQLModel tables in `aaiclick/viewer/models.py`, migrated through the
existing Alembic chain (`generate-migration` skill).

**Implementation**: `aaiclick/viewer/models.py` (`SavedQueryRow`, `DashboardRow`)

| Table               | Columns                                                                                                 | Unique              |
|---------------------|---------------------------------------------------------------------------------------------------------|---------------------|
| `viewer_queries`    | `id`, `tenant_id`, `name`, `scope`, `object`, `where`, `fields`, `order_by`, `cell_view`, `updated_at` | `(tenant_id, name)` |
| `viewer_dashboards` | `id`, `tenant_id`, `name`, `scope`, `html`, `queries`, `updated_at`                                     | `(tenant_id, name)` |

`tenant_id` is a plain `BigInteger` without FK, per
`aaiclick/orchestration/models.py`. `cell_view` is raw YAML and `order_by` /
`fields` / `queries` are JSON text, stored verbatim and interpreted by the
kernel in the browser; the server validates shape, and `where` with the
expression rules above.

## Surfaces

- **REST** `aaiclick/server/routers/viewer.py`, prefix `/viewer`, with the
  objects router's dependencies (`orch_scope_with_ch`, tenant): `POST /query`,
  `GET|PUT|DELETE /queries[/{name}]`, `GET|PUT|DELETE /dashboards[/{name}]`,
  `POST /dashboards/{name}:run`. Any tenant member may
  read, run, and save; nothing here drops data.
- **MCP** `aaiclick/server/mcp.py`: `query_object`, `list_saved_queries`,
  `save_query`, `delete_saved_query`, `list_dashboards`, `get_dashboard`,
  `save_dashboard`, `delete_dashboard`, `run_dashboard`, each opening
  `orch_context(with_ch=True)` like the existing tools. The lineage-scoped
  `query_table` stays as is.
- **CLI** `python -m aaiclick data query <object> [--scope job:<ref>]
  [--where EXPR] [--fields a,b] [--order-by col:desc] [--limit N]`, `view
  queries list|save|delete`, `view dashboards list|get|save|delete|run`,
  rendered from the same view models.

**Implementation**: `aaiclick/server/routers/viewer.py`; `aaiclick/server/mcp.py`
(viewer section); `aaiclick/__main__.py` (`_run_data_query`, `_run_view_*`) and
`aaiclick/cli_renderers.py` (`render_query_result`, `render_saved_query`, …)

# Frontend

## Modes

Prompt forms follow `docs/designs/ui.md`; `src/prompt.ts` gains the routes.

| Prompt                              | Mode      | Content                                                                              |
|-------------------------------------|-----------|--------------------------------------------------------------------------------------|
| `@data`                             | objects   | scope tree (Persistent, Jobs with search) and the objects of the selected scope      |
| `@data job <ref>`                   | objects   | same, with the job scope selected                                                    |
| `@data [job <ref>] <object>`        | objects   | object header (scope, job, rows, size, created) and its first page of rows           |
| `@query [job <ref>] [<object>]`     | query     | object picker, `where` expression, limit/offset, Fields and Order by, cell-view modal, params, saved-query dropdown, CSV download |
| `@dashboard [name]`                 | dashboard | dashboard picker, Save, sandboxed frame                                              |

Clicking an object in `@data` sets the prompt to `@data … <object>`; its
"Query" button sets `@query [job <ref>] <object>`. Object rows in `@data`
come from the same `query_object` with no constraints, so both modes share
one hook and one result shape.

**Implementation**: `src/views/Data.tsx`, `src/views/Query.tsx`,
`src/views/Dashboard.tsx`; `src/components/ScopeTree.tsx`,
`src/components/ObjectsTable.tsx`, `src/components/QueryPanel.tsx`;
`src/api/hooks.ts` (`useObjects`, `useObject`, `useQueryObject`,
`useSavedQueries`, `useSaveQuery`, `useDeleteSavedQuery`, `useDashboards`,
`useDashboard`, `useRunDashboard`, `useSaveDashboard`); `src/prompt.ts`
(`parseScoped`).

The wire format is aaiclick's: `OrderBy` is a `[name, dir]` pair and the
result is `ObjectQueryResult`; `src/lib/viewer.ts` (`orderColsToPairs`,
`pairsToOrderCols`, `rowsFromResult`, `fieldsFromSchema`) converts to the
kernel's `OrderCol` / `QueryRows` / `Field`. The Fields picker is fed from
`useObject`'s schema (persistent scope; job-scoped objects show every column
— `future.md`). Only static `params:` options are supported — `options_sql`
needs free SQL (`future.md`). Job nodes in the scope tree reuse `useJobs`
with the name filter.

# Local and Distributed Modes

Local (`python -m aaiclick local start`) runs the API and workers in one
process, so the viewer's ClickHouse access nests into the runtime context
and the chdb lock is not an issue. Distributed runs it inside the API server
process over clickhouse-connect; nothing in the viewer is per-process.

# Testing

- `aaiclick/internal_api/test_viewer.py` (chdb): persistent and job objects
  created through the Object API; `query_object` selects fields, filters,
  orders, and pages, rejects a `where` with DDL, a statement separator, or a
  raw table name, reports an unknown object, and refuses another job's
  object; saved query and dashboard round-trips with tenant isolation;
  `run_dashboard` column orientation.
- `aaiclick/server/routers/test_viewer.py`: routes, 404 / 422 problems, 401
  without a token in distributed mode.
- Vitest (`npm test`): the kernel's own tests, `src/prompt.test.ts` for the
  new routes, `src/lib/viewer.test.ts` for the adapters.
- Playwright (`test_e2e/web/test_viewer.py`, seeded by `seed.py` before the
  server starts): `@data` previews the seeded object, `@query` runs and
  pages, `@dashboard` renders the saved dashboard.

# Rollout

1. Backend: `ObjectFilter.job`, job-scope listing, `Object.select_sql`,
   `query_text`, `viewer` models and migration, `internal_api/viewer.py`,
   REST, MCP, CLI — landed; see the implementation references above.
2. Frontend: kernel copy and alias, `@data`, `@query`, `@dashboard` —
   landed; see the implementation references above and `ui.md`.
