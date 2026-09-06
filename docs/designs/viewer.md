Viewer: QueryView Plugin over Objects
---

aaiclick has no UI for the rows of an Object. QueryView provides one
(explorer, query page, dashboards, MCP push tools) as an embeddable plugin,
`queryview-plugin`, whose views run over host-defined backends. This doc
specifies aaiclick's side: the `viewer` extra, the backends that map QueryView
scopes onto persistent and job-scoped Objects, tenant-aware storage for saved
queries and dashboards, and auth.

The plugin contract is specified in the QueryView repo
(`docs/superpowers/specs/2026-09-06-queryview-plugin-design.md`). Everything
below implements that contract.

# Packaging and Wiring

- New extra `viewer = ["queryview-plugin"]`, folded into `all`.
- New package `aaiclick/viewer/` with `views.py` (the backends), `store.py`
  (`ViewStore` over SQL), `models.py` (SQLModel tables), `mount.py` (FastAPI
  and MCP wiring).
- `aaiclick/server/app.py` includes the viewer when the plugin is importable:

```python
views = build_views()                      # aaiclick/viewer/mount.py
app.include_router(
    build_router(views, api_dependencies=[Depends(require_tenant)]),
    prefix="/viewer",
)
```

- `aaiclick/server/mcp.py` contributes the plugin's tools into the existing
  `/mcp` server, so there is one MCP endpoint:

```python
register_tools(mcp, views)
```

Backends open their own `orch_context(with_ch=True)` per call (see
[Backends](#backends)), so neither binding site passes a context.

The SPA gets a `@data` prompt mode that opens `/viewer/` in a new tab with the
auth handoff described under [Auth](#auth). No data UI is built in aaiclick's
own SPA.

# Scope Model

QueryView shows a scope tree; aaiclick translates scope keys and table keys to
ClickHouse table names with the helpers in `aaiclick/data/scope.py`. Keys are
opaque to the plugin.

| Scope key    | Children                              | Tables listed                                       | Table key      | Display name           |
|--------------|---------------------------------------|-----------------------------------------------------|----------------|------------------------|
| `persistent` | none                                  | tenant's `p_*` rows in `table_registry`             | `p_7_orders`   | `orders`               |
| `jobs`       | `job:<id>`, paged, searchable by name | none                                                |                | `<job.name> #<id>`     |
| `job:<id>`   | none                                  | `table_registry` rows with that `job_id`            | `j_42_result`  | `result`               |

Temp tables (`t_*`) and the oplog internal tables never appear. Display names
come from `name_from_table`; row counts, sizes, and creation times come from
`_fetch_table_metadata` in `aaiclick/internal_api/objects.py`.

SQL in the query page uses real table names. The browse query is generated
(`SELECT * FROM j_42_result`), so users rarely type them. Rewriting bare
object names in user SQL is deferred (see `future.md`) because a column and an
object can share a name.

# Backends

`aaiclick/viewer/views.py` is a fourth renderer over `internal_api`, like the
CLI, REST, and MCP. It never reads `table_registry` directly. Every method is
wrapped with `@in_orch_context` (`aaiclick/viewer/context.py`), which opens
`orch_context(with_ch=True)` around the call: under the REST router it nests
into the request's context, under an MCP tool it is the only context, exactly
as the tools in `aaiclick/server/mcp.py` do today.

```python
class ObjectExplorer:                                   # ExplorerBackend
    @in_orch_context
    async def session(self, ctx) -> SessionInfo:
        return SessionInfo(connected=True, name="aaiclick", type="aaiclick",
                           noun="objects", default_scope="persistent")

    async def list_scopes(self, ctx, parent, search, limit) -> list[Scope]:
        if parent is None:
            return [Scope("persistent", "Persistent", False, True),
                    Scope("jobs", "Jobs", True, False)]
        if parent == "jobs":
            page = await jobs_api.list_jobs(JobListFilter(name=search, limit=limit))
            return [Scope(f"job:{j.id}", f"{j.name} #{j.id}", False, True) for j in page.items]
        raise NotFound(parent)

    async def list_tables(self, ctx, scope) -> list[TableEntry]:
        page = await objects_api.list_objects(object_filter_for(scope))
        return [table_entry(o, self._queries_for(o)) for o in page.items]


class ObjectQuery(StoreViews):                          # QueryBackend
    ident_quote = "`"

    async def run_query(self, ctx, scope, sql, limit, offset, order_by, fmt) -> QueryResult:
        scan = normalize_sql_for_scan(sql)
        if err := validate_select_safety(sql, scan=scan):
            raise ViewError(400, err.message)
        allowed = await tables_in(scope) | await tables_in("persistent")
        if err := validate_scope(sql, allowed, scan=scan):
            raise ViewError(400, err.message)
        paged = wrap_paginated(sql, limit, offset, build_order_by(order_by, "`"))
        result = await get_ch_client().query(paged, settings={"max_execution_time": 30})
        return serialize_rows(result.column_names, result.result_rows, fmt)

    async def describe(self, ctx, scope, sql) -> list[Column]:
        ...  # DESCRIBE (sql) through the same safety and scope checks
```

The allowlist for a job scope is that job's tables plus the tenant's persistent
tables, so a job query can join a persistent Object. `validate_select_safety`,
`validate_scope`, and `normalize_sql_for_scan` are the existing functions in
`aaiclick/ai/agents/lineage_tools.py`.

`ObjectDashboards` implements `DashboardBackend` by inheriting the store half
from `StoreViews` and running `run_queries` through `ObjectQuery.run_query`
with a tenant-wide allowlist (persistent plus every job table the tenant
owns); the `connection` field is always `aaiclick`.

**Declared queries** come from a pure hook passed at construction,
`build_views(queries_for: Callable[[ObjectView], list[SavedQuery]])`, so
application code can attach default queries to objects by name or job without
a global registry. Persisting declarations next to `schema_doc` is listed in
`future.md`.

## Prerequisites in `internal_api`

- `objects_api.list_objects` accepts `scope="job"` with `job_id`, returning
  `j_<job_id>_*` rows for jobs the active tenant owns. Today it rejects every
  scope but global.
- `ObjectFilter` gains `job_id: int | None`.
- `jobs_api.list_jobs` accepts a name substring filter for the scope search.

These land as ordinary CLI/REST/MCP improvements before the viewer uses them.

# Storage

Saved queries and dashboards are aaiclick data, stored in aaiclick's SQL
database (SQLite locally, Postgres distributed) and migrated through
aaiclick's own Alembic chain. `aaiclick/viewer/store.py` implements
`ViewStore` over two tables in `aaiclick/viewer/models.py`:

| Table               | Columns                                                                                                   | Unique                          |
|---------------------|-----------------------------------------------------------------------------------------------------------|---------------------------------|
| `viewer_queries`    | `id`, `tenant_id`, `type`, `name`, `sql`, `scope_key`, `table_key`, `cell_view`, `order_by`, `fields`     | `(tenant_id, type, name)`       |
| `viewer_dashboards` | `id`, `tenant_id`, `name`, `connection`, `html`, `queries`, `updated_at`                                  | `(tenant_id, name)`             |

`tenant_id` follows the `BigInteger`-without-FK convention of
`aaiclick/orchestration/models.py`. `StoreContext` is resolved per request as
`StoreContext(tenant_id=get_active_tenant_id(), workspace_id=0)`. Workspaces
and git sync are off in v1 (`Views(workspaces=None, git=None)`); YAML export
and import come from `StoreViews` and stay on.

Migrations are generated with the `generate-migration` skill after adding the
models module import to `aaiclick/orchestration/migrations/env.py`.

# Auth

The plugin router carries no auth. aaiclick attaches `require_tenant`, which
resolves the Bearer token and `X-Tenant-Id` and pins the tenancy ContextVar,
so every `internal_api` call and `get_ch_client()` inside the backends is
tenant-scoped.

Any tenant member can browse, run, and save; nothing in the viewer deletes
data. Static assets need no credentials because `api_dependencies` apply to
`/api/*` only.

**Browser handoff.** aaiclick auth is Bearer-only and lives in the SPA's
storage, so the `@data` link opens:

```
/viewer/#headers={"Authorization":"Bearer <jwt>","X-Tenant-Id":"7"}
```

The plugin SPA stores those headers in `sessionStorage`, strips the fragment,
and sends them on every API call. Fragments never reach the server or logs.
The viewer tab does not refresh tokens; on 401 it tells the user to reopen it
from aaiclick. Minting a longer-lived read-only viewer token is in `future.md`.

**MCP.** Tools run behind the existing `AdminAuthMiddleware` on `/mcp` and,
like aaiclick's own tools, act on the default tenant.

# Local and Distributed Modes

- Local (`python -m aaiclick local start`): the API server, workers, and the
  viewer share one process, so the chdb session lock is not an issue; the
  backends' `orch_context` nests into the runtime's outer context.
- Distributed: the API server process opens ClickHouse over
  `clickhouse-connect`; the viewer runs inside the same uvicorn process as the
  REST API. Remote-push channels are per-process, so live push works only
  against the instance that served the browser.

# Testing

- `aaiclick/viewer/test_store.py`: run the plugin's contract kit
  (`queryview_plugin.testing`) against `SqlViewStore` on SQLite, plus tenant
  isolation.
- `aaiclick/viewer/test_views.py` (chdb): create a persistent and a job-scoped
  object, assert the scope tree, table entries, allowlist rejection of a
  foreign table and of DDL, and a paged `run_query`.
- `aaiclick/server/test_viewer_api.py`: router mounted with auth, 401 without
  a token, tenant-scoped listing, static index served with `<base
  href="/viewer/">`.

# Rollout

1. `internal_api` prerequisites (job-scope listing, job name filter).
2. `aaiclick/viewer/` models, store, migration, backends, unit tests.
3. App and MCP wiring, `@data` link, API tests.
4. Declared-queries hook and docs.
