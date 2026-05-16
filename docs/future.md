Future Plans
---

Planned work across aaiclick, ordered by priority.

---

# High Priority

## Progressive Tutorial

7-page tutorial using named snippets (`pymdownx.snippets` section markers) from existing
example files — 6 of 7 pages need zero new code. Pages: Your First Object, Operations,
Aggregations, Multi-Column Data, Views & Filters, Persistence, Orchestration. Add
`# --8<-- [start:name]` / `# --8<-- [end:name]` markers to example `.py` files, then
include specific sections in tutorial `.md` pages via snippet syntax.

Add "See Also" footers and cross-page links alongside the tutorial.

---

# Medium Priority

## Elide Materialization for Small / Scalar Results

> **Related shipped work:** `docs/object.md` documents the shipped `LazyOperator` that defers materialization until `await` and adds `.as_(name, scope=...)` for naming control on the 16 binary operators. The proposal below is a deeper change — *eliding the result table entirely* for small / scalar paths — and remains future work.

## Lazy Operator Results (Operators Return Views, Not Tables)

Every operator today materializes its result into a fresh ClickHouse table via `create_object(schema)` + `INSERT INTO ... SELECT ...`. For scalar and small-result aggregations (`sum`, `nunique`, `count`, `min`, `max`, `mean`, single-key `group_by.sum`), the extra `CREATE TABLE ... ENGINE = Memory` round-trip dominates wall clock on cheap queries.

**Evidence** (1M rows, chdb 26, `aaiclick/example_projects/chdb_benchmark`):

| Operation | Native `SELECT` | aaiclick `CREATE + INSERT SELECT` | Empty `CREATE TABLE` alone |
|---------------|----------------:|----------------------------------:|---------------------------:|
| Count distinct | 3.89 ms | 9.01 ms | 4.18 ms |
| Group-by sum | 6.62 ms | 8.44 ms | — |

~60–70% of the aaiclick overhead on scalar aggregations is the DDL round-trip — a fixed ~4 ms cost paid to register a throwaway sink table in the catalog. The remaining ~30–40% is Python orchestration (Schema build, Object register, async plumbing).

**Root cause**: `operators.nunique_agg` / `operators.group_by_agg` / `_apply_aggregation` build a `Schema` in Python, then call `create_object(schema)` which emits `CREATE TABLE <result> (...) ENGINE = Memory` with column comments — just to hold a 1-row or 10-row result that the caller almost always unwraps via `.data()`. The schema is fully known in Python before the DDL is sent; the CREATE just *serializes* metadata the runtime already has.

**Proposal**: Scalar and small-result operators return a `LazyScalar` / `LazyView` wrapper carrying the same `Schema` (types, fieldtype, nullability, LowCardinality, descriptions) plus the query SQL. Materialization into a real table happens only when genuinely needed — e.g. `.materialize()`, cross-process handoff, or downstream ops that require a table source.

```python
# Today
async def nunique_agg(info, ch_client):
    schema = Schema(...)
    result = await create_object(schema)          # CREATE TABLE + comments
    await ch_client.command(f"INSERT INTO {result.table} ... SELECT count() ...")
    return result

# Lazy
async def nunique_agg(info, ch_client):
    schema = Schema(...)                          # same Schema
    sql = f"SELECT count() FROM (SELECT value FROM {info.source} GROUP BY value)"
    return LazyScalar(schema=schema, sql=sql, ch_client=ch_client)
    # .data() → one SELECT (saves the ~4 ms CREATE round-trip)
    # .materialize() → falls back to today's behavior when a table is needed
```

**What doesn't change**: `Schema`, `ColumnInfo` (including `low_cardinality`, `nullable`, `array`, `description`), column comments on **persistent** / **job-scoped** tables, cross-process handoff via table name, `open_object()` reconstruction. Metadata remains Python-side first; the CREATE TABLE stays as the serialization path for tables that need to cross a process or session boundary.

**Where a table is still required**:

- Persistent (`p_<name>`) / job-scoped (`j_<job_id>_<name>`) objects.
- Orch task outputs handed off to downstream workers.
- Repeated reads where the result should be cached.
- Joining a result as a table source (rare for scalars; broadcasting as a literal is usually better).

Add `.materialize()` as the explicit escape hatch so callers can opt in.

**Work**:
- `aaiclick/data/object/operators.py` — new `LazyScalar` / `LazyView` classes or extend existing `View`; route `nunique_agg`, `_apply_aggregation` (sum/mean/min/max/count/std/var), `group_by_agg` for small results through them.
- `aaiclick/data/object/object.py` — `.data()` on a lazy result executes the SQL directly; chain operators inline the lazy SQL as a subquery instead of reading from a table name.
- Decide group-by threshold: always lazy vs. materialize above N result rows — likely always lazy, let downstream `.copy()` or `.materialize()` decide.
- Benchmark: `chdb_benchmark` should show `Count distinct` / `Group-by sum` dropping from ~10 ms → ~5 ms at 1M rows.
- Tests: every operator test that currently asserts against a materialized table still passes (via implicit materialize-on-data or an explicit `.materialize()` in tests that introspect `.table`).

Pairs with the "scalar Object unwrapping" idea — once `.data()` is cheap, the ergonomic case of "just give me the number" becomes the fast default. Also a hard precondition for the `.as_()` postfix naming entry below: naming only lands cleanly when it can ride on the first CREATE.

## Clear Task + Downstream

Reset a specific task and all its downstream tasks to PENDING — same concept as Airflow's "clear task". Upstream tasks are untouched; their output tables remain as-is. Useful for re-running part of a pipeline without re-executing the entire job. Independent of lineage — general orchestration capability.

## Fail-Fast for Doomed Group Siblings

`cascade_upstream_failed` in `background/handler.py` marks downstream PENDING tasks `UPSTREAM_FAILED` on any upstream failure, but **siblings in the failing group keep running** — matches Airflow's default `all_success`. Wasted compute when the group's only consumer is already doomed.

Add an Airflow-analog opt-in `fail_fast: bool` flag on `RegisteredJob`/`Job`. When true, a task failing/cancelling also kills its group siblings:

- **PENDING / CLAIMED / PENDING_CLEANUP**: UPDATE → `CANCELLED` in the same `try_complete_job` pass.
- **RUNNING**: ride the existing cancellation monitor (`execution/claiming.py:209`) — reusing `CANCELLED` makes the worker abort path inherit for free, including the COMPLETED-race handling.

Opt-in (not default) so the bug-fix cascade stays backwards-compatible.

**Work**: `fail_fast` column on `RegisteredJob`/`Job` + migration; `cascade_abort_group_siblings()` in `background/handler.py` gated on the flag; tests for PENDING/CLAIMED/RUNNING siblings and the completed-race. ~150 lines, 5–8 tests.

## ClickHouse Migration Framework

aaiclick has no migration system for the ClickHouse side. Alembic manages the SQL schema (`jobs`, `tasks`, `dependencies`, `registered_jobs`, `table_registry`, …), but ClickHouse tables created via the `ChClient` — `operation_log`, all `p_*` / `t_*` / `j_*` data tables produced at runtime — are created with `CREATE TABLE IF NOT EXISTS` in `aaiclick/oplog/models.py` plus a column-existence validator. No versions, no history, no upgrade path.

The consequence: any DDL change in the Python source that would need to alter an existing table is silently a no-op on installs that already have it. Today this has bitten the `operation_log` `ORDER BY` change; it will keep biting every time anything structural changes on the CH side. Column types, new required columns, MergeTree key changes, TTL clauses, materialized projections, etc. all need a coordinated server-side update that the current setup cannot perform.

Also relevant: ClickHouse's own `ALTER TABLE` is limited — `MODIFY ORDER BY` can only append freshly added columns to the sort key, you can't reshape existing ones without rebuilding the table. So even a "real" migration framework has to handle per-change execution strategies (pure ALTER, shadow-table-rebuild, or drop-and-recreate with manual data move), not just a linear script runner.

**What a minimal framework would look like**:

- A `schema_version` table in ClickHouse tracked per-database.
- Versioned DDL scripts under `aaiclick/oplog/migrations/` (or a broader `aaiclick/ch_migrations/`) applied in order by `init_oplog_tables()` on startup.
- Each script declares its own execution strategy — inline `ALTER`, shadow-table rewrite, or a Python callable for data-move logic.
- A `--dry-run` mode for operators.
- Column validator (`_validate_schema`) grows a version check and surfaces a clear error ("your table is at v3, code expects v5, run `aaiclick migrate`").

**Alternatives to building a framework**:

- **Release-notes recipe** — document a maintenance step per release. Zero code, high operator burden, easy to miss.
- **Per-change maintenance CLIs** — `aaiclick maintenance rebuild-oplog`, etc. Works but doesn't scale past a handful of changes.

No action today — fresh installs keep working, existing installs degrade gracefully at worst. Revisit once there is a third structural CH-side change (which makes the per-change CLI approach untenable) or once a change actually breaks (not just slows down) an existing install.

## `.as_()` Naming for Aggregations, Unary, Joins, Concat, Copy, Group-By

Phase 1 (documented in `docs/object.md`) shipped `.as_(name, scope=...)` for the 16 binary operators (arithmetic, comparison, bitwise). Follow-up phases extend the same `LazyOperator` pattern to the remaining operations that materialize a new table:

- Aggregations (`.sum()`, `.mean()`, `.min()`, `.max()`, `.std()`, `.var()`, `.count()`, `.count_if()`, `.quantile()`, `.unique()`, `.nunique()`)
- Unary transforms (`.year()`, `.month()`, `.day_of_week()`, `.lower()`, `.upper()`, `.length()`, `.trim()`, `.abs()`, `.log2()`, `.sqrt()`)
- `.copy()`, `.concat()`, `.join()`, `.group_by(...).sum()` etc.

Each follow-up phase is mechanical: convert the entry method from `async def → Object` to a sync planner returning `LazyOperator(lhs=self, rhs=None, operator=<name>)` and pass `name`/`scope` through to the underlying `create_object` call. The data shape (`rhs: Object | ValueScalarType | None`) was designed phase 1 to accommodate `rhs=None` for unary / aggregation ops without a migration.

---

# Deferred

Items deferred until preconditions are met.

## `Object.export()` HTML Format

`.html` extension → ClickHouse `HTML` output format. The format is supported
by upstream ClickHouse but the chdb build that aaiclick ships against rejects
it with `UNKNOWN_FORMAT` (chdb appears to omit the HTML output handler). Add
an `.html` / `HTML` entry to `FORMATS` in `aaiclick/data/formats.py` and the
corresponding test once chdb's build includes it, or once aaiclick gains a
way to fall back to clickhouse-connect for formats chdb doesn't ship.

## SSE Cross-Host Fanout (Redis)

The v0 SSE pipeline (`docs/frontend.md`) feeds deltas onto a single
in-process bus inside one FastAPI process — Postgres `LISTEN/NOTIFY` for
distributed mode, polling for SQLite local mode. That works for any
deployment where there is exactly one API process per host that clients
can connect to.

Once we run multiple FastAPI workers across machines (e.g. behind a load
balancer for horizontal scale), a notification arriving on host A's
`LISTEN` connection won't reach an SSE client connected to host B.
LISTEN/NOTIFY can't cheaply solve cross-host fanout — every host would
need its own `LISTEN`, which doesn't scale and amplifies DB load.

**Solution when it lands**: Redis Pub/Sub. Workers (or the LISTEN
adapter) publish to a Redis channel; every FastAPI host subscribes and
forwards onto its in-process bus. The in-process bus and SSE delivery
layer don't change — only the *feeder* gets a third option.

**When to revisit**: when we horizontally scale the API server beyond a
single host, or when the single-process bus becomes a measurable
bottleneck for connection count or fan-out throughput.

## Frontend Unit Tests

The SPA (`docs/frontend.md`) ships with no unit-test layer in v0 — only
TypeScript's static type check (`tsc --noEmit`) and Playwright e2e
coverage in `test_e2e/web/`. Add Vitest + React Testing Library when
component logic grows enough that e2e feedback is too coarse to localize
regressions: typically when a single component owns enough branching
behavior (form validation, derived state, conditional rendering paths)
that an e2e failure can't tell you which branch broke.

**Work when revisited**:

- Add `vitest`, `@testing-library/react`, `jsdom` to `package.json` dev deps.
- `npm test` script + `vitest.config.ts` reusing the Vite config.
- Co-locate tests next to the component (`Foo.tsx` → `Foo.test.tsx`),
  matching the Python convention of test files alongside the modules
  they test.
- Add an `npm test` step to the CI workflow that runs the SPA gates.

## Comparison Page

`docs/comparison.md` — feature matrix comparing aaiclick vs Pandas, Spark, and Dask. Defer until the project has enough real-world usage to make meaningful claims.

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.
