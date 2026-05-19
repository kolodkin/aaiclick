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

## LazyOperator — Elide Materialization for Small / Scalar Results

See [future_lazy_operator.md → Phase 3](future_lazy_operator.md#phase-3-defer-the-create-table--materialize-only-on-demand). Real wall-clock wins on scalar aggregations (~10 ms → ~5 ms) by deferring the throwaway `CREATE TABLE` inside the existing `LazyOperator` — no new class. `.data()` runs the inner SELECT directly; the table only materializes when explicitly needed (`.as_()`, `.table`, downstream table-source).

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

## LazyOperator — `.as_()` for Joins, Concat, Copy, Group-By

See [future_lazy_operator.md → Phase 2b](future_lazy_operator.md#phase-2b-as_-for-joins-concat-copy-group-by). Mechanical extension of the now-shipped Phase 2a (aggregations + unary transforms) to the remaining operations that materialize a new table — same sync-planner pattern, same `.as_(name, scope=...)` API. The `rhs: Object | ValueScalarType | None` + `params: dict | None` data shape covers binary, unary, and parametrized operators alike.

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
