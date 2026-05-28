Future Plans
---

Planned work across aaiclick, ordered by priority.

---

# Medium Priority

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

## SSE `/events` Endpoint + LISTEN/NOTIFY Fanout

v0 uses 2 s `refetchInterval` polling. The designed real-time path is:

1. `GET /api/v0/events` → `text/event-stream` (one connection per UI session).
2. Workers emit `NOTIFY job_events` in the same commit as every status write.
3. FastAPI holds one `LISTEN` connection per backend and forwards
   notifications onto an in-process pub/sub bus.
4. The SSE endpoint subscribes and streams typed events (`job.updated`,
   `task.updated`, `task.log`) to the browser.
5. The browser calls `queryClient.invalidateQueries(...)` and lets REST
   fetch authoritative state — events are signals, not payloads.

**SQLite local mode**: poll + snapshot diff every 2 s (same latency as current
polling, but avoids N×M HTTP requests from N browser tabs).

**When to revisit**: when polling overhead is measurable (many tabs or many
concurrent jobs), or when sub-2 s latency matters for operators.

## Cross-Host Log Access

`task.log_path` stores the filesystem path written by the worker process.
In local mode (single process) `aaiclick/internal_api/tasks.py` — `get_task_logs`
reads the file directly. In distributed / Docker mode the log file lives on the
worker host's filesystem and is not accessible to the API server.

**Solution when it lands**: either (a) workers stream log lines into a DB column
or a dedicated log table as they write, or (b) a sidecar log-shipping agent
uploads completed log files to object storage (S3 / GCS) and `get_task_logs`
redirects to a presigned URL.

**When to revisit**: when Docker or multi-host distributed runs become the
primary deployment mode and operators need task logs in the UI.

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

## OpenAPI Codegen

`src/api/types.ts` is hand-written to mirror the pydantic view models.
When the API surface grows, generate it from `GET /api/v0/openapi.json`
using `openapi-typescript` or similar — run as a pre-build step so the
TypeScript types always match the server schema.

**Work when revisited**: add `openapi-typescript` dev dep, `npm run gen-types`
script, CI check that the generated file is up to date (commit the output;
fail if dirty after re-gen).

## Operator UI Auth

The v0 server is unauthenticated (`localhost-only` intent). When the UI is
exposed beyond localhost, add an auth layer:

- Simple option: HTTP Basic via a reverse proxy (nginx / Caddy).
- Integrated option: cookie session with a configurable password via a FastAPI
  middleware; the SPA sends the cookie on every request.
- Enterprise option: OAuth2 / OIDC via an identity provider.

**When to revisit**: when the server is intentionally exposed on a network
interface accessible to untrusted clients.

## Wheel Packaging of `aaiclick/server/static/`

The Vite build output (`aaiclick/server/static/`) is gitignored and not
included in the source tree. Release wheels must include it so the server
works out of the box after `pip install aaiclick`.

**Work when revisited**: add a `npm run build` step to the release CI workflow
before `python -m build`; configure `pyproject.toml` `[tool.hatch.build]`
(or equivalent) to include `aaiclick/server/static/**` in the sdist and wheel.

## Comparison Page

`docs/comparison.md` — feature matrix comparing aaiclick vs Pandas, Spark, and Dask. Defer until the project has enough real-world usage to make meaningful claims.

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.
