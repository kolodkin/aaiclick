Future Plans
---

Planned work across aaiclick, ordered by priority.

---

# Deferred

Items deferred until preconditions are met.

## Event Fanout — Beyond Postgres LISTEN/NOTIFY

`GET /api/v0/events` streams change signals fed by `pg_notify` on every
job/task commit (`docs/designs/frontend.md` — Live updates). Escape hatches,
should the feeder ever measurably hurt:

- **Redis Pub/Sub** — only if listener count or notification volume becomes a
  real cost (dozens of hosts, very high event rates). Signals carry no
  payload, so Postgres's ~8 KB `NOTIFY` limit never bites.
- **ClickHouse tail** — each API host polls `operation_log` past a watermark:
  N pollers, no touch on the SQL commit path. But latency is poll-bound and
  the CH insert is unordered relative to the SQL commit, so a client can
  refetch before the status write is visible.
- **Typed per-job events with tenant filtering** — every view is job-scoped
  and refetches the same few queries, so the coarse signal costs nothing
  today; widen the payload only if a view needs to ignore other jobs' churn.

## Change Signals — Consumers Beyond the UI

The signal (`aaiclick/orchestration/events`) is "a job, task or group row
committed", not a UI concept; the SSE stream is merely its first subscriber.
Next in line:

- **`cli_wait.wait_for_job`** — polls job stats on a fixed interval today.
  It could run the active transport's `feed` and block on
  `EventBus.subscribe()` instead, re-reading stats only when a signal lands:
  sub-second reaction, zero idle queries. Keep a slow poll as the fallback,
  as the browser does. Local mode is the harder case: the CLI is a separate
  process from a running local server, and `LocalTransport` only sees
  commits in its own process, so a wait on a job the server is running
  would need the Postgres transport or the SSE stream over HTTP.
- **MCP / SDK waiters** — the same subscribe-then-refetch loop serves any
  in-process caller that blocks on a job; external tools in distributed
  mode can `LISTEN aaiclick_events` on Postgres directly.

## Task Logs — Per-Attempt History in the Log Panel

`get_task_logs` (`aaiclick/internal_api/tasks.py`) reads `task.run_ids[-1]`, so
the panel shows only the latest attempt. Earlier attempts are already in
ClickHouse — `task_logs` tags each line with `run_id`, and `Task.run_ids` /
`Task.run_statuses` hold the ordered attempts and how each ended — so a retried
task's failed runs are retained but unreachable. That is exactly the output you
want after a flaky task finally passes.

Shape, following Airflow's per-try log selector:

- `GET /tasks/{id}/logs` takes an optional 1-based `attempt`, resolved through
  `run_ids`; defaults to the last.
- `TaskLogsView` carries the attempts and their statuses, so the selector costs
  no second request.
- `LogViewer` shows the selector only when `run_ids` has more than one entry.
  Polling stays on the latest attempt; older ones are immutable.

!!! note "Pending input"
    Airflow screenshots to follow as the reference for layout and wording — do
    not settle the UI details before then.

## Tenant RBAC — Remaining Phases

Phases 1 (backend core) and 2 (object tenancy) are implemented —
`docs/designs/tenant_rbac.md`. Remaining:

- **Phase 3 — SPA**: tenant switcher sending `X-Tenant-Id`, membership admin
  UI, superadmin-gated controls.

## Password Reset by Email

A superadmin mints reset links today and hands them over out of band
(`docs/designs/auth.md` — Password Reset). A self-service "email me a link"
flow needs an SMTP sender plus a public request endpoint that always answers
`204`, so it never discloses whether an account exists. `users.email` is
already populated — set through the API / CLI, or from the OIDC `email` claim
— so the missing pieces are the sender, its configuration, and the endpoint.
An earlier `aaiclick/auth/mail.py` (`smtplib` on a worker thread via
`asyncio.to_thread`) was removed as unused; it is recoverable from git history.

**When to revisit**: when deployments have a reachable SMTP server, or when
operators mint links often enough for it to hurt.

## Java Task SDK — Shim Jar (`jvm` Entry Type)

**Decision**: Java payloads run through the existing shell/container path,
claimed by Python workers — a `.jar` already runs on every runner via
`entry_type="shell"` plus a prebuilt JVM image. What shell tasks lack is the
data plane: typed kwargs, a return value, downstream consumption. The shim-jar
SDK closes that gap without a second worker implementation.

- **`aaiclick-task-api` Maven module** (the parent POM anticipates it):
  `@AaiTask` annotation + registry, plus a bootstrap `main()` mirroring the
  Python `remote_result` shim — load the task row by
  `--task-id N --run-epoch M`, Jackson-bind `kwargs` to the annotated method,
  write the JSON result row (plain values only). The user's image embeds the
  SDK; the runner invocation is the shim, like the module path's layer-2
  bootstrap.
- **`"jvm"` entry type**: add to the `EntryType` Literal (plain String column
  — code change only, no migration). `tasks.entrypoint` holds the Java class
  name. Python workers claim `jvm` tasks and dispatch them like module
  container tasks, injecting the full runner env (same trust model as module
  images). CLI, `run_job()`, and `RunJobRequest` grow the `jvm` choice.
- **Submission validation**: `jvm` tasks must not receive Object/View refs as
  kwargs and their results are never auto-converted to Objects — enforced at
  commit points alongside `validate_image_sources()`
  (`aaiclick/orchestration/image_injection.py`).
- **Publishing**: `aaiclick-task-api` to Maven Central via the Central
  Publisher Portal, on the **same tag** as the Python package (lockstep
  versioning — the compatibility contract is a release's PostgreSQL schema
  and task semantics). Namespace `io.github.kolodkin` auto-verifies against
  the GitHub account; needs GPG signing + sources/javadoc jars,
  `central-publishing-maven-plugin`, two secrets (portal token, GPG key).
  De-risk early with a one-time `0.0.x` dry-run publish of an empty artifact.
- **Salvage from git history**: the standalone `java/aaiclick-worker` claim
  loop was removed (superseded — it duplicated claim/heartbeat/rollup
  semantics in a distributed-only component the local chdb + SQLite dev loop
  could never exercise, and closed none of the data-plane gap). Its
  `ChClient` / `Db` / `NamedParamSql` classes are reusable starting points
  for the SDK, recoverable from git history.

## Viewer Follow-ups

See `viewer.md` for the shipped design.

- **SQL over several objects at once** (joins, unions): today `query_object`
  reads one object with a `where` filter. A `query_sql` verb would take
  `scope`, a SQL text, and a map of the object names it uses
  (`{"o": "orders", "c": "customers"}`); the user writes `SELECT … FROM o
  JOIN c ON …` and the server prepends one CTE per entry (`WITH o AS (SELECT *
  FROM p_7_orders), c AS (…)`), so the SQL still never names a table and the
  tenant / scope rules stay server-side. Verified on chdb that such CTEs
  resolve inside the pagination wrapper and alongside the user's own `WITH`.
  Deferred until single-object queries prove insufficient.
- **Agent push to the browser**: QueryView's remote channel (an agent pushes a
  query or dashboard into a live tab) has no aaiclick equivalent yet; it
  needs the SSE endpoint planned above.
- **Git sync and YAML export** for saved queries and dashboards, as QueryView
  has (QueryView's workspaces map to tenants here, so nothing else is needed).
- **`options_sql` params**: the kernel's `params:` block accepts a query
  whose first column feeds a dropdown; aaiclick has no free-SQL endpoint, so
  `QueryPanel` renders static `options` only.
- **Dashboard authoring in the UI**: `@dashboard` picks and runs; HTML and
  panel queries are written through MCP, REST, or `view dashboards save`.

## Lazy Operator — Chain Fusion

Every `LazyOperator` node materializes into its own table. For single-source
families (unary transforms, aggregations, string ops) the upstream SELECT
could instead be wrapped as a subquery, so `obj.abs().sum()` writes one table
rather than two. Not a correctness problem; measure before acting.

Weigh it carefully: "each node materializes into its own table — no fusion"
is a stated invariant in `docs/user_guide/object.md`, and the per-node tables
are what make `.as_()` and refcounted cleanup work.

Separately, `LazyOperator` keeps `lhs` / `rhs` after `_materialized` is set,
so holding an awaited chain pins its intermediate tables (table lifetime is
refcounted off Python object lifetime). Clearing them needs `as_()` — the only
reader — handled first.

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.
