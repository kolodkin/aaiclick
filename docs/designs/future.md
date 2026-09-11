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

The log panel shows only the latest attempt: `get_task_logs`
(`aaiclick/internal_api/tasks.py:53`) reads `task.run_ids[-1]`. Every earlier
attempt's output is already in ClickHouse — `task_logs` tags each line with
`run_id`, and `Task.run_ids` / `Task.run_statuses` keep the ordered list of
attempts with how each ended — so a retried task's failed runs are retained
and simply unreachable from the UI. That is exactly the output you most want
after a flaky task finally passes.

Shape, following Airflow's per-try log selector:

- `GET /tasks/{id}/logs` takes an optional `attempt` (1-based, defaulting to
  the last), resolving it through `run_ids`.
- `TaskLogsView` carries the attempt list with each one's status, so the
  panel can render a selector per attempt without a second request.
- `LogViewer` gains that selector, shown only when `run_ids` has more than
  one entry. Live polling stays on the latest attempt; older ones are
  immutable and need none.

!!! note "Pending input"
    The user will supply Airflow screenshots as the reference for the
    selector's layout and wording. Do not settle the UI details before then.

## API Auth — Beyond Username/Password + RBAC

Username/password users, admin/viewer RBAC, and JWT login (access + refresh)
ship today (`docs/designs/auth.md`). Follow-ups, once more callers / finer control are
needed:

- **Long-lived API tokens / PATs with scopes** — user-minted, named, expiring
  tokens with per-token `read` / `write` scopes for unattended CLI / SDK / MCP
  clients (currently they log in with username/password and ride the refresh
  flow). Includes a token-management UI + CLI.
- **Per-tool MCP RBAC** — the `/mcp` mount is admin-only today; expose
  read-only tools to `viewer` once per-tool gating is worth the complexity.
- **Admin user-management UI** — admins manage users via REST + CLI today.
- **OAuth 2.0 / OIDC / SSO**, **MFA**, **password-reset flow** — delegated /
  hardened identity for enterprise deployments.
- **Per-request audit log** — who called what, when.

## Tenant RBAC — Remaining Phases

Phases 1 (backend core) and 2 (object tenancy) are implemented —
`docs/designs/tenant_rbac.md`. Remaining:

- **Phase 3 — SPA**: tenant switcher sending `X-Tenant-Id`, membership admin
  UI, superadmin-gated controls.

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
