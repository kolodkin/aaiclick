Future Plans
---

Planned work across aaiclick, ordered by priority.

---

# Deferred

Items deferred until preconditions are met.

## Change Signals — Consumers Beyond the UI

The signal (`aaiclick/orchestration/events`) is "a job, task or group row
committed", not a UI concept. The SSE stream and `cli_wait.wait_for_job` both
consume it; `SignalTransport.cross_process` marks which transports can carry
it between processes.

- **Local mode across processes** — `LocalTransport.cross_process` is `False`:
  chdb's file lock means jobs run only inside the `local start` server
  process, so a CLI waiting in another process falls back to polling. Closing
  that gap needs either the Postgres transport or an SSE client in the CLI.
- **MCP / SDK waiters** — `wait_for_job` is already the reusable
  subscribe-then-refetch loop; an in-process caller can await it directly.
  External tools in distributed mode can `LISTEN aaiclick_events` on Postgres
  instead.

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

## Tenants — Kubernetes Control Plane

Multi-tenancy as a fleet layer rather than a filtered column: a control
plane that provisions one full aaiclick installation per tenant, each in
its own Kubernetes namespace, with central identity and direct routing to
each tenant's own ingress.

An installation carries no tenant state — see `docs/designs/auth.md` for
the RBAC it does carry. Tenancy is a Kubernetes-only feature; there is no
Compose or local-mode equivalent.

Full design: `docs/designs/tenants_draft.md`.

**When to revisit**: when a deployment must serve mutually-distrusting
parties. The earlier metadata-level scheme filtered one shared database by
an active tenant and never provided that, which is why it was removed.

## Password Reset by Email

An admin mints reset links today and hands them over out of band
(`docs/designs/auth.md` — Password Reset). A self-service "email me a link"
flow needs an SMTP sender plus a public request endpoint that always answers
`204`, so it never discloses whether an account exists. `users.email` is
already populated — set through the API / CLI, or from the OIDC `email` claim
— so the missing pieces are the sender, its configuration, and the endpoint.
An earlier `aaiclick/auth/mail.py` (`smtplib` on a worker thread via
`asyncio.to_thread`) was removed as unused; it is recoverable from git history.

**When to revisit**: when deployments have a reachable SMTP server, or when
operators mint links often enough for it to hurt.

## OIDC / SSO Login

Authorization-code login against any OpenID Connect provider was built and
then removed: no identity provider is connected, so it was code nobody could
run. Recoverable from git history — `aaiclick/auth/oidc.py` (discovery, PKCE,
code exchange, `id_token` validation against the JWKS) and
`aaiclick/internal_api/oidc.py` (config / start / callback), with the SPA half
in `src/lib/auth.ts` and `src/views/Login.tsx`.

Restoring it needs both back, plus `users.oidc_subject` (`"<issuer>|<sub>"`,
unique) to link a local user to an external identity, an `oidc_states` table
holding one row per in-flight login, and `pyjwt[crypto]` again for RS256
`id_token` signatures. The subject must stay issuer-qualified: `sub` is unique
only within an issuer, so a bare value lets two providers collide.

**When to revisit**: when a deployment has an IdP to point at.

## Foreign-Key Enforcement in the Local Test Backend

SQLite defaults `PRAGMA foreign_keys` to `0`, and SQLAlchemy does not turn it
on, so every `REFERENCES` clause in the local test schema is declared and never
checked. Postgres enforces them always. The local half of the CI matrix is
therefore structurally unable to catch a referential-integrity bug, and half of
the 16 jobs are local — the scope-ladder branch shipped a test helper that
inserted a child row for a parent with no row, which 2595 local tests passed
straight over and only `Internal API dist` rejected.

The fix is a `connect` event listener on the test engine issuing
`PRAGMA foreign_keys=ON`. The cost is unknown until tried: turning enforcement
on may surface existing violations in suites that have been quietly relying on
the laxity, and each one wants fixing rather than suppressing.

**When to revisit**: next time a foreign-key bug reaches `dist` after passing
`local`, or alongside any work already touching the shared test fixtures.

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
  FROM p_orders), c AS (…)`), so the SQL still never names a table and the
  scope rules stay server-side. Verified on chdb that such CTEs
  resolve inside the pagination wrapper and alongside the user's own `WITH`.
  Deferred until single-object queries prove insufficient.
- **Agent push to the browser**: QueryView's remote channel (an agent pushes a
  query or dashboard into a live tab) has no aaiclick equivalent yet; it
  needs the SSE endpoint planned above.
- **Git sync and YAML export** for saved queries and dashboards, as QueryView
  has (QueryView's workspaces have no aaiclick counterpart — one installation
  is one workspace).
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
