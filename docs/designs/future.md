Future Plans
---

Planned work across aaiclick, ordered by priority.

---

# Deferred

Items deferred until preconditions are met.

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
polling, but avoids N×M HTTP requests from N browser tabs). SQLite pairs with
chdb, and local mode is single-process — cross-host fanout doesn't arise there.

**Multi-host is already covered**: Postgres delivers each `NOTIFY` to every
connection that has issued `LISTEN`, so N API hosts just hold N `LISTEN`
connections — no extra broker. `NOTIFY` in the same commit as the status
write guarantees a refetching client sees the committed state. Escape
hatches, should the feeder ever measurably hurt:

- **Redis Pub/Sub** — only if listener count or notification volume becomes a
  real cost (dozens of hosts, very high event rates), or payloads outgrow
  Postgres's ~8 KB `NOTIFY` limit; events are signals, not payloads, so they
  stay tiny.
- **ClickHouse tail** — each API host polls `operation_log` (or a dedicated
  events table) past a watermark: N pollers instead of N×M browser polls, no
  touch on the SQL commit path. But latency is poll-bound and the CH insert
  is unordered relative to the SQL commit, so a client can refetch before the
  status write is visible. chdb is in-process single-session — not a bus.

**When to revisit**: when polling overhead is measurable (many tabs or many
concurrent jobs), or when sub-2 s latency matters for operators.

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

## Opaque Object Table Names

Persistent objects encode both the user-visible name and (from Phase 2 above)
the tenant into the ClickHouse table name, so `aaiclick/data/scope.py` parses
names back out of tables and object names must satisfy an identifier regex plus
a length cap that shrinks as the tenant id grows.

Decoupling the two removes all of it: store `p_<snowflake>` in ClickHouse and
keep the human name only in `table_registry` under a `UNIQUE (tenant_id, name)`
constraint. Prefix parsing disappears (`name_from_table` and every `p_`-prefix
scan retire), listing becomes a plain SQL query, per-tenant name uniqueness is
enforced by the database rather than by string layout, and the name-length
budget stops depending on the tenant id's digit count.

**When to revisit**: when object naming rules or prefix parsing become a
recurring source of friction. The cost is renaming every existing `p_*` table
plus a compatibility path for objects opened by name.

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

- **Object names in user SQL**: rewrite bare object names inside a scope to
  their table names before validation, using the same tokenizer as
  `validate_scope`. Deferred because a column and an object can share a name.
- **Viewer token**: mint a longer-lived read-only token for the viewer tab
  instead of handing over the access JWT; the tab cannot refresh today.
- **Persisted declared queries**: a `queries_doc` column beside
  `table_registry.schema_doc`, written at object creation, so declarations
  travel with the object instead of a construction-time hook.
- **Workspaces and git sync** in the viewer (`Views(workspaces=..., git=...)`)
  over aaiclick storage.

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
