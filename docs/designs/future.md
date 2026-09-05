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

## Job Graph View — Group Containers

The job graph view (`docs/designs/ui.md`) renders tasks only. Groups
are honoured semantically — dependencies touching a group are expanded onto its
source / sink tasks — but are not drawn. Containers would render as React Flow
subflows (`parentId` + `extent`) with a status rolled up from member tasks.
The endpoint already accommodates this: `GraphNodeView.kind` gains `"group"`
and `parent_group_id` is populated today.

**When to revisit**: when jobs routinely use nested groups and the flattened
view loses structure operators need. Expect to reassess the layout engine at
the same time — dagre's nested-cluster quality is its weakest area, and the
MIT-compatible escape hatch is Graphviz WASM (`@hpcc-js/wasm-graphviz`), not
elkjs (dual EPL-2.0 / GPL-3.0-or-later).

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

## CLI Lineage AI Commands

A CLI surface for AI lineage (e.g. `aaiclick explain <table>` /
`aaiclick debug <table> "<question>"`). When it lands, add thin
`internal_api` wrappers over `ai.agents.lineage_agent.explain_lineage` and
`ai.agents.debug_agent.debug_result`, kept separate from
`internal_api.lineage` so callers without the `ai` extra can still import
the primitives (a previous unwired version, `internal_api/lineage_ai.py`,
was removed as dead code). MCP intentionally exposes only the
AI-independent primitives (`server/mcp.py`).

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

## ContextVar Guideline — Codebase Sweep

CLAUDE.md requires runtime-mutable process state to live in a
`contextvars.ContextVar` rather than a module global (reference:
`aaiclick/tenancy.py`). The `global` sites below predate that guideline.

Triage, not mechanical conversion — some are correctly process-wide, and a
`ContextVar` would break them (a per-context ID sequence is no longer unique).
Classify each site and leave a one-line comment on the ones that stay global,
so the choice reads as deliberate.

| Site                                              | State                              | Expected verdict                                        |
|---------------------------------------------------|------------------------------------|---------------------------------------------------------|
| `aaiclick/snowflake/snowflake_id.py`              | `_in_memory_last_ms`, `_in_memory_sequence` | Stays global — uniqueness requires one process-wide sequence |
| `aaiclick/orchestration/oplog_backfill.py`        | `_migration_done`                  | Stays global — a once-per-process latch; per-context would re-run it |
| `aaiclick/data/data_context/ch_client.py`         | `_debug_ch_client`                 | Candidate — a debug/test injection point that concurrent tests share |
| `aaiclick/example_projects/chdb_benchmark/...`    | `_session`, `_sink_seq`            | Out of scope — standalone example project                |

Do this when next in these modules, or if a concurrency bug implicates one.
A lint gate would need an allowlist for the deliberate cases — ruff has no
built-in `global` check.

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
