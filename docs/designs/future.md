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

## Inline No-Registry Build Holds the Worker Slot

In registry mode, image builds are ordinary graph tasks gated by dependency
edges — no worker ever waits on someone else's build. Without a registry the
docker launch path builds inline (`docker_build.resolve_launch_image`),
holding the worker slot for the cold build. Accepted: no-registry is de facto
single-host / small-scale mode, and per-host daemon cache dedups repeats.
**When to revisit**: only if no-registry multi-worker hosts with cold builds
become a real workload — the likely fix is a registry, not scheduler work.

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

## CLI Lineage AI Commands

A CLI surface for AI lineage (e.g. `aaiclick explain <table>` /
`aaiclick debug <table> "<question>"`). When it lands, add thin
`internal_api` wrappers over `ai.agents.lineage_agent.explain_lineage` and
`ai.agents.debug_agent.debug_result`, kept separate from
`internal_api.lineage` so callers without the `ai` extra can still import
the primitives (a previous unwired version, `internal_api/lineage_ai.py`,
was removed as dead code). MCP intentionally exposes only the
AI-independent primitives (`server/mcp.py`).

## Java Worker Release Flow

`publish.yaml` has no Java steps yet — the wiring needs a real `vX.Y.Z`
release tag to test against. When added, a `java` job on the **same tag** as
the Python package (lockstep versioning: the worker's compatibility contract
is a specific release's PostgreSQL schema and task semantics):

1. Derive the Maven version from the tag, `mvn -B package`, attach the shaded
   `aaiclick-worker` jar as a GitHub Release asset, and publish a docker
   image alongside the existing ones.
2. Phase 2 adds Maven Central via the Central Publisher Portal for the
   `aaiclick-task-api` module (namespace `io.github.kolodkin` auto-verifies
   against the GitHub account; needs GPG signing + sources/javadoc jars,
   `central-publishing-maven-plugin`, two secrets: portal token, GPG key).
   De-risk early with a one-time `0.0.x` dry-run publish of an empty
   artifact — namespace verification and signing setup are the only steps
   with bureaucratic latency.

## Java Worker Phase 2 — `jvm` Entry Type

Native Java tasks on the existing worker (`java/aaiclick-worker` runs
shell-only today):

- Add `"jvm"` to the `EntryType` Literal (code change only — plain String
  column, no migration). `tasks.entrypoint` holds a Java class name; `kwargs`
  JSON binds to method parameters via Jackson; the return value is JSON
  (plain values only).
- **Submission validation**: `jvm` tasks must not receive Object/View refs as
  kwargs and their results are never auto-converted to Objects — enforced at
  commit points alongside `validate_image_sources()`
  (`aaiclick/orchestration/image_injection.py`). This keeps the worker's
  "no ClickHouse object support" boundary honest.
- **Java task SDK**: a new `aaiclick-task-api` Maven module (the parent POM
  anticipates it) — `@AaiTask` annotation + registry; each task runs in a
  spawned child JVM mirroring the mp worker's isolation/timeout/kill
  semantics.
- **Claim filters are bound values** in the shared
  `sql/claim_next_task.sql`: Python workers drop `jvm` from their
  `entry_types` bind; the Java worker widens to `['shell', 'jvm']`. CLI,
  `run_job()`, and `RunJobRequest` grow the `jvm` choice.

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.
