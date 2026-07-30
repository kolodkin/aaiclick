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

## Per-Task Image Requirement

Replace the job-level ("entrypoint") image concept: the image becomes a task
requirement (`tasks.image_source`, parent→child inheritance), and the build
becomes a plain task running `docker build`. Registry mode injects one build
task per distinct image with every dependent task wired to it; no registry
means always-inline per-host builds. Retires the `build_tasks` claim/lease
machinery in favor of graph dependencies + registry pull-first. Design:
`docs/designs/task_image.md`. **When to revisit**: when a job needs tasks on
different images, or the lease machinery's complexity needs paying down.

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

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.
