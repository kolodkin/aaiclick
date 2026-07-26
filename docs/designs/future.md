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

## Live Log Streaming

`task_logs` is flushed once per run, after the task body completes — logs appear
when the attempt finishes, not while it runs. For live tailing of a long-running
task, the writer would flush incrementally (periodic drain of the
`_ChLogSink` buffer). Deferred to keep the writer off the task's shared
(chdb single-session) client during execution; revisit alongside the SSE
`task.log` event path below.

## Non-Blocking Image-Build Wait (Release-and-Requeue)

On-demand image builds are inline at dispatch: a worker resolves a build-source
task through `ensure_image` (`aaiclick/orchestration/execution/image_builder.py`),
and exactly one worker wins the `build_tasks` claim and builds while any other
worker that already claimed a task needing the same image **polls** the row
(2 s loop) until it is `READY`. The poller holds its worker slot for the whole
build. In the common case the DAG's entry task gates the graph, so only the
builder is busy — but a job whose first runnable layer is a wide fan-out of
independent tasks can tie up N−1 slots polling for a single cold `docker build`.

The fix has two halves, both keyed on the `build_tasks` row's `BUILDING` status:

- **Claim-time avoidance** — `claim_next_task`
  (`aaiclick/orchestration/execution/claiming.py`) prefers tasks whose image is
  *not* currently building, so a free worker picks up independent runnable work
  instead of blocking behind an in-flight build. This is the primary motivation:
  scheduling should route around the image being built at that moment, not just
  wait on it.
- **Release-and-requeue** — a worker that would otherwise poll releases its
  claim (back to `PENDING` with a short `retry_after`) instead of holding the
  slot; the task is re-claimed once the image reaches `READY`. The builder still
  blocks on its own build (unavoidable — someone has to build), but everyone
  else frees their slot.

Correctness doesn't depend on this — the current design is "correct but can
occupy slots on wide fan-outs." **When to revisit**: when wide fan-out jobs on a
cold image measurably tie up workers.

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
