Future Plans
---

Planned work across aaiclick, ordered by priority.

---

# Medium Priority

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

## Native Arrow Insert for Ingest

`create_object_from_value` converts arrow leaves to Python lists
(`to_pylist()`) for the list-based `ChClient.insert`. A follow-up can add
an arrow-native insert to the `ChClient` protocol (clickhouse-connect
`insert_arrow`; chdb `Python()` table engine) to skip that conversion.
Worth doing only if profiling shows the conversion matters — the
per-record Python passes are already gone.

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
polling, but avoids N×M HTTP requests from N browser tabs).

**When to revisit**: when polling overhead is measurable (many tabs or many
concurrent jobs), or when sub-2 s latency matters for operators.

## Retire File-Based Task Logs

Cross-host log access shipped: every runner streams captured stdout/stderr into
the ClickHouse `task_logs` table from inside the task process, and `get_task_logs`
reads that single host-independent source (`aaiclick/orchestration/logging.py`,
`aaiclick/oplog/models.py`). Two now-redundant legacy paths remain until a
cleanup pass:

- **`task.log_path` / file tee** — still written for on-host debugging and
  surfaced in `TaskDetail`. Drop once nothing reads the local file.
- **Kubernetes `kubectl logs` host fetch** — `_capture_pod_logs` writes a host
  file the read path no longer consults (the Pod streams to CH directly). Remove
  it and the `RemoteTaskResult.log_path` column once the file tee is gone.

**When to revisit**: opportunistically, once the file path has demonstrably no
consumers; no functional gap blocks it.

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

## SSE Cross-Host Fanout (Redis)

The v0 SSE pipeline (`docs/designs/frontend.md`) feeds deltas onto a single
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
