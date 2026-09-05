SSE Events + LISTEN/NOTIFY Design
---

Replace the UI's 2 s `refetchInterval` polling with a server-sent events
stream fed by Postgres `NOTIFY` (distributed) or an in-process bus (local).

# Goals

- Sub-second UI latency on job / task status changes, zero idle traffic.
- One mechanism for every status write, including raw-SQL sites.
- No cross-tenant leakage through the event channel.
- Polling remains as an automatic fallback when the stream is down.

# Event model

One event kind, `changed`, with no payload. Events are signals: the browser
invalidates its React Query cache and REST supplies authoritative state.
Per-id events were considered and dropped — every view is job-scoped and
refetches the same few queries, so targeting buys nothing while costing a
tenant lookup per write and a tenant filter per stream.

# Components

## `aaiclick/orchestration/events.py`

- `EventBus`: process-local pub/sub. `subscribe()` yields signals; each
  subscriber queue has depth 1, so a burst collapses into one pending
  signal. `close()` ends every subscriber stream (server shutdown).
- `_event_bus_var: ContextVar[EventBus]` with a process-wide default bus,
  `get_event_bus()`, and an `event_bus(bus)` context manager for tests.
- SQLAlchemy `Session` listeners registered once at import:
  - `do_orm_execute`: textual `INSERT INTO|UPDATE|DELETE FROM` on `jobs`,
    `tasks` or `groups` flags the session (`session.info`).
  - `before_flush`: ORM `Job` / `Task` / `Group` instances in
    `new` / `dirty` / `deleted` flag the session.
  - `before_commit`: flush, then in Postgres mode execute
    `SELECT pg_notify('aaiclick_events', '')` in the same transaction.
  - `after_commit`: in SQLite mode publish on the context bus. Clear flag.
  - `after_rollback`: clear flag.

## `aaiclick/server/events.py`

- `router`: `GET /events` → `StreamingResponse(text/event-stream)`,
  guarded by `require_tenant`. Frames: `event: changed\ndata: {}\n\n`; a
  `: keepalive` comment every 15 s; at most one frame per 500 ms.
- `listen_postgres(bus)`: lifespan task in distributed mode. Holds one
  autocommit connection from a `NullPool` engine, registers an asyncpg
  listener through the raw driver connection (typed via a `Protocol`, no
  top-level `asyncpg` import), pings `SELECT 1` every 30 s, and on any
  failure reconnects with capped backoff and publishes `changed` so
  clients resync.

## Browser (`src/api/events.ts`)

- `useLiveUpdates()` mounted once in `App` after auth resolves: opens
  `/api/v0/events` via `fetch` (bearer + tenant headers; `EventSource`
  cannot send headers), parses SSE frames, invalidates all queries on
  `changed` and on every (re)connect, reconnects with backoff (1 s → 30 s).
- `isLiveConnected()` feeds the QueryClient default
  `refetchInterval: () => (isLiveConnected() ? false : 2000)`.
- Task logs: `useTaskLogs(id, live)` polls at 2 s only while the task is
  non-terminal (logs reach ClickHouse from the task process, never via a
  SQL commit).

# Testing

- Bus: subscribe / publish / burst coalescing / close ends streams.
- Session hooks (both backends): a `update_task_status` commit publishes
  exactly one signal; a commit touching only `execution_workers` publishes
  none; rollback publishes none.
- Postgres listener (distributed matrix only): a commit in the test process
  reaches the bus through `LISTEN`.
- Endpoint: 401 without auth; a stream receives a frame after a publish and
  ends when the bus closes.

# Docs

`docs/designs/frontend.md` (real-time section), `docs/designs/api_server.md`
(endpoint), remove the item from `docs/designs/future.md`. This spec is
deleted once the feature lands.
