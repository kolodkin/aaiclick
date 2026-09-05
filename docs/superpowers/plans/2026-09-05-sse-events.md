# SSE Events + LISTEN/NOTIFY Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the UI's 2 s polling with an SSE `/events` stream fed by Postgres NOTIFY (distributed) or an in-process bus (local).

**Architecture:** SQLAlchemy `Session` listeners flag any transaction that writes `jobs`/`tasks`/`groups`; on commit they `pg_notify` (Postgres) or publish to a process-local `EventBus` (SQLite). The server exposes `GET /api/v0/events` as `text/event-stream` and, in distributed mode, runs one `LISTEN` connection that feeds the bus. The browser opens the stream with `fetch`, invalidates React Query on every `changed`, and falls back to 2 s polling while disconnected.

**Tech Stack:** SQLAlchemy 2 events, FastAPI `StreamingResponse`, asyncpg listeners via the raw driver connection, TanStack Query 5, TypeScript.

**Spec:** `docs/superpowers/specs/2026-09-05-sse-events-design.md`

## Global Constraints

- All imports at file top; no `Any`; `ContextVar` for mutable process state; `Literal` over enums.
- Tests are flat `async def test_*` functions next to the module; `filterwarnings = error`.
- Router tests assert HTTP plumbing only.
- `src/api/schema.ts` is regenerated with `npm run gen-types`, never hand-edited.

---

### Task 1: EventBus + context accessor

**Files:**
- Create: `aaiclick/orchestration/events.py`
- Test: `aaiclick/orchestration/test_events.py`

**Interfaces (produces):**
- `class EventBus`: `publish() -> None`, `subscribe() -> AsyncIterator[None]`, `close() -> None`, `closed: bool`.
- `get_event_bus() -> EventBus`, `@contextmanager event_bus(bus: EventBus) -> Iterator[None]`.

- [ ] Write failing tests: subscriber receives a published signal; a burst of publishes before the subscriber reads yields exactly one pending signal; `close()` ends the subscriber loop; `event_bus()` swaps `get_event_bus()` and restores it.
- [ ] Run `uv run pytest aaiclick/orchestration/test_events.py -v` → ImportError.
- [ ] Implement `EventBus` with per-subscriber `asyncio.Queue[bool](maxsize=1)`; `publish` does `put_nowait` and swallows `QueueFull`; `close` puts a `False` sentinel and drops later subscribes; `_event_bus_var: ContextVar[EventBus]` with a process-wide default instance (comment why).
- [ ] Run tests → PASS. Commit `feat(orchestration): in-process EventBus for UI change signals`.

### Task 2: Session listeners (write detection + notify)

**Files:**
- Modify: `aaiclick/orchestration/events.py`
- Modify: `aaiclick/orchestration/orch_context.py` (import `events` so listeners register)
- Test: `aaiclick/orchestration/test_events.py`

**Interfaces (produces):** `EVENTS_CHANNEL = "aaiclick_events"`, `WATCHED_TABLES = ("jobs", "tasks", "groups")`, `statement_touches_watched(sql: str) -> bool`.

- [ ] Write failing tests (with `orch_ctx_no_ch`): `update_task_status` on a created job publishes one signal to a bus entered via `event_bus()`; a commit writing only `execution_workers` publishes none; `cancel_job` (raw `UPDATE tasks`) publishes; a rolled-back session publishes none. Parametrize `statement_touches_watched` with `("UPDATE tasks SET …", True)`, `("\n  UPDATE jobs …", True)`, `("INSERT INTO table_run_refs …", False)`, `("SELECT … FROM tasks", False)`.
- [ ] Run → FAIL.
- [ ] Implement listeners on `sqlalchemy.orm.Session`: `do_orm_execute` (textual statements via `statement_touches_watched(str(state.statement))`), `before_flush` (instances of `Job`/`Task`/`Group` in `new | dirty | deleted`), `before_commit` (`session.flush()`; if flagged and `is_postgres()`: `session.execute(text("SELECT pg_notify(:c, '')"), {"c": EVENTS_CHANNEL})`), `after_commit` (if flagged and not postgres: `get_event_bus().publish()`; pop flag), `after_rollback` (pop flag). Flag key `_FLAG = "aaiclick_events_dirty"` in `session.info`.
- [ ] Import `from . import events  # noqa: F401 — registers Session listeners` in `orch_context.py`.
- [ ] Run tests → PASS. Run `uv run pytest aaiclick/orchestration/execution/test_cancel_job.py aaiclick/orchestration/background -q` to confirm no regressions. Commit `feat(orchestration): flag job/task writes and notify on commit`.

### Task 3: Server — `/events` stream + Postgres listener

**Files:**
- Create: `aaiclick/server/events.py`
- Modify: `aaiclick/server/app.py` (include router with `require_tenant`; start listener in lifespan when `is_postgres()`; `bus.close()` on shutdown)
- Test: `aaiclick/server/test_events.py`

**Interfaces (produces):** `router: APIRouter` (`GET /events`), `async def listen_postgres(bus: EventBus, *, stop: asyncio.Event) -> None`, `MIN_FRAME_INTERVAL = 0.5`, `KEEPALIVE_INTERVAL = 15.0`.

- [ ] Write failing tests: `anon_client` gets 401 in distributed mode (skip when local); with `event_bus(bus)`: start `app_client.get(f"{API_PREFIX}/events")` as a task, publish, `bus.close()`, await → status 200, `content-type` starts with `text/event-stream`, body contains `event: changed`. Postgres-only test: run `listen_postgres` as a task, commit a task-status write via `orch_ctx_no_ch`, await a subscriber signal within 5 s, set stop.
- [ ] Run → FAIL.
- [ ] Implement `_stream(bus)` generator: `async for _ in bus.subscribe()` — but with keepalive: pull from the subscription with `asyncio.wait_for(anext(it), KEEPALIVE_INTERVAL)`, on timeout yield `": keepalive\n\n"`, on signal yield `"event: changed\ndata: {}\n\n"` then `await asyncio.sleep(MIN_FRAME_INTERVAL)`. Response headers `Cache-Control: no-cache`, `X-Accel-Buffering: no`.
- [ ] Implement `listen_postgres`: engine `create_async_engine(get_db_url(), poolclass=NullPool)`; loop until `stop`: `async with engine.connect() as conn: conn = await conn.execution_options(isolation_level="AUTOCOMMIT"); raw = await conn.get_raw_connection(); driver = cast(_Listenable, raw.driver_connection); await driver.add_listener(EVENTS_CHANNEL, cb)`; `cb` publishes; then `while not stop.is_set(): await asyncio.wait_for(stop.wait(), PING_INTERVAL)` with `await conn.execute(text("SELECT 1"))` on timeout; on exception log, sleep backoff (1 s → 30 s), reconnect and `bus.publish()`.
- [ ] Wire into `app.py`.
- [ ] Run → PASS. Commit `feat(server): SSE /events endpoint fed by Postgres LISTEN or the local bus`.

### Task 4: Browser — live updates with polling fallback

**Files:**
- Create: `src/api/events.ts`
- Modify: `src/api/client.ts` (export `openStream(path, signal)`), `src/main.tsx`, `src/App.tsx`, `src/api/hooks.ts` (`useTaskLogs(id, live)`), `src/components/LogViewer.tsx` (accept `live`), `src/views/TaskDetail.tsx`, `src/api/schema.ts` (regenerated).

- [ ] `npm ci`; `npm run gen-types`; `npm run check` → schema includes `/api/v0/events`.
- [ ] `events.ts`: module `connected` flag + listeners; `isLiveConnected()`; `useLiveUpdates()` effect: loop `openStream` → on response ok set connected, `qc.invalidateQueries()`; read `res.body` with `TextDecoder`, split frames on `\n\n`, for a frame containing `event: changed` call `qc.invalidateQueries()`; on end/error set disconnected, `qc.invalidateQueries()`, backoff `min(30000, 1000 * 2**n)`; abort on unmount; do not reconnect after a 401 (client already signals unauthorized).
- [ ] `main.tsx`: `refetchInterval: () => (isLiveConnected() ? false : 2000)`.
- [ ] `App.tsx`: render `<LiveUpdates />` (a null component calling `useLiveUpdates`) once `me` is set.
- [ ] Task logs: `useTaskLogs(id, live: boolean)` → `refetchInterval: live ? 2000 : undefined`; `TaskDetail` passes `live={!TERMINAL.has(task.status)}` to `LogViewer`.
- [ ] `npm run check && npm run build` → clean. Commit `feat(ui): consume /events stream, poll only as fallback`.

### Task 5: Docs + cleanup

- [ ] `docs/designs/frontend.md`: Real-time row → SSE; rewrite "Data layer" polling sentences; add "Live updates" subsection with implementation references.
- [ ] `docs/designs/api_server.md`: add `GET /events` to the REST surface.
- [ ] `docs/designs/future.md`: delete the SSE section.
- [ ] Delete this plan and the spec; `git grep` for references.
- [ ] `uv run ruff check . && uv run ruff format --check .`; full `uv run pytest aaiclick/orchestration/test_events.py aaiclick/server -q`. Commit `docs: describe SSE live updates`.
