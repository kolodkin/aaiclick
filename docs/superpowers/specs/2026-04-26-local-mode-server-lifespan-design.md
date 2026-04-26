Local-mode Server Lifespan
---

# Motivation

Today, running the FastAPI or MCP server in local mode (`chdb` + `sqlite`) is broken: jobs submitted via `POST /api/v0/jobs:run` (or the MCP `run_job` tool) are written to the database but never picked up — there is no execution worker and no background worker in the server process. The only working local-mode entrypoint is `python -m aaiclick local start`, which runs workers but exposes no HTTP / MCP surface.

This design wires the workers into the server's `lifespan`, gated on `is_local()`. After the change, every local-mode entrypoint — CLI verb or direct uvicorn — converges on a single shared helper that owns startup/shutdown ordering. Distributed mode is unchanged: the lifespan no-ops, and `worker start` / `background start` still run as separate processes.

The single-process constraint of local mode (chdb's file lock) is honoured naturally: the workers and the per-request handlers share one chdb session inside one outer `orch_context(with_ch=True)`.

# Scope

In scope:

- A new shared async context manager `local_runtime()` that starts and stops the BackgroundWorker and execution `worker_main_loop` for the duration of its block.
- A `lifespan` for the FastAPI / FastMCP ASGI apps that calls `local_runtime()` when `is_local()` is true.
- Three named ASGI surfaces in `aaiclick/server/app.py`: REST-only (`rest_app`), MCP-only (`mcp_app`), and the existing combined `app`.
- Two CLI verbs: `local start` (binds `rest_app`) and `local-mcp start` (binds `mcp_app`). Both use programmatic uvicorn launch.
- Tests covering the lifespan path (worker / background startup, in-flight task cancel on shutdown) and the new CLI verb wiring.

Out of scope:

- Changing distributed-mode worker or background entrypoints.
- Changing `POST /api/v0/workers` semantics (still refuses local mode).
- Adding a graceful drain timeout for in-flight tasks on shutdown — recorded for `docs/future.md` if the immediate-cancel behavior becomes painful.
- Adding a single-instance file lock for local mode — the chdb file lock is already a hard guard; an explicit lock can be added later if the diagnostic is unfriendly.
- Authentication changes (the existing `AAICLICK_API_TOKEN` static-token check still applies, unchanged).

# Architecture

```
                ┌──────────────────────────────────────────────┐
                │ aaiclick/server/app.py                       │
                │                                              │
   local start ─┼─► uvicorn ──► rest_app (FastAPI, no MCP)     │
                │                  │                           │
local-mcp start ┼─► uvicorn ──► mcp_app (FastMCP HTTP)         │
                │                  │                           │
 uvicorn server ┼─► uvicorn ──► app (FastAPI + mounted MCP)    │
                │                  │                           │
                │                  ▼ shared lifespan           │
                │   if is_local(): async with local_runtime()  │
                │   else:           yield                      │
                └──────────────────────────────────────────────┘
                                   │
                                   ▼
                ┌──────────────────────────────────────────────┐
                │ aaiclick/orchestration/local_runtime.py      │
                │                                              │
                │   1. setup() if not is_setup_done()          │
                │   2. BackgroundWorker.start()                │
                │   3. async with orch_context(with_ch=True):  │
                │       4. asyncio.create_task(                │
                │              worker_main_loop(               │
                │                install_signal_handlers=False)│
                │          )                                   │
                │       yield                                  │
                │       5. cancel worker task                  │
                │   6. BackgroundWorker.stop()                 │
                └──────────────────────────────────────────────┘
```

**Key properties:**

- One source of truth for "what runs alongside the server in local mode": `local_runtime()`.
- Mode gating happens in the lifespan, not in `local_runtime()` itself — the helper is strict and refuses to run in distributed mode (clear error if misused).
- The per-request `orch_context()` calls in routers and MCP tools nest into the lifespan-level `orch_context(with_ch=True)` — no second chdb client is created per request.
- Three local entrypoints, one code path. Picking the entrypoint changes which surfaces are exposed; the worker wiring is identical.

# Entrypoint matrix

| Command                                          | ASGI app served    | Surfaces            | Mode        |
|--------------------------------------------------|--------------------|---------------------|-------------|
| `python -m aaiclick local start`                 | `rest_app`         | REST + workers      | local only  |
| `python -m aaiclick local-mcp start`             | `mcp_app`          | MCP + workers       | local only  |
| `uvicorn aaiclick.server.app:app`                | `app` (combined)   | REST + MCP + workers | local or distributed |
| `python -m aaiclick worker start`                | (none)             | workers only        | distributed only |
| `python -m aaiclick background start`            | (none)             | background only     | distributed only |

In distributed mode, `local start` and `local-mcp start` raise `RuntimeError` with the same message style today's `worker start` and `background start` use when called in local mode.

# File / module layout

```
aaiclick/
  orchestration/
    local_runtime.py        ← NEW: local_runtime() async context manager
    test_local_runtime.py   ← NEW: helper-level tests
    cli.py                  ← MODIFIED:
                                start_local()       — rewritten: binds rest_app via uvicorn
                                start_local_mcp()   — NEW: binds mcp_app via uvicorn
                                start_worker()      — unchanged (distributed)
                                start_background()  — unchanged (distributed)
    background/
      background_worker.py  ← unchanged
    execution/
      worker.py             ← unchanged
      mp_worker.py          ← unchanged
  server/
    app.py                  ← MODIFIED: exports rest_app, mcp_app, app + shared lifespan
    test_app.py             ← MODIFIED: cover lifespan behaviour
    test_mcp.py             ← MODIFIED: lifespan smoke test against mcp_app
  __main__.py               ← MODIFIED:
                                local start         — wires to rewritten start_local
                                local-mcp start     — NEW subcommand group
                                worker start        — unchanged
                                background start    — unchanged
```

`start_background()` and `worker start` remain the supported distributed-mode entrypoints. The lifespan is a no-op in distributed mode, so the server process and these worker processes coexist exactly as they do today.

# `local_runtime()` semantics

```python
# aaiclick/orchestration/local_runtime.py
from contextlib import asynccontextmanager

@asynccontextmanager
async def local_runtime() -> AsyncIterator[None]:
    if not is_local():
        raise RuntimeError(
            "local_runtime() requires local mode (chdb + sqlite). "
            "In distributed mode, run `worker start` and `background start` "
            "as separate processes."
        )
    if not is_setup_done():
        render_setup_result(setup())

    background = BackgroundWorker()
    await background.start()
    try:
        async with orch_context(with_ch=True):
            worker_task = asyncio.create_task(
                worker_main_loop(install_signal_handlers=False)
            )
            try:
                yield
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task
    finally:
        await background.stop()
```

**Decisions baked in:**

- **Auto-setup** mirrors today's `start_local()` behavior. A user running `local-mcp start` for the first time gets the SQL migrations applied automatically.
- **Mode guard** hard-fails if `is_local()` is false. Tests and production callers (the lifespan) won't reach this branch — but the guard makes accidental misuse loud.
- **`install_signal_handlers=False`** on `worker_main_loop`: the outer process owns SIGTERM / SIGINT. uvicorn handles signals for the server entrypoints; the standalone `local start` standalone path (if ever revived) would wrap the helper with its own signal-driven `asyncio.Event`.
- **Shutdown order** — cancel worker → stop background → exit `orch_context`. Matches today's `start_local()` ordering.
- **In-flight task on shutdown** — cancel immediately. `asyncio.CancelledError` propagates through the running task; existing `_handle_task_result` / `_set_pending_cleanup` paths transition the task to `PENDING_CLEANUP`. On the next process boot, the BackgroundWorker either retries the task (attempts remaining) or fails it. A drain-with-timeout variant is deferred to `docs/future.md`.

# Server-side wiring

`aaiclick/server/app.py` exports three ASGI apps. The shared `lifespan` is reused; the combined `app` chains FastMCP's existing lifespan with `local_runtime()`:

```python
# aaiclick/server/app.py
@asynccontextmanager
async def lifespan(app) -> AsyncIterator[None]:
    if is_local():
        async with local_runtime():
            yield
    else:
        yield

# 1. REST only
rest_app = FastAPI(title="aaiclick (REST)", ..., lifespan=lifespan)
for router in (jobs.router, registered_jobs.router, tasks.router,
               workers.router, objects.router):
    rest_app.include_router(router, prefix=API_PREFIX)
register_exception_handlers(rest_app)

# 2. MCP only — FastMCP HTTP surface, lifespan injected at construction
mcp_app = mcp.http_app(path="/", lifespan=lifespan)

# 3. Combined — REST + mounted MCP. Chains FastMCP's own lifespan with ours.
_combined_mcp_app = mcp.http_app(path="/")

@asynccontextmanager
async def _combined_lifespan(app) -> AsyncIterator[None]:
    async with _combined_mcp_app.lifespan(app):
        if is_local():
            async with local_runtime():
                yield
        else:
            yield

app = FastAPI(title="aaiclick", ..., lifespan=_combined_lifespan)
for router in (...):
    app.include_router(router, prefix=API_PREFIX)
register_exception_handlers(app)
app.mount(MCP_PATH, _combined_mcp_app)
```

**Authentication** (existing `require_bearer` / ASGI middleware) is wired identically on `rest_app` and `app`. `mcp_app` (standalone) gets the same ASGI middleware that the combined app's `/mcp` mount uses today.

# CLI changes

`aaiclick/orchestration/cli.py`:

```python
async def start_local(host: str = "127.0.0.1", port: int = 8000) -> None:
    """Run the REST surface + workers in a single local-mode process."""
    if not is_local():
        raise RuntimeError(
            "'local start' requires local mode (chdb + SQLite). "
            "Use `worker start` + `background start` + `uvicorn aaiclick.server.app:app` "
            "in distributed mode."
        )
    from aaiclick.server.app import rest_app
    import uvicorn
    config = uvicorn.Config(rest_app, host=host, port=port, log_level="info")
    await uvicorn.Server(config).serve()


async def start_local_mcp(host: str = "127.0.0.1", port: int = 8000) -> None:
    """Run the MCP surface + workers in a single local-mode process."""
    if not is_local():
        raise RuntimeError(
            "'local-mcp start' requires local mode (chdb + SQLite). "
            "Use `worker start` + `background start` in distributed mode."
        )
    from aaiclick.server.app import mcp_app
    import uvicorn
    config = uvicorn.Config(mcp_app, host=host, port=port, log_level="info")
    await uvicorn.Server(config).serve()
```

Both verbs lazy-import `aaiclick.server.app` so users without the `[server]` extra installed get a clear `ImportError → "install aaiclick[server]"` message rather than a load-time crash.

`aaiclick/__main__.py` adds a new `local-mcp` subcommand group (mirroring the existing `local` group's `start` action), and the existing `local start` action is rewritten to call the new `start_local()` (no body changes for `local-mcp start` other than wiring the new function).

# Auth and `/api/v0/workers`

- `AAICLICK_API_TOKEN` static-token check is applied to `rest_app`, `mcp_app`, and `app` identically — see existing wiring in `docs/api_server.md` ("Authentication").
- `POST /api/v0/workers` continues to refuse local mode (existing constraint). The endpoint exists on `rest_app` and `app` and behaves identically.

# Testing

- `aaiclick/orchestration/test_local_runtime.py` (new):
  - `local_runtime()` registers a Worker row, runs a submitted Job to COMPLETED, deregisters on exit.
  - `local_runtime()` raises `RuntimeError` outside local mode.
  - In-flight task cancellation: submit a long-running task, cancel the runtime mid-flight, observe the task transitions to `PENDING_CLEANUP`.
- `aaiclick/server/test_app.py` (extended):
  - Lifespan in local mode starts BackgroundWorker + execution worker (uses `httpx.AsyncClient` + `ASGITransport` with lifespan, asserts a worker row appears).
  - Lifespan in distributed mode is a no-op (no worker row appears).
- `aaiclick/server/test_mcp.py` (extended):
  - Same lifespan smoke test against `mcp_app`.
- Existing distributed-mode worker tests (`test_worker.py`, `test_mp_worker.py`, etc.) are untouched.

Tests follow the project's existing pattern (`async def test_*`, no class wrappers, fixtures from `aaiclick/server/conftest.py`).

# Failure modes

| Scenario                                              | Behavior                                                                 |
|-------------------------------------------------------|--------------------------------------------------------------------------|
| `local start` invoked in distributed mode             | `RuntimeError` at CLI entry, before uvicorn starts                       |
| `local-mcp start` invoked in distributed mode         | `RuntimeError` at CLI entry, before uvicorn starts                       |
| `aaiclick[server]` extra not installed, `local start` | `ImportError` with `"install aaiclick[server]"` hint                     |
| Two local-mode processes started concurrently         | Second process fails at chdb open with the existing file-lock error      |
| In-flight task when SIGTERM hits the server           | Task → `PENDING_CLEANUP`; retry-or-fail on next boot via existing path  |
| `BackgroundWorker.start()` fails during lifespan      | Lifespan startup fails → uvicorn aborts startup; user sees the traceback |
| `setup()` fails during auto-setup                     | Lifespan startup fails; same surface as above                            |

# Migration / backward compatibility

- **`local start` keeps the same CLI shape.** Today: workers only. After: workers + REST. Users who relied on the absence of an HTTP port now bind to `127.0.0.1:8000`. Documented in the changelog; no rollback flag.
- **`uvicorn aaiclick.server.app:app` becomes functional in local mode.** Today it's broken (no workers); after, it works. No code path is removed.
- **Distributed mode is byte-for-byte unchanged.**
