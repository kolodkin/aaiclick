Local-mode Server Lifespan
---

# Motivation

Today, running the FastAPI / MCP server in local mode (`chdb` + `sqlite`) is broken: jobs submitted via `POST /api/v0/jobs:run` (or the MCP `run_job` tool) are written to the database but never picked up — there is no execution worker and no background worker in the server process. The only working local-mode entrypoint is `python -m aaiclick local start`, which runs workers but exposes no HTTP / MCP surface.

This design wires the workers into the existing combined FastAPI + FastMCP server's `lifespan`, gated on `is_local()`. After the change, the single combined ASGI app at `aaiclick.server.app:app` runs REST, MCP, and the local-mode workers in one process. The CLI verb `local start` becomes a thin wrapper that launches uvicorn against that app. Distributed mode is unchanged: the lifespan no-ops, and `worker start` / `background start` still run as separate processes.

The single-process constraint of local mode (chdb's file lock) is honoured naturally: the workers and the per-request handlers share one chdb session inside one outer `orch_context(with_ch=True)`.

# Scope

In scope:

- A new shared async context manager `local_runtime()` that starts and stops the BackgroundWorker and execution `worker_main_loop` for the duration of its block.
- A replacement `lifespan` for the combined ASGI app `aaiclick.server.app:app` that chains FastMCP's existing lifespan with `local_runtime()` when `is_local()` is true.
- Rewriting `start_local()` (CLI: `python -m aaiclick local start`) to launch uvicorn against the combined app.
- Tests covering the lifespan path (worker / background startup, in-flight task cancel on shutdown) and the rewritten CLI verb.

Out of scope:

- Splitting the server into REST-only / MCP-only flavours — the combined app is preferred today and serves both surfaces in one process, which is the only way to expose both in local mode (single chdb lock).
- Changing distributed-mode worker or background entrypoints.
- Changing `POST /api/v0/workers` semantics (still refuses local mode).
- Adding a graceful drain timeout for in-flight tasks on shutdown — recorded for `docs/future.md` if the immediate-cancel behaviour becomes painful.
- Adding a single-instance file lock for local mode — chdb's file lock is already a hard guard; an explicit lock can be added later if the diagnostic is unfriendly.
- Authentication changes (the existing `AAICLICK_API_TOKEN` static-token check still applies, unchanged).

# Architecture

```
                ┌──────────────────────────────────────────────┐
                │ aaiclick/server/app.py                       │
                │                                              │
   local start ─┼─► uvicorn ──┐                                │
                │             ▼                                │
 uvicorn server ┼─► uvicorn ──► app (FastAPI + MCP mount)      │
                │                  │                           │
                │                  ▼ chained lifespan          │
                │   async with _mcp_app.lifespan(app):         │
                │       if is_local():                         │
                │           async with local_runtime(): yield  │
                │       else:                                  │
                │           yield                              │
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
- One ASGI surface (`app`), one CLI verb (`local start`). No new modules added to `aaiclick/server/`.
- The per-request `orch_context()` calls in routers and MCP tools nest into the lifespan-level `orch_context(with_ch=True)` — no second chdb client is created per request.
- The combined app preserves shared auth middleware, single-port deployment, and the existing `/api/v0/*` + `/mcp/*` mount.

# Entrypoint matrix

| Command                                          | ASGI app served    | Surfaces            | Mode        |
|--------------------------------------------------|--------------------|---------------------|-------------|
| `python -m aaiclick local start`                 | `app`              | REST + MCP + workers | local only |
| `uvicorn aaiclick.server.app:app`                | `app`              | REST + MCP (+ workers if local) | local or distributed |
| `python -m aaiclick worker start`                | (none)             | workers only        | distributed only |
| `python -m aaiclick background start`            | (none)             | background only     | distributed only |

In distributed mode, `local start` raises `RuntimeError` at CLI entry — the same way today's `worker start` and `background start` raise when called in local mode.

# File / module layout

```
aaiclick/
  orchestration/
    local_runtime.py        ← NEW: local_runtime() async context manager
    test_local_runtime.py   ← NEW: helper-level tests
    cli.py                  ← MODIFIED:
                                start_local()       — rewritten: launches uvicorn against app
                                start_worker()      — unchanged (distributed)
                                start_background()  — unchanged (distributed)
    background/
      background_worker.py  ← unchanged
    execution/
      worker.py             ← unchanged
      mp_worker.py          ← unchanged
  server/
    app.py                  ← MODIFIED: replace lifespan with chained variant
    mcp.py                  ← unchanged
    test_app.py             ← MODIFIED: cover lifespan behaviour in both modes
  __main__.py               ← MODIFIED: `local start` help text reflects new behaviour;
                                         remove the `--max-tasks` flag (uvicorn-managed lifecycle)
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

- **Auto-setup** mirrors today's `start_local()` behaviour. A user running `local start` for the first time gets the SQL migrations applied automatically.
- **Mode guard** hard-fails if `is_local()` is false. Tests and production callers (the lifespan) won't reach this branch — but the guard makes accidental misuse loud.
- **`install_signal_handlers=False`** on `worker_main_loop`: uvicorn owns SIGTERM / SIGINT and triggers lifespan shutdown; the worker should not steal signals.
- **Shutdown order** — cancel worker → stop background → exit `orch_context`. Matches today's `start_local()` ordering.
- **In-flight task on shutdown** — cancel immediately. `asyncio.CancelledError` propagates through the running task; existing `_handle_task_result` / `_set_pending_cleanup` paths transition the task to `PENDING_CLEANUP`. On the next process boot, the BackgroundWorker either retries the task (attempts remaining) or fails it. A drain-with-timeout variant is deferred to `docs/future.md`.

# Server-side wiring

`aaiclick/server/app.py` — the lifespan changes from "FastMCP's lifespan only" to "FastMCP's lifespan + (when local) `local_runtime()`":

```python
# aaiclick/server/app.py
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from fastapi import FastAPI

from aaiclick.backend import is_local
from aaiclick.orchestration.local_runtime import local_runtime

from .errors import register_exception_handlers
from .mcp import mcp
from .routers import jobs, objects, registered_jobs, tasks, workers

API_PREFIX = "/api/v0"
MCP_PATH = "/mcp"

_mcp_app = mcp.http_app(path="/")


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with _mcp_app.lifespan(app):
        if is_local():
            async with local_runtime():
                yield
        else:
            yield


app = FastAPI(
    title="aaiclick",
    description="REST surface over aaiclick's internal_api. Localhost-only, unauthenticated (v0).",
    version="0.0.0",
    docs_url=f"{API_PREFIX}/docs",
    redoc_url=f"{API_PREFIX}/redoc",
    openapi_url=f"{API_PREFIX}/openapi.json",
    lifespan=_lifespan,
)

register_exception_handlers(app)

for router in (
    jobs.router,
    registered_jobs.router,
    tasks.router,
    workers.router,
    objects.router,
):
    app.include_router(router, prefix=API_PREFIX)

app.mount(MCP_PATH, _mcp_app)


@app.get("/health", include_in_schema=False)
async def health() -> dict[str, str]:
    return {"status": "ok"}
```

The only structural difference from today is the named `_lifespan` callable that wraps `_mcp_app.lifespan(app)` with a conditional `local_runtime()` block.

**Authentication** (existing `require_bearer` / ASGI middleware) is unchanged.

# CLI changes

`aaiclick/orchestration/cli.py` — `start_local()` is rewritten; `start_worker()` and `start_background()` are unchanged:

```python
async def start_local(host: str = "127.0.0.1", port: int = 8000) -> None:
    """Run the combined REST + MCP server with workers in a single local-mode process."""
    if not is_local():
        raise RuntimeError(
            "'local start' requires local mode (chdb + SQLite). "
            "Use `worker start` + `background start` + "
            "`uvicorn aaiclick.server.app:app` in distributed mode."
        )
    from aaiclick.server.app import app
    import uvicorn
    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    await uvicorn.Server(config).serve()
```

The lazy import of `aaiclick.server.app` keeps users without the `[server]` extra installed off the import path until they actually run `local start`. If the extra is missing, the `ImportError` carries an "install aaiclick[server]" hint in its message.

`aaiclick/__main__.py`:

- The existing `local start` subcommand stays. Its `--max-tasks` argument is removed: with uvicorn driving the process lifecycle, the worker loop runs unbounded until SIGTERM, and `--max-tasks` was a CLI-only test affordance that no longer fits.
- Help text updates: `Start worker + background in a single process` → `Start the combined REST + MCP server with workers (local mode)`.
- No new subcommand groups.

# Auth and `/api/v0/workers`

- `AAICLICK_API_TOKEN` static-token check applies to `app` exactly as today.
- `POST /api/v0/workers` continues to refuse local mode (existing constraint, unchanged).

# Testing

- `aaiclick/orchestration/test_local_runtime.py` (new):
  - `local_runtime()` registers a Worker row, runs a submitted Job to COMPLETED, deregisters on exit.
  - `local_runtime()` raises `RuntimeError` outside local mode (skipped under non-local CI matrix).
  - In-flight task cancellation: submit a long-running task, cancel the runtime mid-flight, observe the task transitions to `PENDING_CLEANUP`.
- `aaiclick/server/test_app.py` (extended):
  - Lifespan in local mode starts BackgroundWorker + execution worker (uses `httpx.AsyncClient` + `ASGITransport`, asserts a worker row appears during the request lifecycle).
  - Lifespan in distributed mode is a no-op (no worker row appears).
- Existing distributed-mode worker tests (`test_worker.py`, `test_mp_worker.py`, etc.) are untouched.

Tests follow the project's existing pattern (`async def test_*`, no class wrappers, fixtures from `aaiclick/server/conftest.py` and `aaiclick/orchestration/conftest.py`).

# Failure modes

| Scenario                                              | Behaviour                                                                |
|-------------------------------------------------------|--------------------------------------------------------------------------|
| `local start` invoked in distributed mode             | `RuntimeError` at CLI entry, before uvicorn starts                       |
| `aaiclick[server]` extra not installed, `local start` | `ImportError` with `"install aaiclick[server]"` hint                     |
| Two local-mode processes started concurrently         | Second process fails at chdb open with the existing file-lock error      |
| In-flight task when SIGTERM hits the server           | Task → `PENDING_CLEANUP`; retry-or-fail on next boot via existing path  |
| `BackgroundWorker.start()` fails during lifespan      | Lifespan startup fails → uvicorn aborts startup; user sees the traceback |
| `setup()` fails during auto-setup                     | Lifespan startup fails; same surface as above                            |
| FastMCP startup fails inside `_lifespan`              | Outer `_mcp_app.lifespan(app)` propagates; `local_runtime()` never enters |

# Migration / backward compatibility

- **`local start` keeps the same CLI shape.** Today: workers only. After: REST + MCP + workers. Users who relied on the absence of an HTTP port now bind to `127.0.0.1:8000`. Documented in the changelog; no rollback flag.
- **`local start --max-tasks`** is removed. It was a debug-only affordance; production callers never set it.
- **`uvicorn aaiclick.server.app:app` becomes functional in local mode.** Today it's broken (no workers); after, it works. No code path is removed.
- **Distributed mode is byte-for-byte unchanged.**
