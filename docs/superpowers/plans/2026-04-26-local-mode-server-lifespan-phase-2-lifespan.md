# Phase 2 — Server lifespan wiring

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this phase task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the FastAPI app's lifespan with a chained variant that runs FastMCP's existing lifespan and (when `is_local()`) `local_runtime()`. After this phase, `uvicorn aaiclick.server.app:app` works in local mode — workers and background cleanup boot with the server.

**Spec:** `docs/superpowers/specs/2026-04-26-local-mode-server-lifespan-design.md` — §"Server-side wiring".

**Branch:** `claude/fastapi-lifespan-worker-X8KZm` (existing).

**Parent plan:** `2026-04-26-local-mode-server-lifespan.md`.

**Prerequisite:** Phase 1 complete (`local_runtime` is importable).

---

## Task 1: Replace `app.py`'s lifespan

**Files:**
- Modify: `aaiclick/server/app.py`

- [ ] **Step 1: Write the new file content**

Replace the entire contents of `aaiclick/server/app.py` with:

```python
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from aaiclick.backend import is_local
from aaiclick.orchestration.local_runtime import local_runtime

from .errors import register_exception_handlers
from .mcp import mcp
from .routers import jobs, objects, registered_jobs, tasks, workers

API_PREFIX = "/api/v0"
MCP_PATH = "/mcp"

# FastMCP's streamable-HTTP sub-app needs its lifespan to run; we chain
# it with local_runtime() (when in local mode) so workers come up with
# the server.
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

- [ ] **Step 2: Verify the module imports**

Run: `uv run --extra server --extra test python -c "from aaiclick.server.app import app, _lifespan; print(app)"`
Expected: prints the FastAPI instance, no `ImportError`.

- [ ] **Step 3: Commit**

```bash
git add aaiclick/server/app.py
git commit -m "$(cat <<'EOF'
feature: chain local_runtime() into the FastAPI lifespan

Replaces the previous lifespan (FastMCP's, forwarded as-is) with a
named _lifespan that runs FastMCP's startup, then — only in local
mode — wraps the request lifecycle in local_runtime() so the
background and execution workers boot with the server.

Distributed-mode behaviour is unchanged: the inner branch is a
plain `yield` and the existing `worker start` / `background start`
processes continue to run separately.
EOF
)"
```

---

## Task 2: Existing app tests still pass

- [ ] **Step 1: Run the existing server-app tests in local mode**

Run: `AAICLICK_SQL_URL='' AAICLICK_CH_URL='' uv run --extra server --extra test pytest aaiclick/server/test_app.py aaiclick/server/test_mcp.py -v`
Expected: all current tests still pass. The `app_client` fixture in `conftest.py` already enters the lifespan via `httpx.ASGITransport`; since `is_local()` is true, `local_runtime()` runs around every test request.

If a test that previously skipped the lifespan now hangs or fails, capture the failure and fix locally before continuing. Likely culprits: tests that assume no Worker/BackgroundWorker rows in the DB (the lifespan now creates a Worker row on entry).

- [ ] **Step 2: Run the same suite in distributed mode**

Run: `AAICLICK_SQL_URL='postgresql+asyncpg://aaiclick:secret@localhost:5432/aaiclick' AAICLICK_CH_URL='clickhouse://default@localhost:8123/default' uv run --extra server --extra distributed --extra test pytest aaiclick/server/test_app.py -v`

Expected: tests pass. If distributed services are unavailable in your environment, skip this step locally — CI will run the matrix.

- [ ] **Step 3: Commit any test fixes (if needed)**

If any existing test required adjustment to coexist with the new lifespan, commit with:

```bash
git commit -am "bugfix: <what was adjusted> for new lifespan"
```

---

## Task 3: New lifespan smoke test (local mode)

**Files:**
- Modify: `aaiclick/server/test_app.py`

- [ ] **Step 1: Inspect the existing fixture pattern**

Run: `cat aaiclick/server/conftest.py`
Note that `app_client` already enters the ASGI lifespan. New tests can rely on this.

- [ ] **Step 2: Append the lifespan smoke tests**

Append to `aaiclick/server/test_app.py`:

```python
from sqlmodel import select

from aaiclick.backend import is_local
from aaiclick.orchestration.models import Worker, WorkerStatus
from aaiclick.orchestration.orch_context import get_sql_session


async def test_lifespan_starts_worker_in_local_mode(app_client):
    """In local mode, the lifespan registers an execution Worker row."""
    if not is_local():
        pytest.skip("lifespan starts workers only in local mode")

    response = await app_client.get("/health")
    assert response.status_code == 200

    async with get_sql_session() as session:
        result = await session.execute(
            select(Worker).where(Worker.status == WorkerStatus.ACTIVE),
        )
        active_workers = result.scalars().all()

    assert active_workers, "expected at least one ACTIVE worker during lifespan"


async def test_lifespan_no_worker_in_distributed_mode(app_client):
    """In distributed mode, the lifespan is a no-op for workers."""
    if is_local():
        pytest.skip("verifies the distributed-mode no-op path")

    response = await app_client.get("/health")
    assert response.status_code == 200

    async with get_sql_session() as session:
        result = await session.execute(
            select(Worker).where(Worker.status == WorkerStatus.ACTIVE),
        )
        active_workers = result.scalars().all()

    assert not active_workers, (
        "no ACTIVE worker should be registered by the lifespan in distributed mode"
    )
```

If `pytest` is not yet imported at the top of `aaiclick/server/test_app.py`, add `import pytest`.

- [ ] **Step 3: Run the lifespan smoke tests in local mode**

Run: `AAICLICK_SQL_URL='' AAICLICK_CH_URL='' uv run --extra server --extra test pytest aaiclick/server/test_app.py::test_lifespan_starts_worker_in_local_mode aaiclick/server/test_app.py::test_lifespan_no_worker_in_distributed_mode -v`
Expected: `test_lifespan_starts_worker_in_local_mode` PASS; `test_lifespan_no_worker_in_distributed_mode` SKIPPED.

- [ ] **Step 4: Commit**

```bash
git add aaiclick/server/test_app.py
git commit -m "test: lifespan starts/skips workers based on mode"
```

---

## Task 4: Full server suite + lint

- [ ] **Step 1: Run the full server suite (local mode)**

Run: `AAICLICK_SQL_URL='' AAICLICK_CH_URL='' uv run --extra server --extra test pytest aaiclick/server/ -v`
Expected: all tests pass.

- [ ] **Step 2: Run lint and pyright**

Run: `uv run --extra server --extra test ruff check aaiclick/server/app.py aaiclick/server/test_app.py`
Run: `uv run --extra server --extra test pyright aaiclick/server/app.py aaiclick/server/test_app.py`
Expected: no errors.

- [ ] **Step 3: Push the branch**

```bash
git push origin claude/fastapi-lifespan-worker-X8KZm
```

- [ ] **Step 4: Run `/check-pr`**

```
/check-pr 262
```

Wait for CI. Diagnose and fix any matrix-specific failures (Server local, Server dist, Internal API local, etc.) before continuing.

- [ ] **Step 5: Run `/simplify`**

```
/simplify aaiclick/server/app.py aaiclick/server/test_app.py
```

Apply suggested improvements that fit the phase's scope. Commit and push.

- [ ] **Step 6: Confirm Phase 2 complete**

Phase 2 is complete when:
- `aaiclick/server/app.py` exposes the chained `_lifespan` and the existing `app` symbol still imports.
- `aaiclick/server/test_app.py` covers both local-mode and distributed-mode lifespan behaviour.
- CI is green on PR #262 across all matrix legs.

Move on to Phase 3: `2026-04-26-local-mode-server-lifespan-phase-3-cli-docs.md`.
