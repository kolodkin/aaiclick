Server-package guidelines
---

# Running the server

See `docs/api_server.md` — Running the server.

# FastAPI tests — use `httpx.AsyncClient` + `ASGITransport`

All router / app tests are `async def` (they depend on the async
`orch_ctx` fixture), so we follow FastAPI's async-tests pattern:

```python
import httpx
from aaiclick.server.app import app

transport = httpx.ASGITransport(app=app)
async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
    response = await client.get("/api/v0/jobs")
```

Reference: https://fastapi.tiangolo.com/advanced/async-tests/

- The shared `app_client` fixture in `aaiclick/server/conftest.py` yields
  an already-entered `httpx.AsyncClient` — depend on it rather than
  instantiating your own.
- Always `await` its methods (`await app_client.get(...)`).
- Do NOT use `fastapi.testclient.TestClient` here — it is the sync client
  and spawns a portal thread; our async tests need contextvars to stay on
  the test's event loop so the outer `orch_ctx` propagates into the
  per-request nested `orch_context`.

# `/api/v0` prefix

All routes mount under `API_PREFIX = "/api/v0"` (see `app.py`). Routers
declare their paths *relative* to the prefix (`APIRouter(prefix="/jobs")`)
— the version segment never appears in individual router files.

When writing tests, build URLs with the constant: `f"{API_PREFIX}/jobs"`,
not `"/api/v0/jobs"`. If the prefix ever graduates to `/api/v1`, the
constant update propagates automatically.

# `Problem` codes — use `ProblemCode` enum

Error responses carry a `ProblemCode` enum value (`NOT_FOUND`, `CONFLICT`,
`INVALID`) — never a raw string. Both `server/errors.py` and tests import
it from `aaiclick.view_models`. Adding a new error class is a one-line
edit to `_PROBLEM_MAP` in `errors.py`.

# Scope dependencies on routers

Single-scope routers declare the scope once at the `APIRouter(...)`
level via `dependencies=[Depends(orch_scope)]`; individual endpoints
drop the `_scope` parameter. Only `jobs.py` keeps per-endpoint
dependencies because it mixes `orch_scope` (reads) and
`orch_scope_with_ch` (`run_job`).

# Test scope

Router tests assert HTTP plumbing only — status codes, route registration,
error envelope shape (`Problem`), and that JSON bodies round-trip through
the declared `response_model`. Business-logic coverage lives in
`aaiclick/internal_api/test_*.py`; do not duplicate it here.
