from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI
from fastapi.staticfiles import StaticFiles

from aaiclick.auth import config, security, store
from aaiclick.auth.models import ROLE_ADMIN
from aaiclick.backend import is_local
from aaiclick.deploy.placeholders import require_no_placeholder_credentials
from aaiclick.orchestration.local_runtime import local_runtime
from aaiclick.orchestration.orch_context import OrchResources, current_orch_resources, orch_context
from aaiclick.view_models import API_VERSION

from .audit import AuditMiddleware
from .auth import AllowedHostMiddleware, PrincipalAuthMiddleware, require_principal, warn_if_open
from .deps import ORCH_STATE_KEY, SharedOrchMiddleware
from .errors import register_exception_handlers
from .events import live_events
from .events import router as events_router
from .mcp import mcp
from .routers import audit as audit_router
from .routers import auth as auth_router
from .routers import execution_workers, jobs, objects, registered_jobs, sandbox, tasks, viewer
from .routers import invites as invites_router
from .routers import users as users_router

API_PREFIX = f"/api/{API_VERSION}"
MCP_PATH = "/mcp"
STATIC_DIR = Path(__file__).parent / "static"

# FastMCP's streamable-HTTP sub-app needs its lifespan to run; we chain it
# with local_runtime() (when local) so execution workers come up with the server.
_mcp_app = mcp.http_app(path="/")


async def _seed_admin() -> None:
    """Insert the env-configured admin on first startup (empty users table)."""
    seed = config.admin_seed()
    if seed is None or not config.auth_enabled():
        return
    async with orch_context():
        if not await store.has_users():
            await store.create_user(
                username=seed.username,
                password_hash=security.hash_password(seed.password),
                role=ROLE_ADMIN,
            )


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[dict[str, OrchResources] | None]:
    warn_if_open()
    require_no_placeholder_credentials()
    if config.auth_enabled():
        # Fail at startup, not on the first login: a distributed server
        # without a signing secret would pass health and 500 every login.
        config.require_jwt_secret()
    await _seed_admin()
    async with _mcp_app.lifespan(app), live_events(app):
        if is_local():
            async with local_runtime():
                # Requests share this context: a per-request engine on local.db would race its pool.
                yield {ORCH_STATE_KEY: current_orch_resources()}
        else:
            yield


app = FastAPI(
    title="aaiclick",
    description="REST surface over aaiclick's internal_api. JWT auth + RBAC in distributed mode (see docs/designs/auth.md); open in local mode.",
    version=API_VERSION,
    docs_url=f"{API_PREFIX}/docs",
    redoc_url=f"{API_PREFIX}/redoc",
    openapi_url=f"{API_PREFIX}/openapi.json",
    lifespan=_lifespan,
)

register_exception_handlers(app)
app.add_middleware(AllowedHostMiddleware)
# Outside every route and mount so it sees the final status of every request, /mcp included.
app.add_middleware(AuditMiddleware)
# Around the audit write too, so it nests into the lifespan's orch_context.
app.add_middleware(SharedOrchMiddleware)

# Every resource router needs a principal; scope guards are declared per route.
for router in (
    jobs.router,
    registered_jobs.router,
    sandbox.router,
    tasks.router,
    objects.router,
    events_router,
    viewer.router,
):
    app.include_router(router, prefix=API_PREFIX, dependencies=[Depends(require_principal)])
app.include_router(execution_workers.router, prefix=API_PREFIX, dependencies=[Depends(require_principal)])

# `/auth` is public (login/refresh mint the credential); `/users` and `/audit`
# declare their own guards per route (`require_admin`). None takes the blanket
# dependency above.
app.include_router(auth_router.router, prefix=API_PREFIX)
app.include_router(users_router.router, prefix=API_PREFIX)
app.include_router(invites_router.router, prefix=API_PREFIX)
app.include_router(audit_router.router, prefix=API_PREFIX)

# `Depends` doesn't cross the mount boundary into the FastMCP sub-app, so the
# principal check runs as ASGI middleware wrapping the mount; per-tool RBAC
# runs inside FastMCP (see mcp_rbac.py).
app.mount(MCP_PATH, PrincipalAuthMiddleware(_mcp_app))


@app.get("/health", include_in_schema=False)
async def health() -> dict[str, str]:
    return {"status": "ok"}


# SPA: serve the Vite build (gitignored, produced by `npm run build`). Mounted
# last so the API routers, /mcp, and /health keep priority. `html=True` serves
# index.html at "/" and resolves hashed asset paths under /assets/*.
if STATIC_DIR.is_dir():
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="spa")
