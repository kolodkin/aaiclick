from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI
from fastapi.staticfiles import StaticFiles

from aaiclick.auth import config, security, store
from aaiclick.auth.models import ROLE_ADMIN
from aaiclick.backend import is_local
from aaiclick.orchestration.local_runtime import local_runtime
from aaiclick.orchestration.orch_context import orch_context

from .auth import AdminAuthMiddleware, require_principal, warn_if_open
from .errors import register_exception_handlers
from .mcp import mcp
from .routers import auth as auth_router
from .routers import execution_workers, jobs, objects, registered_jobs, tasks
from .routers import users as users_router

API_PREFIX = "/api/v0"
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
    async with orch_context(with_ch=False):
        if not await store.has_users():
            await store.create_user(
                username=seed.username,
                password_hash=security.hash_password(seed.password),
                role=ROLE_ADMIN,
            )


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    warn_if_open()
    await _seed_admin()
    async with _mcp_app.lifespan(app):
        if is_local():
            async with local_runtime():
                yield
        else:
            yield


app = FastAPI(
    title="aaiclick",
    description="REST surface over aaiclick's internal_api. JWT auth + RBAC in distributed mode (see docs/auth.md); open in local mode.",
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
    execution_workers.router,
    objects.router,
):
    app.include_router(router, prefix=API_PREFIX, dependencies=[Depends(require_principal)])

# `/auth` is public (login/refresh mint the credential); `/users` carries its
# own `require_admin`. Neither takes the blanket `require_principal` above.
app.include_router(auth_router.router, prefix=API_PREFIX)
app.include_router(users_router.router, prefix=API_PREFIX)

# `Depends` doesn't cross the mount boundary into the FastMCP sub-app, so the
# admin-only check runs as ASGI middleware wrapping the mount.
app.mount(MCP_PATH, AdminAuthMiddleware(_mcp_app))


@app.get("/health", include_in_schema=False)
async def health() -> dict[str, str]:
    return {"status": "ok"}


# SPA: serve the Vite build (gitignored, produced by `npm run build`). Mounted
# last so the API routers, /mcp, and /health keep priority. `html=True` serves
# index.html at "/" and resolves hashed asset paths under /assets/*.
if STATIC_DIR.is_dir():
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="spa")
