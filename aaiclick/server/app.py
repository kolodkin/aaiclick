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

# FastMCP's streamable-HTTP sub-app needs its lifespan to run; we chain it
# with local_runtime() (when local) so workers come up with the server.
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
