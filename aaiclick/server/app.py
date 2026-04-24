from __future__ import annotations

from fastapi import FastAPI

from .errors import register_exception_handlers
from .mcp import mcp
from .routers import jobs, objects, registered_jobs, tasks, workers

API_PREFIX = "/api/v0"
MCP_PATH = "/mcp"

# FastMCP's streamable-HTTP sub-app needs its lifespan to run; forward it onto FastAPI.
_mcp_app = mcp.http_app(path="/")

app = FastAPI(
    title="aaiclick",
    description="REST surface over aaiclick's internal_api. Localhost-only, unauthenticated (v0).",
    version="0.0.0",
    docs_url=f"{API_PREFIX}/docs",
    redoc_url=f"{API_PREFIX}/redoc",
    openapi_url=f"{API_PREFIX}/openapi.json",
    lifespan=_mcp_app.lifespan,
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
