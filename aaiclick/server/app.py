"""FastAPI application factory for the aaiclick REST surface.

All resource routes are mounted under ``API_PREFIX`` (``/api/v0``). The prefix
is intentionally ``v0`` — the schema is still experimental and may break;
graduate to ``/api/v1`` once the contract has stabilised and external callers
exist. The version lives in exactly one place so the bump is a single-line
edit here.
"""

from __future__ import annotations

from fastapi import FastAPI

from .errors import register_exception_handlers
from .routers import jobs, objects, registered_jobs, tasks, workers

API_PREFIX = "/api/v0"


def create_app() -> FastAPI:
    """Build a configured FastAPI app with every router mounted under ``/api/v0``."""
    app = FastAPI(
        title="aaiclick",
        description="REST surface over aaiclick's internal_api.",
        docs_url=f"{API_PREFIX}/docs",
        redoc_url=f"{API_PREFIX}/redoc",
        openapi_url=f"{API_PREFIX}/openapi.json",
    )

    register_exception_handlers(app)

    app.include_router(jobs.router, prefix=API_PREFIX)
    app.include_router(registered_jobs.router, prefix=API_PREFIX)
    app.include_router(tasks.router, prefix=API_PREFIX)
    app.include_router(workers.router, prefix=API_PREFIX)
    app.include_router(objects.router, prefix=API_PREFIX)

    @app.get("/health", include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
