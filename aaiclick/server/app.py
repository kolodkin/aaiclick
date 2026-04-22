from __future__ import annotations

from fastapi import FastAPI

from .errors import register_exception_handlers
from .routers import jobs, objects, registered_jobs, tasks, workers

API_PREFIX = "/api/v0"


def create_app() -> FastAPI:
    app = FastAPI(
        title="aaiclick",
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
