from __future__ import annotations

from fastapi import FastAPI

from .errors import register_exception_handlers
from .routers import jobs, objects, registered_jobs, tasks, workers

API_PREFIX = "/api/v0"

app = FastAPI(
    title="aaiclick",
    docs_url=f"{API_PREFIX}/docs",
    redoc_url=f"{API_PREFIX}/redoc",
    openapi_url=f"{API_PREFIX}/openapi.json",
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


@app.get("/health", include_in_schema=False)
async def health() -> dict[str, str]:
    return {"status": "ok"}
