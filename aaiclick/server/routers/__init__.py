"""FastAPI routers — one per ``internal_api`` command group.

Each router declares paths *relative* to ``/api/v0`` — the versioned prefix
is applied in ``server/app.py`` via ``app.include_router(..., prefix=API_PREFIX)``
so the version lives in exactly one place.
"""

from . import jobs, objects, registered_jobs, tasks, workers
