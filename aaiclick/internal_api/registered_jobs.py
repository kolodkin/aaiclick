"""Internal API for registered-job commands.

Every function runs inside an active ``orch_context()`` and reads the SQL
resource via the contextvar getter (``get_sql_session``). Returns pydantic
view models from ``aaiclick.orchestration.view_models``.

The heavy CRUD lives in ``aaiclick.orchestration.registered_jobs``; this
module is a thin pagination + error-translation layer that returns views
instead of SQLModel rows.
"""

from __future__ import annotations

from sqlmodel import col, func, select

from aaiclick.orchestration.models import RegisteredJob
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.registered_jobs import (
    disable_job as _disable_job_impl,
)
from aaiclick.orchestration.registered_jobs import (
    enable_job as _enable_job_impl,
)
from aaiclick.orchestration.registered_jobs import (
    get_registered_job,
)
from aaiclick.orchestration.registered_jobs import (
    register_job as _register_job_impl,
)
from aaiclick.orchestration.view_models import (
    RegisteredJobView,
    registered_job_to_view,
)
from aaiclick.view_models import Page, RegisterJobRequest, RegisteredJobFilter

from .errors import Conflict, NotFound


async def list_registered_jobs(filter: RegisteredJobFilter | None = None) -> Page[RegisteredJobView]:
    """Return a page of registered jobs ordered by ``name``.

    ``filter.name`` is matched with SQL ``LIKE`` (caller supplies wildcards).
    ``filter.enabled`` restricts to enabled / disabled entries when set.
    Pagination uses ``filter.limit`` / ``filter.offset``; ``filter.cursor``
    is reserved for a future cursor-based REST/MCP surface and is currently
    ignored.
    """
    filter = filter or RegisteredJobFilter()

    count_query = select(func.count()).select_from(RegisteredJob)
    list_query = select(RegisteredJob)
    if filter.enabled is not None:
        count_query = count_query.where(RegisteredJob.enabled == filter.enabled)
        list_query = list_query.where(RegisteredJob.enabled == filter.enabled)
    if filter.name is not None:
        count_query = count_query.where(col(RegisteredJob.name).like(filter.name))
        list_query = list_query.where(col(RegisteredJob.name).like(filter.name))

    list_query = list_query.order_by(col(RegisteredJob.name)).limit(filter.limit).offset(filter.offset)
    async with get_sql_session() as session:
        total = (await session.execute(count_query)).scalar_one()
        rows = (await session.execute(list_query)).scalars().all()

    return Page[RegisteredJobView](items=[registered_job_to_view(rj) for rj in rows], total=total)


async def register_job(request: RegisterJobRequest) -> RegisteredJobView:
    """Register a new job in the catalog.

    Raises ``Conflict`` if a registration with the same name already exists —
    the CLI's ``register-job`` verb surfaces this so a user who wants an
    update-in-place uses a separate verb (future) instead of silently
    clobbering the existing entry.
    """
    try:
        registered = await _register_job_impl(
            name=request.name,
            entrypoint=request.entrypoint,
            schedule=request.schedule,
            default_kwargs=request.default_kwargs,
            enabled=request.enabled,
            preservation_mode=request.preservation_mode,
        )
    except ValueError as exc:
        if "already exists" in str(exc):
            raise Conflict(str(exc)) from exc
        raise
    return registered_job_to_view(registered)


async def enable_job(name: str) -> RegisteredJobView:
    """Enable a registered job and recompute its next fire time.

    Raises ``NotFound`` if no registration matches ``name``.
    """
    try:
        await _enable_job_impl(name)
    except ValueError as exc:
        if "not found" in str(exc):
            raise NotFound(str(exc)) from exc
        raise

    refreshed = await get_registered_job(name)
    if refreshed is None:
        raise RuntimeError(f"Registered job '{name}' disappeared after enable")
    return registered_job_to_view(refreshed)


async def disable_job(name: str) -> RegisteredJobView:
    """Disable a registered job and clear its next fire time.

    Raises ``NotFound`` if no registration matches ``name``.
    """
    try:
        await _disable_job_impl(name)
    except ValueError as exc:
        if "not found" in str(exc):
            raise NotFound(str(exc)) from exc
        raise

    refreshed = await get_registered_job(name)
    if refreshed is None:
        raise RuntimeError(f"Registered job '{name}' disappeared after disable")
    return registered_job_to_view(refreshed)
