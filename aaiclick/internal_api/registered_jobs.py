"""Internal API for registered-job commands.

Each function runs inside an active ``orch_context()`` and reads the SQL
session via the contextvar getter. Returns pydantic view models.
"""

from __future__ import annotations

from sqlmodel import col, func, select

from aaiclick.orchestration.models import RegisteredJob
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.registered_jobs import (
    RegisteredJobAlreadyExists,
    RegisteredJobNotFound,
)
from aaiclick.orchestration.registered_jobs import (
    disable_job as _disable_job_impl,
)
from aaiclick.orchestration.registered_jobs import (
    enable_job as _enable_job_impl,
)
from aaiclick.orchestration.registered_jobs import (
    register_job as _register_job_impl,
)
from aaiclick.orchestration.view_models import (
    RegisteredJobView,
    registered_job_to_view,
)
from aaiclick.view_models import Page, RegisteredJobFilter, RegisterJobRequest

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

    Raises ``Conflict`` if a registration with the same name already exists.
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
    except RegisteredJobAlreadyExists as exc:
        raise Conflict(str(exc)) from exc
    return registered_job_to_view(registered)


async def enable_job(name: str) -> RegisteredJobView:
    """Enable a registered job and recompute its next fire time.

    Raises ``NotFound`` if no registration matches ``name``.
    """
    try:
        registered = await _enable_job_impl(name)
    except RegisteredJobNotFound as exc:
        raise NotFound(str(exc)) from exc
    return registered_job_to_view(registered)


async def disable_job(name: str) -> RegisteredJobView:
    """Disable a registered job and clear its next fire time.

    Raises ``NotFound`` if no registration matches ``name``.
    """
    try:
        registered = await _disable_job_impl(name)
    except RegisteredJobNotFound as exc:
        raise NotFound(str(exc)) from exc
    return registered_job_to_view(registered)
