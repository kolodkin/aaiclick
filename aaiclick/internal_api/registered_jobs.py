"""Internal API for registered-job commands.

Each function runs inside an active ``orch_context()`` and reads the SQL
session via the contextvar getter. Returns pydantic view models.
"""

from __future__ import annotations

from sqlmodel import col

from aaiclick.orchestration.models import RegisteredJob
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
from .pagination import paginate


async def list_registered_jobs(filter: RegisteredJobFilter | None = None) -> Page[RegisteredJobView]:
    """Return a page of registered jobs ordered by ``name``.

    ``filter.name`` is matched with SQL ``LIKE`` (caller supplies wildcards).
    ``filter.enabled`` restricts to enabled / disabled entries when set.
    Pagination uses ``filter.limit`` / ``filter.offset``; ``filter.cursor``
    is reserved for a future cursor-based REST/MCP surface and is currently
    ignored.
    """
    filter = filter or RegisteredJobFilter()

    predicates = []
    if filter.enabled is not None:
        predicates.append(RegisteredJob.enabled == filter.enabled)
    if filter.name is not None:
        predicates.append(col(RegisteredJob.name).like(filter.name))

    page = await paginate(
        RegisteredJob,
        where=predicates,
        order_by=col(RegisteredJob.name).asc(),
        limit=filter.limit,
        offset=filter.offset,
    )
    return Page[RegisteredJobView](
        items=[registered_job_to_view(rj) for rj in page.rows],
        total=page.total,
    )


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
            preserve_all=request.preserve_all,
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
