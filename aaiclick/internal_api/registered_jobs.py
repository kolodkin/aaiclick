"""Internal API for registered-job commands.

Each function runs inside an active ``orch_context()`` and reads the SQL
session via the contextvar getter. Returns pydantic view models.
"""

from __future__ import annotations

from sqlmodel import col

from aaiclick.orchestration.execution.runner import import_callback
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

from .errors import Conflict, Invalid, NotFound
from .pagination import paginate


def _validate_entrypoint(entrypoint: str) -> None:
    """Resolve ``entrypoint`` to a callable, raising on failure.

    Mirrors the worker's runtime resolution (``import_callback``) so a typo
    or a function/label mix-up surfaces at registration time rather than as
    a FAILED task on first run.

    Raises:
        NotFound: The module portion cannot be imported.
        Invalid: The entrypoint is malformed, names a missing attribute, or
            resolves to something that is not callable.
    """
    try:
        import_callback(entrypoint)
    except ModuleNotFoundError as exc:
        raise NotFound(f"entrypoint '{entrypoint}' module not found: {exc}") from exc
    except (ImportError, AttributeError, ValueError, TypeError) as exc:
        raise Invalid(f"entrypoint '{entrypoint}' does not resolve to a callable: {exc}") from exc


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

    Raises ``Conflict`` if a registration with the same name already exists,
    or ``NotFound`` / ``Invalid`` if ``entrypoint`` does not resolve to a
    callable (validated before persisting).

    Entrypoint validation is skipped for prebuilt-image registrations
    (``image`` set): the entrypoint resolves inside the image at run time
    (module entry) or is unused (shell entry), so it is not importable on the
    host doing the registration.
    """
    if request.image is None:
        _validate_entrypoint(request.entrypoint)
    try:
        registered = await _register_job_impl(
            name=request.name,
            entrypoint=request.entrypoint,
            schedule=request.schedule,
            default_kwargs=request.default_kwargs,
            enabled=request.enabled,
            preservation_mode=request.preservation_mode,
            runner_mode=request.runner_mode,
            dockerfile=request.dockerfile,
            git_remote=request.git_remote,
            image=request.image,
            kubernetes_config=request.kubernetes_config,
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
