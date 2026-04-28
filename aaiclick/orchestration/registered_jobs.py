"""CRUD operations for registered jobs."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from croniter import croniter
from sqlmodel import select

from ..snowflake import get_snowflake_id
from .factories import _UNSET, create_job, create_task, resolve_preserve
from .models import Job, PreservationMode, Preserve, RegisteredJob, RunType
from .orch_context import get_sql_session


class RegisteredJobAlreadyExists(ValueError):
    """Raised when registering a name that already exists."""


class RegisteredJobNotFound(ValueError):
    """Raised when enabling/disabling a non-existent registration."""


def compute_next_run(cron_expr: str, after: datetime | None = None) -> datetime:
    """Compute the next fire time for a cron expression.

    Args:
        cron_expr: Cron expression (e.g. "0 8 * * *")
        after: Base time to compute from (default: utcnow)

    Returns:
        Next fire datetime
    """
    base = after or datetime.utcnow()
    return croniter(cron_expr, base).get_next(datetime)


def _next_run_at(schedule: str | None, enabled: bool, now: datetime) -> datetime | None:
    """Compute next_run_at from schedule if enabled, else None."""
    return compute_next_run(schedule, now) if schedule and enabled else None


async def register_job(
    *,
    name: str,
    entrypoint: str,
    schedule: str | None = None,
    default_kwargs: dict[str, Any] | None = None,
    enabled: bool = True,
    preservation_mode: PreservationMode | None = None,
    preserve: Preserve = None,
) -> RegisteredJob:
    """Register a new job in the catalog.

    Args:
        name: Unique job name
        entrypoint: Python dotted path (e.g. "myapp.pipelines.etl_job")
        schedule: Cron expression for scheduled runs (optional)
        default_kwargs: Default kwargs for scheduled runs (optional)
        enabled: Whether the job is enabled (default: True)
        preservation_mode: Legacy preservation mode (kept until Phase 6).
        preserve: Default preserve declaration for every run; individual
            runs override via ``run_job(..., preserve=...)``.

    Returns:
        Created RegisteredJob

    Raises:
        RegisteredJobAlreadyExists: If a job with this name already exists.
    """
    now = datetime.utcnow()
    registered_job = RegisteredJob(
        id=get_snowflake_id(),
        name=name,
        entrypoint=entrypoint,
        enabled=enabled,
        schedule=schedule,
        default_kwargs=default_kwargs,
        preservation_mode=preservation_mode,
        preserve=resolve_preserve(explicit=preserve, registered=None),
        next_run_at=_next_run_at(schedule, enabled, now),
        created_at=now,
        updated_at=now,
    )

    async with get_sql_session() as session:
        existing = await session.execute(select(RegisteredJob).where(RegisteredJob.name == name))
        if existing.scalar_one_or_none() is not None:
            raise RegisteredJobAlreadyExists(f"Registered job '{name}' already exists")

        session.add(registered_job)
        await session.commit()
        await session.refresh(registered_job)

    return registered_job


async def get_registered_job(name: str) -> RegisteredJob | None:
    """Look up a registered job by name.

    Args:
        name: Job name

    Returns:
        RegisteredJob if found, None otherwise
    """
    async with get_sql_session() as session:
        result = await session.execute(select(RegisteredJob).where(RegisteredJob.name == name))
        return result.scalar_one_or_none()


async def upsert_registered_job(
    *,
    name: str,
    entrypoint: str,
    schedule: str | None = None,
    default_kwargs: dict[str, Any] | None = None,
    enabled: bool = True,
    preservation_mode: PreservationMode | None = None,
    preserve: Preserve = None,
) -> RegisteredJob:
    """Insert or update a registered job.

    If a job with the given name exists, updates entrypoint, schedule,
    default_kwargs, preservation_mode, preserve, and enabled.
    Otherwise creates a new entry.
    """
    now = datetime.utcnow()
    normalized_preserve = resolve_preserve(explicit=preserve, registered=None)

    async with get_sql_session() as session:
        result = await session.execute(select(RegisteredJob).where(RegisteredJob.name == name))
        existing = result.scalar_one_or_none()

        if existing is not None:
            existing.entrypoint = entrypoint
            existing.schedule = schedule
            existing.default_kwargs = default_kwargs
            existing.preservation_mode = preservation_mode
            existing.preserve = normalized_preserve
            existing.enabled = enabled
            existing.updated_at = now
            existing.next_run_at = _next_run_at(schedule, enabled, now)
            session.add(existing)
            await session.commit()
            await session.refresh(existing)
            return existing

        registered_job = RegisteredJob(
            id=get_snowflake_id(),
            name=name,
            entrypoint=entrypoint,
            enabled=enabled,
            schedule=schedule,
            default_kwargs=default_kwargs,
            preservation_mode=preservation_mode,
            preserve=normalized_preserve,
            next_run_at=_next_run_at(schedule, enabled, now),
            created_at=now,
            updated_at=now,
        )
        session.add(registered_job)
        await session.commit()
        await session.refresh(registered_job)
        return registered_job


async def enable_job(name: str) -> RegisteredJob:
    """Enable a registered job and recompute next_run_at.

    Args:
        name: Job name

    Returns:
        The enabled RegisteredJob

    Raises:
        RegisteredJobNotFound: If no job with this name exists
    """
    now = datetime.utcnow()

    async with get_sql_session() as session:
        result = await session.execute(select(RegisteredJob).where(RegisteredJob.name == name))
        job = result.scalar_one_or_none()
        if job is None:
            raise RegisteredJobNotFound(f"Registered job '{name}' not found")

        job.enabled = True
        job.updated_at = now
        job.next_run_at = _next_run_at(job.schedule, True, now)
        session.add(job)
        await session.commit()
        await session.refresh(job)
        return job


async def disable_job(name: str) -> RegisteredJob:
    """Disable a registered job and clear next_run_at.

    Args:
        name: Job name

    Returns:
        The disabled RegisteredJob

    Raises:
        RegisteredJobNotFound: If no job with this name exists
    """
    async with get_sql_session() as session:
        result = await session.execute(select(RegisteredJob).where(RegisteredJob.name == name))
        job = result.scalar_one_or_none()
        if job is None:
            raise RegisteredJobNotFound(f"Registered job '{name}' not found")

        job.enabled = False
        job.next_run_at = None
        job.updated_at = datetime.utcnow()
        session.add(job)
        await session.commit()
        await session.refresh(job)
        return job


async def list_registered_jobs(
    *,
    enabled_only: bool = False,
) -> list[RegisteredJob]:
    """List registered jobs.

    Args:
        enabled_only: If True, only return enabled jobs

    Returns:
        List of RegisteredJob entries
    """
    async with get_sql_session() as session:
        query = select(RegisteredJob).order_by(RegisteredJob.name)
        if enabled_only:
            query = query.where(RegisteredJob.enabled == True)  # noqa: E712
        result = await session.execute(query)
        return list(result.scalars().all())


async def run_job(
    name: str,
    entrypoint: str,
    *,
    kwargs: dict[str, Any] | None = None,
    run_type: RunType = RunType.MANUAL,
    preservation_mode: PreservationMode | None = None,
    preserve: Preserve | object = _UNSET,
) -> Job:
    """Run a job immediately, auto-registering if needed.

    Upserts into registered_jobs (without schedule), merges kwargs
    over default_kwargs, then creates a Job + entry point Task.

    Args:
        name: Job name
        entrypoint: Python dotted path
        kwargs: Override parameters (merged over default_kwargs)
        run_type: How the job was triggered (default: MANUAL)
        preservation_mode: Legacy preservation mode (kept until Phase 6).
        preserve: Override the registered preserve default. Omit (or pass
            the sentinel) to inherit; pass ``[]`` to explicitly preserve
            nothing.

    Returns:
        Created Job
    """
    registered = await get_registered_job(name)
    if registered is None:
        registered = await register_job(name=name, entrypoint=entrypoint)

    merged_kwargs = {**(registered.default_kwargs or {}), **(kwargs or {})}

    task = create_task(entrypoint, merged_kwargs, name=name)
    return await create_job(
        name=name,
        entry=task,
        run_type=run_type,
        registered_job_id=registered.id,
        preservation_mode=preservation_mode,
        preserve=preserve,
        registered=registered,
    )
