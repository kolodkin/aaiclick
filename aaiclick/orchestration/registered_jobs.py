"""CRUD operations for registered jobs."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from croniter import croniter
from sqlmodel import select

from .factories import create_job, create_task
from .models import Job, RegisteredJob, RunType
from .orch_context import get_sql_session
from ..snowflake_id import get_snowflake_id


def compute_next_run(cron_expr: str, after: Optional[datetime] = None) -> datetime:
    """Compute the next fire time for a cron expression.

    Args:
        cron_expr: Cron expression (e.g. "0 8 * * *")
        after: Base time to compute from (default: utcnow)

    Returns:
        Next fire datetime
    """
    base = after or datetime.utcnow()
    return croniter(cron_expr, base).get_next(datetime)


def _next_run_at(schedule: Optional[str], enabled: bool, now: datetime) -> Optional[datetime]:
    """Compute next_run_at from schedule if enabled, else None."""
    return compute_next_run(schedule, now) if schedule and enabled else None


async def register_job(
    *,
    name: str,
    entrypoint: str,
    schedule: Optional[str] = None,
    default_kwargs: Optional[Dict[str, Any]] = None,
    enabled: bool = True,
) -> RegisteredJob:
    """Register a new job in the catalog.

    Args:
        name: Unique job name
        entrypoint: Python dotted path (e.g. "myapp.pipelines.etl_job")
        schedule: Cron expression for scheduled runs (optional)
        default_kwargs: Default parameters for scheduled runs (optional)
        enabled: Whether the job is enabled (default: True)

    Returns:
        Created RegisteredJob

    Raises:
        ValueError: If a job with this name already exists
    """
    now = datetime.utcnow()
    registered_job = RegisteredJob(
        id=get_snowflake_id(),
        name=name,
        entrypoint=entrypoint,
        enabled=enabled,
        schedule=schedule,
        default_kwargs=default_kwargs,
        next_run_at=_next_run_at(schedule, enabled, now),
        created_at=now,
        updated_at=now,
    )

    async with get_sql_session() as session:
        existing = await session.execute(
            select(RegisteredJob).where(RegisteredJob.name == name)
        )
        if existing.scalar_one_or_none() is not None:
            raise ValueError(f"Registered job '{name}' already exists")

        session.add(registered_job)
        await session.commit()
        await session.refresh(registered_job)

    return registered_job


async def get_registered_job(name: str) -> Optional[RegisteredJob]:
    """Look up a registered job by name.

    Args:
        name: Job name

    Returns:
        RegisteredJob if found, None otherwise
    """
    async with get_sql_session() as session:
        result = await session.execute(
            select(RegisteredJob).where(RegisteredJob.name == name)
        )
        return result.scalar_one_or_none()


async def upsert_registered_job(
    *,
    name: str,
    entrypoint: str,
    schedule: Optional[str] = None,
    default_kwargs: Optional[Dict[str, Any]] = None,
    enabled: bool = True,
) -> RegisteredJob:
    """Insert or update a registered job.

    If a job with the given name exists, updates entrypoint, schedule,
    default_kwargs, and enabled. Otherwise creates a new entry.

    Args:
        name: Unique job name
        entrypoint: Python dotted path
        schedule: Cron expression (optional)
        default_kwargs: Default parameters (optional)
        enabled: Whether the job is enabled

    Returns:
        The created or updated RegisteredJob
    """
    now = datetime.utcnow()

    async with get_sql_session() as session:
        result = await session.execute(
            select(RegisteredJob).where(RegisteredJob.name == name)
        )
        existing = result.scalar_one_or_none()

        if existing is not None:
            existing.entrypoint = entrypoint
            existing.schedule = schedule
            existing.default_kwargs = default_kwargs
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
            next_run_at=_next_run_at(schedule, enabled, now),
            created_at=now,
            updated_at=now,
        )
        session.add(registered_job)
        await session.commit()
        await session.refresh(registered_job)
        return registered_job


async def enable_job(name: str) -> int:
    """Enable a registered job and recompute next_run_at.

    Args:
        name: Job name

    Returns:
        ID of the enabled registered job

    Raises:
        ValueError: If no job with this name exists
    """
    now = datetime.utcnow()

    async with get_sql_session() as session:
        result = await session.execute(
            select(RegisteredJob).where(RegisteredJob.name == name)
        )
        job = result.scalar_one_or_none()
        if job is None:
            raise ValueError(f"Registered job '{name}' not found")

        job.enabled = True
        job.updated_at = now
        job.next_run_at = _next_run_at(job.schedule, True, now)
        session.add(job)
        await session.commit()
        return job.id


async def disable_job(name: str) -> int:
    """Disable a registered job and clear next_run_at.

    Args:
        name: Job name

    Returns:
        ID of the disabled registered job

    Raises:
        ValueError: If no job with this name exists
    """
    async with get_sql_session() as session:
        result = await session.execute(
            select(RegisteredJob).where(RegisteredJob.name == name)
        )
        job = result.scalar_one_or_none()
        if job is None:
            raise ValueError(f"Registered job '{name}' not found")

        job.enabled = False
        job.next_run_at = None
        job.updated_at = datetime.utcnow()
        session.add(job)
        await session.commit()
        return job.id


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
    kwargs: Optional[Dict[str, Any]] = None,
    run_type: RunType = RunType.MANUAL,
) -> Job:
    """Run a job immediately, auto-registering if needed.

    Upserts into registered_jobs (without schedule), merges kwargs
    over default_kwargs, then creates a Job + entry point Task.

    Args:
        name: Job name
        entrypoint: Python dotted path
        kwargs: Override parameters (merged over default_kwargs)
        run_type: How the job was triggered (default: MANUAL)

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
    )
