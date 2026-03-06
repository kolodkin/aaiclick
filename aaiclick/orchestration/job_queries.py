"""Job query functions for orchestration backend."""

from __future__ import annotations

from typing import Optional

from sqlmodel import func, select

from .context import get_orch_session
from .models import Job, JobStatus


async def get_job(job_id: int) -> Optional[Job]:
    """Get a job by ID.

    Args:
        job_id: Job ID

    Returns:
        Job if found, None otherwise
    """
    async with get_orch_session() as session:
        result = await session.execute(select(Job).where(Job.id == job_id))
        return result.scalar_one_or_none()


async def list_jobs(
    *,
    status: Optional[JobStatus] = None,
    name_like: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> list[Job]:
    """List jobs with optional filtering and pagination.

    Args:
        status: Filter by job status
        name_like: Filter by name pattern (SQL LIKE, e.g. "%etl%")
        limit: Maximum number of results (default: 50)
        offset: Number of results to skip (default: 0)

    Returns:
        List of jobs matching criteria, ordered by created_at descending
    """
    async with get_orch_session() as session:
        query = select(Job)
        if status is not None:
            query = query.where(Job.status == status)
        if name_like is not None:
            query = query.where(Job.name.like(name_like))
        query = query.order_by(Job.created_at.desc()).limit(limit).offset(offset)

        result = await session.execute(query)
        return list(result.scalars().all())


async def count_jobs(
    *,
    status: Optional[JobStatus] = None,
    name_like: Optional[str] = None,
) -> int:
    """Count jobs matching filters.

    Args:
        status: Filter by job status
        name_like: Filter by name pattern (SQL LIKE)

    Returns:
        Number of matching jobs
    """
    async with get_orch_session() as session:
        query = select(func.count()).select_from(Job)
        if status is not None:
            query = query.where(Job.status == status)
        if name_like is not None:
            query = query.where(Job.name.like(name_like))

        result = await session.execute(query)
        return result.scalar_one()
