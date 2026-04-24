"""Internal API for job commands.

Every function runs inside an active ``orch_context()`` and reads SQL/CH
resources via the contextvar getters (``get_sql_session``, ``get_ch_client``).
Returns pydantic view models from ``aaiclick.orchestration.view_models``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col, func, select

from aaiclick.orchestration.execution.claiming import (
    JobAlreadyTerminal,
    JobNotFound as _JobNotFound,
    cancel_job as _cancel_job_impl,
)
from aaiclick.orchestration.models import Job, Task
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.registered_jobs import run_job as _run_job_impl
from aaiclick.orchestration.view_models import (
    JobDetail,
    JobStatsView,
    JobView,
    compute_job_stats_view,
    job_to_detail,
    job_to_view,
)
from aaiclick.view_models import JobListFilter, Page, RefId, RunJobRequest

from .errors import Conflict, NotFound


@asynccontextmanager
async def _sql_session(session: AsyncSession | None) -> AsyncIterator[AsyncSession]:
    """Yield ``session`` if non-None, otherwise open one via ``get_sql_session``."""
    if session is not None:
        yield session
        return
    async with get_sql_session() as owned:
        yield owned


async def _resolve_job(ref: RefId, session: AsyncSession | None = None) -> Job | None:
    """Look up a job by numeric ID or the most recent job with a matching name."""
    async with _sql_session(session) as s:
        if isinstance(ref, int):
            return (await s.execute(select(Job).where(Job.id == ref))).scalar_one_or_none()

        if ref.isdigit():
            found = (await s.execute(select(Job).where(Job.id == int(ref)))).scalar_one_or_none()
            if found is not None:
                return found

        return (
            await s.execute(select(Job).where(Job.name == ref).order_by(col(Job.created_at).desc()).limit(1))
        ).scalar_one_or_none()


async def list_jobs(filter: JobListFilter | None = None) -> Page[JobView]:
    """Return a page of jobs ordered by ``created_at`` descending.

    ``filter.name`` is matched with SQL ``LIKE`` (caller supplies wildcards).
    ``filter.since`` filters on ``created_at >= since``. Pagination uses
    ``filter.limit`` / ``filter.offset``; ``filter.cursor`` is reserved for a
    future cursor-based REST/MCP surface and is currently ignored.
    """
    filter = filter or JobListFilter()

    count_query = select(func.count()).select_from(Job)
    list_query = select(Job)
    if filter.status is not None:
        count_query = count_query.where(Job.status == filter.status)
        list_query = list_query.where(Job.status == filter.status)
    if filter.name is not None:
        count_query = count_query.where(col(Job.name).like(filter.name))
        list_query = list_query.where(col(Job.name).like(filter.name))
    if filter.since is not None:
        count_query = count_query.where(Job.created_at >= filter.since)
        list_query = list_query.where(Job.created_at >= filter.since)

    list_query = list_query.order_by(col(Job.created_at).desc()).limit(filter.limit).offset(filter.offset)
    async with get_sql_session() as session:
        total = (await session.execute(count_query)).scalar_one()
        rows = (await session.execute(list_query)).scalars().all()

    return Page[JobView](items=[job_to_view(j) for j in rows], total=total)


async def get_job(ref: RefId) -> JobDetail:
    """Return full job detail including all tasks, ordered by creation time."""
    async with get_sql_session() as session:
        job = await _resolve_job(ref, session)
        if job is None:
            raise NotFound(f"Job not found: {ref}")
        tasks = (
            (await session.execute(select(Task).where(Task.job_id == job.id).order_by(col(Task.created_at))))
            .scalars()
            .all()
        )
    return job_to_detail(job, list(tasks))


async def job_stats(ref: RefId) -> JobStatsView:
    """Return execution statistics for a job and its tasks."""
    async with get_sql_session() as session:
        job = await _resolve_job(ref, session)
        if job is None:
            raise NotFound(f"Job not found: {ref}")
        tasks = (
            (await session.execute(select(Task).where(Task.job_id == job.id).order_by(col(Task.created_at))))
            .scalars()
            .all()
        )
    return compute_job_stats_view(job, list(tasks))


async def cancel_job(ref: RefId) -> JobView:
    """Cancel a job and its non-terminal tasks.

    Raises ``NotFound`` if the job does not exist, or ``Conflict`` if the job
    is already in a terminal state. ``ref`` is resolved to a job id first
    (the orchestration impl takes ``int`` only); the atomic cancellation
    lives in ``aaiclick.orchestration.execution.claiming.cancel_job`` and is
    authoritative about terminal state via typed exceptions.
    """
    job = await _resolve_job(ref)
    if job is None:
        raise NotFound(f"Job not found: {ref}")

    try:
        cancelled = await _cancel_job_impl(job.id)
    except _JobNotFound as exc:
        raise NotFound(str(exc)) from exc
    except JobAlreadyTerminal as exc:
        raise Conflict(str(exc)) from exc
    return job_to_view(cancelled)


async def run_job(request: RunJobRequest) -> JobView:
    """Run a job immediately, auto-registering if needed.

    The entrypoint is derived from ``request.name``: dotted names become the
    entrypoint directly, bare names reuse the registered job's entrypoint (or
    fall back to the name itself if not yet registered).
    """
    if "." in request.name:
        entrypoint = request.name
        name = request.name.rsplit(".", 1)[-1]
    else:
        name = request.name
        entrypoint = request.name

    job = await _run_job_impl(
        name=name,
        entrypoint=entrypoint,
        kwargs=request.kwargs or None,
        preservation_mode=request.preservation_mode,
    )
    return job_to_view(job)
