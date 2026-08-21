"""Internal API for job commands.

Every function runs inside an active ``orch_context()`` and reads SQL/CH
resources via the contextvar getters (``get_sql_session``, ``get_ch_client``).
Returns pydantic view models from ``aaiclick.orchestration.view_models``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import func as sa_func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col, select

from aaiclick.orchestration.execution import claiming
from aaiclick.orchestration.models import TASK_COMPLETED, Dependency, Group, Job, Task
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.registered_jobs import get_registered_job
from aaiclick.orchestration.registered_jobs import run_job as _run_job_impl
from aaiclick.orchestration.view_models import (
    JobDetail,
    JobGraphView,
    JobStatsView,
    JobView,
    build_job_graph_view,
    compute_job_stats_view,
    job_to_detail,
    job_to_view,
)
from aaiclick.tenancy import get_active_tenant_id
from aaiclick.view_models import JobListFilter, Page, RefId, RunJobRequest

from .errors import Conflict, NotFound
from .pagination import paginate


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
    tenant_id = get_active_tenant_id()
    async with _sql_session(session) as s:
        if isinstance(ref, int):
            return (
                await s.execute(select(Job).where(Job.id == ref, Job.tenant_id == tenant_id))
            ).scalar_one_or_none()

        if ref.isdigit():
            found = (
                await s.execute(select(Job).where(Job.id == int(ref), Job.tenant_id == tenant_id))
            ).scalar_one_or_none()
            if found is not None:
                return found

        return (
            await s.execute(
                select(Job)
                .where(Job.name == ref, Job.tenant_id == tenant_id)
                .order_by(col(Job.created_at).desc())
                .limit(1)
            )
        ).scalar_one_or_none()


async def list_jobs(filter: JobListFilter | None = None) -> Page[JobView]:
    """Return a page of jobs ordered by ``created_at`` descending.

    ``filter.name`` is matched with SQL ``LIKE`` (caller supplies wildcards).
    ``filter.since`` filters on ``created_at >= since``. Pagination uses
    ``filter.limit`` / ``filter.offset``; ``filter.cursor`` is reserved for a
    future cursor-based REST/MCP surface and is currently ignored.
    """
    filter = filter or JobListFilter()

    predicates = [Job.tenant_id == get_active_tenant_id()]
    if filter.status is not None:
        predicates.append(Job.status == filter.status)
    if filter.name is not None:
        predicates.append(col(Job.name).like(filter.name))
    if filter.since is not None:
        predicates.append(Job.created_at >= filter.since)

    page = await paginate(
        Job,
        where=predicates,
        order_by=col(Job.created_at).desc(),
        limit=filter.limit,
        offset=filter.offset,
    )

    job_ids = [j.id for j in page.rows]
    totals: dict[int, int] = {}
    completed: dict[int, int] = {}
    if job_ids:
        async with get_sql_session() as session:
            rows = (
                await session.execute(
                    select(
                        Task.job_id,
                        sa_func.count().label("total"),
                        sa_func.count().filter(Task.status == TASK_COMPLETED).label("done"),
                    )
                    .where(col(Task.job_id).in_(job_ids))
                    .group_by(Task.job_id)
                )
            ).all()
        for job_id, total, done in rows:
            totals[job_id] = int(total)
            completed[job_id] = int(done or 0)

    return Page[JobView](
        items=[
            job_to_view(j, total_tasks=totals.get(j.id, 0), completed_tasks=completed.get(j.id, 0)) for j in page.rows
        ],
        total=page.total,
    )


async def _load_job_and_tasks(ref: RefId, session: AsyncSession | None = None) -> tuple[Job, list[Task]]:
    """Resolve a job ref and load its tasks ordered by creation time.

    Takes an optional session, like ``_resolve_job``, so a caller that needs
    further queries on the same job can reuse it rather than re-implementing
    the resolve-and-load contract.
    """
    async with _sql_session(session) as s:
        job = await _resolve_job(ref, s)
        if job is None:
            raise NotFound(f"Job not found: {ref}")
        tasks = (
            (await s.execute(select(Task).where(Task.job_id == job.id).order_by(col(Task.created_at)))).scalars().all()
        )
    return job, list(tasks)


async def get_job(ref: RefId) -> JobDetail:
    """Return full job detail including all tasks, ordered by creation time."""
    job, tasks = await _load_job_and_tasks(ref)
    return job_to_detail(job, tasks)


async def get_job_graph(ref: RefId) -> JobGraphView:
    """Return the job's dependency graph as task nodes and task-to-task edges.

    Group dependencies are expanded onto member tasks server-side — the client
    receives no ``Group`` or ``Dependency`` rows.
    """
    async with get_sql_session() as session:
        job, tasks = await _load_job_and_tasks(ref, session)
        groups = list((await session.execute(select(Group).where(Group.job_id == job.id))).scalars().all())
        if not tasks and not groups:
            return JobGraphView(job_id=job.id)

        # Dependency rows are scoped by their endpoint ids, not by job — the
        # table has no ``job_id``. Match with subqueries rather than inlining
        # the ids: the SQL text stays constant-size so the statement cache
        # hits, and a large job cannot blow SQLite's bind-parameter ceiling.
        endpoint_ids = (
            select(Task.id).where(Task.job_id == job.id).union_all(select(Group.id).where(Group.job_id == job.id))
        )
        dependencies = list(
            (
                await session.execute(
                    select(Dependency).where(
                        col(Dependency.next_id).in_(endpoint_ids) | col(Dependency.previous_id).in_(endpoint_ids)
                    )
                )
            )
            .scalars()
            .all()
        )
    return build_job_graph_view(job, tasks, groups, dependencies)


async def job_stats(ref: RefId) -> JobStatsView:
    """Return execution statistics for a job and its tasks."""
    job, tasks = await _load_job_and_tasks(ref)
    return compute_job_stats_view(job, tasks)


async def cancel_job(ref: RefId) -> JobView:
    """Cancel a job and its non-terminal tasks.

    Raises ``NotFound`` if the job does not exist, or ``Conflict`` if the job
    is already in a terminal state. String refs are resolved to a job id first
    (the orchestration impl takes ``int`` only); int refs go straight to the
    impl, which is authoritative about both not-found and terminal-state via
    typed exceptions.
    """
    if isinstance(ref, int):
        job_id = ref
    else:
        job = await _resolve_job(ref)
        if job is None:
            raise NotFound(f"Job not found: {ref}")
        job_id = job.id

    try:
        cancelled = await claiming.cancel_job(job_id)
    except claiming.JobNotFound as exc:
        raise NotFound(str(exc)) from exc
    except claiming.JobAlreadyTerminal as exc:
        raise Conflict(str(exc)) from exc
    return job_to_view(cancelled)


async def run_job(request: RunJobRequest) -> JobView:
    """Run a job immediately.

    The entrypoint is derived from ``request.name``: dotted names become the
    entrypoint directly, bare names reuse the registered job's entrypoint (or
    fall back to the name itself if not yet registered). When a matching
    ``RegisteredJob`` exists, the new job links to it and inherits its
    ``default_kwargs``; otherwise it runs standalone.
    """
    if "." in request.name:
        entrypoint = request.name
        name = request.name.rsplit(".", 1)[-1]
    else:
        name = request.name
        registered = await get_registered_job(name)
        entrypoint = registered.entrypoint if registered is not None else request.name

    job = await _run_job_impl(
        name=name,
        entrypoint=entrypoint,
        kwargs=request.kwargs or None,
        preservation_mode=request.preservation_mode,
        entry_type=request.entry_type,
        command=request.command,
        command_env=request.command_env,
        image=request.image,
        git_remote=request.git_remote,
        git_sha=request.git_sha,
        git_branch=request.git_branch,
        dockerfile=request.dockerfile,
        namespace=request.namespace,
        service_account=request.service_account,
        image_pull_secret=request.image_pull_secret,
    )
    return job_to_view(job)
