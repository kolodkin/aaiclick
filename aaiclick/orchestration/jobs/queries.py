"""Job query functions for orchestration backend."""

from __future__ import annotations

from typing import Any

from sqlmodel import col, func, select

from aaiclick.data.object.refs import JOB_ID, OBJECT_TYPE

from ..execution.runner import _deserialize_value
from ..models import Job, JobStatus, Task
from ..orch_context import get_sql_session


async def get_job(job_id: int) -> Job | None:
    """Get a job by ID.

    Args:
        job_id: Job ID

    Returns:
        Job if found, None otherwise
    """
    async with get_sql_session() as session:
        result = await session.execute(select(Job).where(Job.id == job_id))
        return result.scalar_one_or_none()


async def list_jobs(
    *,
    status: JobStatus | None = None,
    name_like: str | None = None,
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
    async with get_sql_session() as session:
        query = select(Job)
        if status is not None:
            query = query.where(Job.status == status)
        if name_like is not None:
            query = query.where(col(Job.name).like(name_like))
        query = query.order_by(col(Job.created_at).desc()).limit(limit).offset(offset)

        result = await session.execute(query)
        return list(result.scalars().all())


async def count_jobs(
    *,
    status: JobStatus | None = None,
    name_like: str | None = None,
) -> int:
    """Count jobs matching filters.

    Args:
        status: Filter by job status
        name_like: Filter by name pattern (SQL LIKE)

    Returns:
        Number of matching jobs
    """
    async with get_sql_session() as session:
        query = select(func.count()).select_from(Job)
        if status is not None:
            query = query.where(Job.status == status)
        if name_like is not None:
            query = query.where(col(Job.name).like(name_like))

        result = await session.execute(query)
        return result.scalar_one()


async def get_task(task_id: int) -> Task | None:
    """Get a task by ID.

    Args:
        task_id: Task ID

    Returns:
        Task if found, None otherwise
    """
    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        return result.scalar_one_or_none()


async def get_task_result_table(task_id: int) -> str | None:
    """Return the output-table name written by a committed task.

    Reads ``Task.result["table"]`` — the canonical slot for an Object-
    returning task's output. Returns ``None`` when the task is missing,
    has no result, or stored a non-Object result.
    """
    async with get_sql_session() as session:
        result = (await session.execute(select(Task.result).where(Task.id == task_id))).scalar_one_or_none()
    if isinstance(result, dict):
        return result.get("table")
    return None


async def get_tasks_for_job(job_id: int) -> list[Task]:
    """Get all tasks for a job, ordered by creation time.

    Args:
        job_id: Job ID

    Returns:
        List of tasks belonging to the job
    """
    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job_id).order_by(col(Task.created_at)))
        return list(result.scalars().all())


async def get_latest_job_by_name(name: str) -> Job | None:
    """Get the most recent job with the given name.

    Args:
        name: Exact job name

    Returns:
        Most recent Job with that name, or None
    """
    async with get_sql_session() as session:
        result = await session.execute(
            select(Job).where(Job.name == name).order_by(col(Job.created_at).desc()).limit(1)
        )
        return result.scalar_one_or_none()


async def resolve_job(ref: str) -> Job | None:
    """Resolve a job reference to a Job instance.

    Tries numeric ID first, then falls back to name lookup (latest).

    Args:
        ref: Job ID (numeric string) or job name

    Returns:
        Job if found, None otherwise
    """
    if ref.isdigit():
        return await get_job(int(ref))
    return await get_latest_job_by_name(ref)


async def get_job_result(job: Job) -> Any:
    """Get the result produced by a completed job.

    The job's entry task must have stored a result via
    ``TaskResult(data=..., tasks=[...])``. Must be called
    within an active ``data_context()``.

    Args:
        job: Completed job whose result to retrieve

    Returns:
        Deserialized result (Object, View, or native value)

    Raises:
        ValueError: If the entry task is missing or has no result
    """
    async with get_sql_session() as session:
        result_row = await session.execute(
            select(Task.result, Task.job_id).where(Task.job_id == job.id, Task.name == job.name)
        )
        row = result_row.one_or_none()

    if row is None:
        raise ValueError(f"Entry task for job {job.id} (name='{job.name}') not found")

    result, task_job_id = row
    if result is None:
        raise ValueError(f"Job {job.id} (name='{job.name}') has no result")

    if isinstance(result, dict) and result.get(OBJECT_TYPE):
        result[JOB_ID] = task_job_id

    async with get_sql_session() as session:
        return await _deserialize_value(result, session)
