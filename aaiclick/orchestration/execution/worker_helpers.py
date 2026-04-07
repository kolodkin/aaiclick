"""Shared helpers used by both the worker main loop and subprocess runner.

These functions are pure SQL operations (no ClickHouse) that update
task/job/worker state in the database.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlmodel import select

from ..models import Job, JobStatus, Task, TaskStatus, Worker
from ..orch_context import get_sql_session
from .claiming import update_job_status


async def try_complete_job(job_id: int) -> None:
    """Check if all tasks for a job are done and update job status accordingly."""
    async with get_sql_session() as session:
        result = await session.execute(
            select(Task.status).where(Task.job_id == job_id)
        )
        statuses = [row[0] for row in result.all()]

        if not statuses:
            return

        if any(s in (TaskStatus.PENDING, TaskStatus.CLAIMED, TaskStatus.RUNNING) for s in statuses):
            return

        if any(s == TaskStatus.FAILED for s in statuses):
            await update_job_status(job_id, JobStatus.FAILED, error="One or more tasks failed")
        else:
            await update_job_status(job_id, JobStatus.COMPLETED)


async def schedule_retry(task_id: int, current_attempt: int, error: str) -> None:
    """Reset a failed task to PENDING with incremented attempt and backoff delay."""
    base_delay = 1  # seconds
    delay = base_delay * (2 ** current_attempt)
    retry_after = datetime.utcnow() + timedelta(seconds=delay)

    async with get_sql_session() as session:
        result = await session.execute(
            select(Task).where(Task.id == task_id).with_for_update()
        )
        task = result.scalar_one()
        task.status = TaskStatus.PENDING
        task.attempt = current_attempt + 1
        task.retry_after = retry_after
        task.error = error
        task.worker_id = None
        task.claimed_at = None
        task.started_at = None
        task.completed_at = None
        if task.run_statuses:
            task.run_statuses = [*task.run_statuses[:-1], TaskStatus.FAILED.value]
        session.add(task)
        await session.commit()


async def increment_worker_stat(worker_id: int, field: str) -> None:
    """Increment a worker stat field (tasks_completed or tasks_failed)."""
    async with get_sql_session() as session:
        result = await session.execute(select(Worker).where(Worker.id == worker_id))
        worker = result.scalar_one_or_none()
        if worker:
            setattr(worker, field, getattr(worker, field) + 1)
            session.add(worker)
            await session.commit()
