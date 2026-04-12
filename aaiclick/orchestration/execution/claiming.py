"""Atomic task claiming and cancellation for distributed workers."""

from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlmodel import select

from ..orch_context import get_db_handler, get_sql_session
from ..models import Job, JobStatus, Task, TaskStatus

# Terminal job statuses that cannot be cancelled
_TERMINAL_JOB_STATUSES = (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)


async def claim_next_task(worker_id: int) -> Optional[Task]:
    """
    Atomically claim the next available task for a worker.

    Delegates to the backend-specific handler from the orchestration context.

    When the first task of a job is claimed:
    - Job status transitions from PENDING to RUNNING
    - Job's started_at is set to current time

    Dependency checking:
    - Task -> Task: Task waits for previous task to complete
    - Group -> Task: Task waits for all tasks in previous group to complete
    - Task -> Group: Tasks in group wait for previous task to complete
    - Group -> Group: Tasks in group wait for all tasks in previous group to complete

    Args:
        worker_id: ID of the worker claiming the task

    Returns:
        Task if one was claimed, None if no tasks available
    """
    handler = get_db_handler()
    async with get_sql_session() as session:
        task = await handler.claim_next_task(session, worker_id, datetime.utcnow())
        await session.commit()
        return task


async def update_task_status(
    task_id: int,
    status: TaskStatus,
    error: Optional[str] = None,
    result: Optional[dict] = None,
    log_path: Optional[str] = None,
) -> bool:
    """
    Update a task's status and optional error/result.

    Also updates the last entry in run_statuses to match the new status.

    Args:
        task_id: Task ID to update
        status: New status
        error: Error message (for FAILED status)
        result: Result reference (for COMPLETED status)
        log_path: Path to the log file for this run

    Returns:
        bool: True if task was found and updated
    """
    handler = get_db_handler()
    async with get_sql_session() as session:
        query = handler.lock_query(select(Task).where(Task.id == task_id))
        query_result = await session.execute(query)
        task = query_result.scalar_one_or_none()
        if task is None:
            return False

        if task.status == TaskStatus.CANCELLED:
            return False

        task.status = status
        if status == TaskStatus.RUNNING:
            task.started_at = datetime.utcnow()
        elif status in (TaskStatus.COMPLETED, TaskStatus.FAILED):
            task.completed_at = datetime.utcnow()
            if error:
                task.error = error
            if result:
                task.result = result

        if log_path is not None:
            task.log_path = log_path

        if task.run_statuses:
            task.run_statuses = [*task.run_statuses[:-1], status.value]

        session.add(task)
        await session.commit()
        return True


async def update_job_status(job_id: int, status: JobStatus, error: Optional[str] = None) -> bool:
    """
    Update a job's status.

    Args:
        job_id: Job ID to update
        status: New status
        error: Error message (for FAILED status)

    Returns:
        bool: True if job was found and updated
    """
    handler = get_db_handler()
    async with get_sql_session() as session:
        query = handler.lock_query(select(Job).where(Job.id == job_id))
        query_result = await session.execute(query)
        job = query_result.scalar_one_or_none()
        if job is None:
            return False

        job.status = status
        if status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            job.completed_at = datetime.utcnow()
            if error:
                job.error = error

        session.add(job)
        await session.commit()
        return True


async def cancel_job(job_id: int) -> bool:
    """
    Cancel a job and all its non-terminal tasks.

    Atomically transitions the job to CANCELLED and bulk-updates all
    PENDING, CLAIMED, and RUNNING tasks to CANCELLED. Tasks already
    COMPLETED or FAILED are left unchanged.

    Only PENDING and RUNNING jobs can be cancelled. Returns False for
    jobs in terminal states (COMPLETED, FAILED, CANCELLED) or if the
    job does not exist.

    Args:
        job_id: Job ID to cancel

    Returns:
        bool: True if job was cancelled, False if not found or already terminal
    """
    handler = get_db_handler()
    async with get_sql_session() as session:
        query = handler.lock_query(select(Job).where(Job.id == job_id))
        query_result = await session.execute(query)
        job = query_result.scalar_one_or_none()
        if job is None:
            return False

        if job.status in _TERMINAL_JOB_STATUSES:
            return False

        now = datetime.utcnow()
        job.status = JobStatus.CANCELLED
        job.completed_at = now
        session.add(job)

        await session.execute(
            text(
                "UPDATE tasks SET status = :cancelled_status, "
                "completed_at = :now "
                "WHERE job_id = :job_id "
                "AND status IN (:pending, :claimed, :running, :pending_cleanup)"
            ),
            {
                "cancelled_status": TaskStatus.CANCELLED.value,
                "now": now,
                "job_id": job_id,
                "pending": TaskStatus.PENDING.value,
                "claimed": TaskStatus.CLAIMED.value,
                "running": TaskStatus.RUNNING.value,
                "pending_cleanup": TaskStatus.PENDING_CLEANUP.value,
            },
        )

        await session.commit()
        return True


async def check_task_cancelled(task_id: int) -> bool:
    """
    Check if a task has been cancelled.

    Used by the worker's cancellation monitor to detect when a running
    task's job has been cancelled via cancel_job().

    Args:
        task_id: Task ID to check

    Returns:
        bool: True if task status is CANCELLED
    """
    async with get_sql_session() as session:
        result = await session.execute(
            select(Task.status).where(Task.id == task_id)
        )
        status = result.scalar_one_or_none()
        return status == TaskStatus.CANCELLED
