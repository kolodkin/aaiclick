"""Atomic task claiming and cancellation for distributed workers."""

from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlmodel import select

from aaiclick.backend import is_local

from .context import get_orch_session
from .models import Job, JobStatus, Task, TaskStatus

# Terminal job statuses that cannot be cancelled
_TERMINAL_JOB_STATUSES = (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)

# Shared dependency check SQL — identical for both backends
_DEPENDENCY_WHERE = """
    AND NOT EXISTS (
        SELECT 1 FROM dependencies d
        JOIN tasks prev ON d.previous_id = prev.id
        WHERE d.next_id = t.id
        AND d.next_type = 'task'
        AND d.previous_type = 'task'
        AND prev.status != :completed_status
    )
    AND NOT EXISTS (
        SELECT 1 FROM dependencies d
        JOIN tasks prev ON prev.group_id = d.previous_id
        WHERE d.next_id = t.id
        AND d.next_type = 'task'
        AND d.previous_type = 'group'
        AND prev.status != :completed_status
    )
    AND NOT EXISTS (
        SELECT 1 FROM dependencies d
        JOIN tasks prev ON d.previous_id = prev.id
        WHERE d.next_id = t.group_id
        AND d.next_type = 'group'
        AND d.previous_type = 'task'
        AND prev.status != :completed_status
        AND t.group_id IS NOT NULL
    )
    AND NOT EXISTS (
        SELECT 1 FROM dependencies d
        JOIN tasks prev ON prev.group_id = d.previous_id
        WHERE d.next_id = t.group_id
        AND d.next_type = 'group'
        AND d.previous_type = 'group'
        AND prev.status != :completed_status
        AND t.group_id IS NOT NULL
    )
"""


async def _claim_next_task_pg(worker_id: int) -> Optional[Task]:
    """PostgreSQL claim: writable CTE with FOR UPDATE SKIP LOCKED."""
    async with get_orch_session() as session:
        result = await session.execute(
            text(f"""
                WITH claimed_task AS (
                    UPDATE tasks
                    SET
                        status = :claimed_status,
                        worker_id = :worker_id,
                        claimed_at = :now
                    WHERE id = (
                        SELECT t.id FROM tasks t
                        JOIN jobs j ON t.job_id = j.id
                        WHERE t.status = :pending_status
                        AND (t.retry_after IS NULL OR t.retry_after <= :now)
                        AND j.status NOT IN (:cancelled_job_status, :failed_job_status)
                        {_DEPENDENCY_WHERE}
                        ORDER BY j.started_at ASC NULLS LAST, t.id ASC
                        LIMIT 1
                        FOR UPDATE OF t SKIP LOCKED
                    )
                    RETURNING id, job_id, entrypoint, name, kwargs, status, result,
                              log_path, error, worker_id, created_at, claimed_at,
                              started_at, completed_at, group_id,
                              max_retries, attempt, retry_after
                ),
                updated_job AS (
                    UPDATE jobs
                    SET
                        started_at = COALESCE(started_at, :now),
                        status = CASE
                            WHEN started_at IS NULL THEN :running_status
                            ELSE status
                        END
                    WHERE id = (SELECT job_id FROM claimed_task)
                    RETURNING id
                )
                SELECT * FROM claimed_task
            """),
            {
                "claimed_status": TaskStatus.RUNNING.value,
                "pending_status": TaskStatus.PENDING.value,
                "completed_status": TaskStatus.COMPLETED.value,
                "running_status": JobStatus.RUNNING.value,
                "cancelled_job_status": JobStatus.CANCELLED.value,
                "failed_job_status": JobStatus.FAILED.value,
                "worker_id": worker_id,
                "now": datetime.utcnow(),
            },
        )

        row = result.mappings().fetchone()
        if row is None:
            return None

        task_data = dict(row)
        task_data["status"] = TaskStatus(task_data["status"])
        task = Task(**task_data)

        await session.commit()
        return task


async def _claim_next_task_sqlite(worker_id: int) -> Optional[Task]:
    """SQLite claim: sequential SELECT + UPDATE in transaction.

    SQLite is single-writer so no row locking is needed.
    """
    async with get_orch_session() as session:
        now = datetime.utcnow()

        # Step 1: find the next eligible task
        find_result = await session.execute(
            text(f"""
                SELECT t.id FROM tasks t
                JOIN jobs j ON t.job_id = j.id
                WHERE t.status = :pending_status
                AND (t.retry_after IS NULL OR t.retry_after <= :now)
                AND j.status NOT IN (:cancelled_job_status, :failed_job_status)
                {_DEPENDENCY_WHERE}
                ORDER BY j.started_at ASC NULLS LAST, t.id ASC
                LIMIT 1
            """),
            {
                "pending_status": TaskStatus.PENDING.value,
                "completed_status": TaskStatus.COMPLETED.value,
                "cancelled_job_status": JobStatus.CANCELLED.value,
                "failed_job_status": JobStatus.FAILED.value,
                "now": now,
            },
        )
        row = find_result.fetchone()
        if row is None:
            return None

        task_id = row[0]

        # Step 2: claim the task
        await session.execute(
            text(
                "UPDATE tasks "
                "SET status = :claimed_status, worker_id = :worker_id, claimed_at = :now "
                "WHERE id = :task_id"
            ),
            {
                "claimed_status": TaskStatus.RUNNING.value,
                "worker_id": worker_id,
                "now": now,
                "task_id": task_id,
            },
        )

        # Step 3: update job status if first claim
        await session.execute(
            text(
                "UPDATE jobs "
                "SET started_at = COALESCE(started_at, :now), "
                "    status = CASE WHEN started_at IS NULL THEN :running_status ELSE status END "
                "WHERE id = (SELECT job_id FROM tasks WHERE id = :task_id)"
            ),
            {
                "now": now,
                "running_status": JobStatus.RUNNING.value,
                "task_id": task_id,
            },
        )

        # Step 4: fetch the claimed task
        task_result = await session.execute(
            select(Task).where(Task.id == task_id)
        )
        task = task_result.scalar_one()

        await session.commit()
        return task


async def claim_next_task(worker_id: int) -> Optional[Task]:
    """
    Atomically claim the next available task for a worker.

    Dispatches to PostgreSQL or SQLite implementation based on backend.

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
    if is_local():
        return await _claim_next_task_sqlite(worker_id)
    return await _claim_next_task_pg(worker_id)


async def update_task_status(
    task_id: int,
    status: TaskStatus,
    error: Optional[str] = None,
    result: Optional[dict] = None,
) -> bool:
    """
    Update a task's status and optional error/result.

    Args:
        task_id: Task ID to update
        status: New status
        error: Error message (for FAILED status)
        result: Result reference (for COMPLETED status)

    Returns:
        bool: True if task was found and updated
    """
    async with get_orch_session() as session:
        query = select(Task).where(Task.id == task_id)
        if not is_local():
            query = query.with_for_update()
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
    async with get_orch_session() as session:
        query = select(Job).where(Job.id == job_id)
        if not is_local():
            query = query.with_for_update()
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
    async with get_orch_session() as session:
        query = select(Job).where(Job.id == job_id)
        if not is_local():
            query = query.with_for_update()
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
                "AND status IN ('PENDING', 'CLAIMED', 'RUNNING')"
            ),
            {
                "cancelled_status": TaskStatus.CANCELLED.value,
                "now": now,
                "job_id": job_id,
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
    async with get_orch_session() as session:
        result = await session.execute(
            select(Task.status).where(Task.id == task_id)
        )
        status = result.scalar_one_or_none()
        return status == TaskStatus.CANCELLED
