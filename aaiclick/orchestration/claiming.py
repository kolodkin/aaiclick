"""Atomic task claiming for distributed workers."""

from datetime import datetime
from typing import Optional

from sqlalchemy import text

from .context import get_orch_context_session
from .models import Job, JobStatus, Task, TaskStatus


async def claim_next_task(worker_id: int) -> Optional[Task]:
    """
    Atomically claim the next available task for a worker.

    Uses PostgreSQL's FOR UPDATE SKIP LOCKED to safely claim tasks
    in a concurrent environment. Prioritizes tasks from oldest running jobs.

    When the first task of a job is claimed:
    - Job status transitions from PENDING to RUNNING
    - Job's started_at is set to current time

    Dependency checking:
    - Task → Task: Task waits for previous task to complete
    - Group → Task: Task waits for all tasks in previous group to complete
    - Task → Group: Tasks in group wait for previous task to complete
    - Group → Group: Tasks in group wait for all tasks in previous group to complete

    Args:
        worker_id: ID of the worker claiming the task

    Returns:
        Task if one was claimed, None if no tasks available
    """
    async with get_orch_context_session() as session:
        # Use raw SQL for FOR UPDATE SKIP LOCKED
        # SQLAlchemy's with_for_update() doesn't support SKIP LOCKED well with subqueries
        result = await session.execute(
            text("""
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
                        -- Check task → task dependencies (previous task must be completed)
                        AND NOT EXISTS (
                            SELECT 1 FROM dependencies d
                            JOIN tasks prev ON d.previous_id = prev.id
                            WHERE d.next_id = t.id
                            AND d.next_type = 'task'
                            AND d.previous_type = 'task'
                            AND prev.status != :completed_status
                        )
                        -- Check group → task dependencies (all tasks in previous group must be completed)
                        AND NOT EXISTS (
                            SELECT 1 FROM dependencies d
                            JOIN tasks prev ON prev.group_id = d.previous_id
                            WHERE d.next_id = t.id
                            AND d.next_type = 'task'
                            AND d.previous_type = 'group'
                            AND prev.status != :completed_status
                        )
                        -- Check task → group dependencies (if task is in a group that depends on a task)
                        AND NOT EXISTS (
                            SELECT 1 FROM dependencies d
                            JOIN tasks prev ON d.previous_id = prev.id
                            WHERE d.next_id = t.group_id
                            AND d.next_type = 'group'
                            AND d.previous_type = 'task'
                            AND prev.status != :completed_status
                            AND t.group_id IS NOT NULL
                        )
                        -- Check group → group dependencies (if task is in a group that depends on another group)
                        AND NOT EXISTS (
                            SELECT 1 FROM dependencies d
                            JOIN tasks prev ON prev.group_id = d.previous_id
                            WHERE d.next_id = t.group_id
                            AND d.next_type = 'group'
                            AND d.previous_type = 'group'
                            AND prev.status != :completed_status
                            AND t.group_id IS NOT NULL
                        )
                        ORDER BY j.started_at ASC NULLS LAST, t.id ASC
                        LIMIT 1
                        FOR UPDATE OF t SKIP LOCKED
                    )
                    RETURNING id, job_id, entrypoint, kwargs, status, result,
                              log_path, error, worker_id, created_at, claimed_at,
                              started_at, completed_at, group_id
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
                "worker_id": worker_id,
                "now": datetime.utcnow(),
            },
        )

        row = result.fetchone()
        if row is None:
            return None

        # Convert row to Task object
        task = Task(
            id=row.id,
            job_id=row.job_id,
            entrypoint=row.entrypoint,
            kwargs=row.kwargs,
            status=TaskStatus(row.status),
            result=row.result,
            log_path=row.log_path,
            error=row.error,
            worker_id=row.worker_id,
            created_at=row.created_at,
            claimed_at=row.claimed_at,
            started_at=row.started_at,
            completed_at=row.completed_at,
            group_id=row.group_id,
        )

        await session.commit()
        return task


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
    async with get_orch_context_session() as session:
        result_query = await session.execute(
            text("SELECT id FROM tasks WHERE id = :task_id FOR UPDATE"),
            {"task_id": task_id},
        )
        if result_query.fetchone() is None:
            return False

        updates = {"status": status.value}
        if status == TaskStatus.RUNNING:
            updates["started_at"] = datetime.utcnow()
        elif status in (TaskStatus.COMPLETED, TaskStatus.FAILED):
            updates["completed_at"] = datetime.utcnow()
            if error:
                updates["error"] = error
            if result:
                updates["result"] = result

        set_clause = ", ".join(f"{k} = :{k}" for k in updates)
        await session.execute(
            text(f"UPDATE tasks SET {set_clause} WHERE id = :task_id"),
            {"task_id": task_id, **updates},
        )
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
    async with get_orch_context_session() as session:
        result = await session.execute(
            text("SELECT id FROM jobs WHERE id = :job_id FOR UPDATE"),
            {"job_id": job_id},
        )
        if result.fetchone() is None:
            return False

        updates = {"status": status.value}
        if status in (JobStatus.COMPLETED, JobStatus.FAILED):
            updates["completed_at"] = datetime.utcnow()
            if error:
                updates["error"] = error

        set_clause = ", ".join(f"{k} = :{k}" for k in updates)
        await session.execute(
            text(f"UPDATE jobs SET {set_clause} WHERE id = :job_id"),
            {"job_id": job_id, **updates},
        )
        await session.commit()
        return True
