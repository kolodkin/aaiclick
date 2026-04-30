"""SQLite-specific SQL operations: sequential SELECT + UPDATE, no row locking."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select
from sqlmodel import select

from ..models import JOB_CANCELLED, JOB_FAILED, JOB_RUNNING, JobStatus, TASK_COMPLETED, TASK_PENDING, TASK_RUNNING, Task, TaskStatus
from .db_handler import DEPENDENCY_WHERE, DbHandler


class SqliteDbHandler(DbHandler):
    """SQLite: sequential SELECT + UPDATE, no row locking."""

    @staticmethod
    async def claim_next_task(session: AsyncSession, worker_id: int, now: datetime) -> Task | None:
        # Step 1: find the next eligible task
        find_result = await session.execute(
            text(f"""
                SELECT t.id FROM tasks t
                JOIN jobs j ON t.job_id = j.id
                WHERE t.status = :pending_status
                AND (t.retry_after IS NULL OR t.retry_after <= :now)
                AND j.status NOT IN (:cancelled_job_status, :failed_job_status)
                {DEPENDENCY_WHERE}
                ORDER BY j.started_at ASC NULLS LAST, t.id ASC
                LIMIT 1
            """),
            {
                "pending_status": TASK_PENDING,
                "completed_status": TASK_COMPLETED,
                "cancelled_job_status": JOB_CANCELLED,
                "failed_job_status": JOB_FAILED,
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
                "claimed_status": TASK_RUNNING,
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
                "running_status": JOB_RUNNING,
                "task_id": task_id,
            },
        )

        # Step 4: fetch the claimed task
        task_result = await session.execute(select(Task).where(Task.id == task_id))
        return task_result.scalar_one()

    @staticmethod
    def lock_query(query: Select) -> Select:
        return query
