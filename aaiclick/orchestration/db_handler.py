"""Backend-specific SQL operations for PostgreSQL and SQLite."""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select
from sqlmodel import select

from .models import Job, JobStatus, Task, TaskStatus

# Shared dependency check SQL — identical for both backends
DEPENDENCY_WHERE = """
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


class DbHandler(Protocol):
    """Protocol for backend-specific SQL operations."""

    @staticmethod
    async def claim_next_task(
        session: AsyncSession, worker_id: int, now: datetime
    ) -> Optional[Task]: ...

    @staticmethod
    def lock_query(query: Select) -> Select: ...


class PgDbHandler:
    """PostgreSQL: writable CTEs, FOR UPDATE SKIP LOCKED."""

    @staticmethod
    async def claim_next_task(
        session: AsyncSession, worker_id: int, now: datetime
    ) -> Optional[Task]:
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
                        {DEPENDENCY_WHERE}
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
                "now": now,
            },
        )

        row = result.mappings().fetchone()
        if row is None:
            return None

        task_data = dict(row)
        task_data["status"] = TaskStatus(task_data["status"])
        return Task(**task_data)

    @staticmethod
    def lock_query(query: Select) -> Select:
        return query.with_for_update()


class SqliteDbHandler:
    """SQLite: sequential SELECT + UPDATE, no row locking."""

    @staticmethod
    async def claim_next_task(
        session: AsyncSession, worker_id: int, now: datetime
    ) -> Optional[Task]:
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
        return task_result.scalar_one()

    @staticmethod
    def lock_query(query: Select) -> Select:
        return query
