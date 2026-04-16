"""PostgreSQL-specific SQL operations: writable CTEs, FOR UPDATE SKIP LOCKED."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from ..models import JobStatus, Task, TaskStatus
from .db_handler import DEPENDENCY_WHERE, DbHandler


class PgDbHandler(DbHandler):
    """PostgreSQL: writable CTEs, FOR UPDATE SKIP LOCKED."""

    @staticmethod
    async def claim_next_task(session: AsyncSession, worker_id: int, now: datetime) -> Task | None:
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
