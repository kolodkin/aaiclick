"""PostgreSQL-specific background cleanup SQL: ANY() array operators."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .handler import BackgroundHandler, PendingCleanupTask


class PgBackgroundHandler(BackgroundHandler):
    """PostgreSQL: batch operations with ANY() array operator."""

    @staticmethod
    async def mark_dead_workers(
        session: AsyncSession, dead_worker_ids: list[int], now: datetime,
    ) -> None:
        await session.execute(
            text(
                "UPDATE workers SET status = 'STOPPED' "
                "WHERE id = ANY(:worker_ids)"
            ),
            {"worker_ids": dead_worker_ids},
        )
        await session.execute(
            text(
                "UPDATE tasks SET status = 'PENDING_CLEANUP', "
                "error = 'Worker died (heartbeat timeout)' "
                "WHERE worker_id = ANY(:worker_ids) "
                "AND status IN ('RUNNING', 'CLAIMED')"
            ),
            {"worker_ids": dead_worker_ids},
        )

    @staticmethod
    async def clean_task_runs(session: AsyncSession, run_ids: list[str]) -> None:
        await session.execute(
            text("DELETE FROM table_run_refs WHERE run_id = ANY(:run_ids)"),
            {"run_ids": run_ids},
        )

    @staticmethod
    async def get_pending_cleanup_tasks(
        session: AsyncSession,
    ) -> list[PendingCleanupTask]:
        result = await session.execute(
            text(
                "SELECT id, job_id, worker_id, error, run_ids, attempt, max_retries "
                "FROM tasks WHERE status = 'PENDING_CLEANUP'"
            ),
        )
        return [PendingCleanupTask._make((*row[:4], row[4] or [], *row[5:])) for row in result.fetchall()]
