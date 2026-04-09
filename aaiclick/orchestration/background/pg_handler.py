"""PostgreSQL-specific background cleanup SQL: ANY() array operators."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .handler import BackgroundHandler, extract_last_run_ids


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
                "UPDATE tasks SET status = 'FAILED', "
                "completed_at = :now, "
                "error = 'Worker died (heartbeat timeout)' "
                "WHERE worker_id = ANY(:worker_ids) "
                "AND status IN ('RUNNING', 'CLAIMED')"
            ),
            {"worker_ids": dead_worker_ids, "now": now},
        )

    @staticmethod
    async def get_dead_worker_run_ids(
        session: AsyncSession, dead_worker_ids: list[int],
    ) -> list[int]:
        result = await session.execute(
            text(
                "SELECT run_ids FROM tasks "
                "WHERE worker_id = ANY(:worker_ids) "
                "AND status IN ('RUNNING', 'CLAIMED')"
            ),
            {"worker_ids": dead_worker_ids},
        )
        return extract_last_run_ids(result.fetchall())

    @staticmethod
    async def clean_task_runs(session: AsyncSession, run_ids: list[str]) -> None:
        await session.execute(
            text("DELETE FROM table_run_refs WHERE run_id = ANY(:run_ids)"),
            {"run_ids": run_ids},
        )
