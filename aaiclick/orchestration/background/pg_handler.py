"""PostgreSQL-specific background cleanup SQL: ANY() array operators."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import TASK_CLAIMED, TASK_PENDING_CANCELLED_CLEANUP, TASK_PENDING_FAILURE_CLEANUP, TASK_RUNNING
from .handler import DEAD_WORKER_ERROR, BackgroundHandler


class PgBackgroundHandler(BackgroundHandler):
    """PostgreSQL: batch operations with ANY() array operator."""

    @staticmethod
    async def mark_dead_execution_workers(session: AsyncSession, dead_execution_worker_ids: list[int]) -> None:
        params = {"execution_worker_ids": dead_execution_worker_ids}
        await session.execute(
            text("UPDATE execution_workers SET status = 'STOPPED' WHERE id = ANY(:execution_worker_ids)"),
            params,
        )
        await session.execute(
            text(
                "UPDATE tasks SET status = :failure_cleanup, error = :error "
                "WHERE execution_worker_id = ANY(:execution_worker_ids) "
                "AND status IN (:running, :claimed)"
            ),
            {
                **params,
                "failure_cleanup": TASK_PENDING_FAILURE_CLEANUP,
                "error": DEAD_WORKER_ERROR,
                "running": TASK_RUNNING,
                "claimed": TASK_CLAIMED,
            },
        )
        await session.execute(
            text(
                "UPDATE tasks SET execution_worker_id = NULL "
                "WHERE execution_worker_id = ANY(:execution_worker_ids) AND status = :cancelled_cleanup"
            ),
            {**params, "cancelled_cleanup": TASK_PENDING_CANCELLED_CLEANUP},
        )

    @staticmethod
    async def clean_task_runs(session: AsyncSession, run_ids: list[str]) -> None:
        await session.execute(
            text("DELETE FROM table_run_refs WHERE run_id = ANY(:run_ids)"),
            {"run_ids": run_ids},
        )
