"""SQLite-specific background cleanup SQL: IN clause, no array operators."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import TASK_CLAIMED, TASK_PENDING_CANCELLED_CLEANUP, TASK_PENDING_FAILURE_CLEANUP, TASK_RUNNING
from ..sql_utils import in_clause
from .handler import DEAD_WORKER_ERROR, BackgroundHandler


class SqliteBackgroundHandler(BackgroundHandler):
    """SQLite: batch operations via IN clause."""

    @staticmethod
    async def mark_dead_execution_workers(session: AsyncSession, dead_execution_worker_ids: list[int]) -> None:
        placeholders, params = in_clause(dead_execution_worker_ids, "wid")
        await session.execute(
            text(f"UPDATE execution_workers SET status = 'STOPPED' WHERE id IN ({placeholders})"),
            params,
        )
        await session.execute(
            text(
                f"UPDATE tasks SET status = :failure_cleanup, error = :error "
                f"WHERE execution_worker_id IN ({placeholders}) "
                f"AND status IN (:running, :claimed)"
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
                f"UPDATE tasks SET execution_worker_id = NULL "
                f"WHERE execution_worker_id IN ({placeholders}) AND status = :cancelled_cleanup"
            ),
            {**params, "cancelled_cleanup": TASK_PENDING_CANCELLED_CLEANUP},
        )

    @staticmethod
    async def clean_task_runs(session: AsyncSession, run_ids: list[str]) -> None:
        placeholders, params = in_clause(run_ids, "rid")
        await session.execute(
            text(f"DELETE FROM table_run_refs WHERE run_id IN ({placeholders})"),
            params,
        )
