"""SQLite-specific background cleanup SQL: IN clause, no array operators."""

from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .handler import BackgroundHandler, PendingCleanupTask, in_clause


class SqliteBackgroundHandler(BackgroundHandler):
    """SQLite: batch operations via IN clause."""

    @staticmethod
    async def mark_dead_workers(
        session: AsyncSession,
        dead_worker_ids: list[int],
        now: datetime,
    ) -> None:
        placeholders, params = in_clause(dead_worker_ids, "wid")
        params["now"] = now
        await session.execute(
            text(f"UPDATE workers SET status = 'STOPPED' WHERE id IN ({placeholders})"),
            params,
        )
        await session.execute(
            text(
                f"UPDATE tasks SET status = 'PENDING_CLEANUP', "
                f"error = 'Worker died (heartbeat timeout)' "
                f"WHERE worker_id IN ({placeholders}) "
                f"AND status IN ('RUNNING', 'CLAIMED')"
            ),
            params,
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
        return [
            PendingCleanupTask._make(
                (
                    *row[:4],
                    row[4] if isinstance(row[4], list) else json.loads(row[4] or "[]"),
                    *row[5:],
                )
            )
            for row in result.fetchall()
        ]
