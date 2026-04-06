"""SQLite-specific background cleanup SQL: per-row loops, no array operators."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .handler import BackgroundHandler


class SqliteBackgroundHandler(BackgroundHandler):
    """SQLite: per-row DELETE/UPDATE, no ANY() support."""

    @staticmethod
    async def delete_job_refs(session: AsyncSession, job_ids: list[int]) -> None:
        for jid in job_ids:
            await session.execute(
                text("DELETE FROM table_context_refs WHERE context_id = :jid"),
                {"jid": jid},
            )

    @staticmethod
    async def mark_dead_workers(
        session: AsyncSession, dead_worker_ids: list[int], now: datetime,
    ) -> None:
        for wid in dead_worker_ids:
            await session.execute(
                text("UPDATE workers SET status = 'STOPPED' WHERE id = :wid"),
                {"wid": wid},
            )
            await session.execute(
                text(
                    "UPDATE tasks SET status = 'FAILED', "
                    "completed_at = :now, "
                    "error = 'Worker died (heartbeat timeout)' "
                    "WHERE worker_id = :wid "
                    "AND status IN ('RUNNING', 'CLAIMED')"
                ),
                {"wid": wid, "now": now},
            )
