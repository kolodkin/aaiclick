"""PostgreSQL-specific background cleanup SQL: ANY() array operators."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .handler import BackgroundHandler


class PgBackgroundHandler(BackgroundHandler):
    """PostgreSQL: batch operations with ANY() array operator."""

    @staticmethod
    async def clean_task_runs(session: AsyncSession, run_ids: list[str]) -> None:
        await session.execute(
            text("DELETE FROM table_run_refs WHERE run_id = ANY(:run_ids)"),
            {"run_ids": run_ids},
        )
