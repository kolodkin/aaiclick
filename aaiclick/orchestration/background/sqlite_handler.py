"""SQLite-specific background cleanup SQL: IN clause, no array operators."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ..sql_utils import batched, in_clause
from .handler import BackgroundHandler


class SqliteBackgroundHandler(BackgroundHandler):
    """SQLite: batch operations via IN clause."""

    @staticmethod
    async def clean_task_runs(session: AsyncSession, run_ids: list[str]) -> None:
        for batch in batched(run_ids):
            placeholders, params = in_clause(batch, "rid")
            await session.execute(
                text(f"DELETE FROM table_run_refs WHERE run_id IN ({placeholders})"),
                params,
            )
