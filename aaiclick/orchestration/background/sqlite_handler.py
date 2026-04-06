"""SQLite-specific background cleanup SQL: IN clause, no array operators."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .handler import BackgroundHandler


def _in_clause(ids: list[int], prefix: str) -> tuple[str, dict]:
    """Build a parameterized IN clause for SQLite.

    Returns (placeholder_string, params_dict) e.g. (":p0, :p1", {"p0": 1, "p1": 2}).
    """
    params = {f"{prefix}{i}": v for i, v in enumerate(ids)}
    placeholders = ", ".join(f":{k}" for k in params)
    return placeholders, params


class SqliteBackgroundHandler(BackgroundHandler):
    """SQLite: batch operations via IN clause."""

    @staticmethod
    async def delete_job_refs(session: AsyncSession, job_ids: list[int]) -> None:
        placeholders, params = _in_clause(job_ids, "jid")
        await session.execute(
            text(f"DELETE FROM table_context_refs WHERE context_id IN ({placeholders})"),
            params,
        )

    @staticmethod
    async def mark_dead_workers(
        session: AsyncSession, dead_worker_ids: list[int], now: datetime,
    ) -> None:
        placeholders, params = _in_clause(dead_worker_ids, "wid")
        params["now"] = now
        await session.execute(
            text(f"UPDATE workers SET status = 'STOPPED' WHERE id IN ({placeholders})"),
            params,
        )
        await session.execute(
            text(
                f"UPDATE tasks SET status = 'FAILED', "
                f"completed_at = :now, "
                f"error = 'Worker died (heartbeat timeout)' "
                f"WHERE worker_id IN ({placeholders}) "
                f"AND status IN ('RUNNING', 'CLAIMED')"
            ),
            params,
        )
