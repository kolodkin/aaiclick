"""SQLite-specific background cleanup SQL: IN clause, no array operators."""

from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .handler import BackgroundHandler, extract_last_run_ids


def _in_clause(ids: list, prefix: str) -> tuple[str, dict]:
    """Build a parameterized IN clause for SQLite.

    Returns (placeholder_string, params_dict) e.g. (":p0, :p1", {"p0": 1, "p1": 2}).
    """
    params = {f"{prefix}{i}": v for i, v in enumerate(ids)}
    placeholders = ", ".join(f":{k}" for k in params)
    return placeholders, params


class SqliteBackgroundHandler(BackgroundHandler):
    """SQLite: batch operations via IN clause."""

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
                f"UPDATE tasks SET status = 'PENDING_CLEANUP', "
                f"error = 'Worker died (heartbeat timeout)' "
                f"WHERE worker_id IN ({placeholders}) "
                f"AND status IN ('RUNNING', 'CLAIMED')"
            ),
            params,
        )

    @staticmethod
    async def get_dead_worker_run_ids(
        session: AsyncSession, dead_worker_ids: list[int],
    ) -> list[int]:
        placeholders, params = _in_clause(dead_worker_ids, "wid")
        result = await session.execute(
            text(
                f"SELECT run_ids FROM tasks "
                f"WHERE worker_id IN ({placeholders}) "
                f"AND status IN ('RUNNING', 'CLAIMED')"
            ),
            params,
        )
        return extract_last_run_ids(result.fetchall())

    @staticmethod
    async def clean_task_runs(session: AsyncSession, run_ids: list[str]) -> None:
        placeholders, params = _in_clause(run_ids, "rid")
        await session.execute(
            text(f"DELETE FROM table_run_refs WHERE run_id IN ({placeholders})"),
            params,
        )

    @staticmethod
    async def get_pending_cleanup_tasks(
        session: AsyncSession,
    ) -> list[tuple[int, int, int, str, list, int]]:
        result = await session.execute(
            text(
                "SELECT id, job_id, worker_id, error, run_ids, attempt, max_retries "
                "FROM tasks WHERE status = 'PENDING_CLEANUP'"
            ),
        )
        rows = result.fetchall()
        parsed = []
        for row in rows:
            task_id, job_id, worker_id, error, run_ids_raw, attempt, max_retries = row
            run_ids = run_ids_raw if isinstance(run_ids_raw, list) else json.loads(run_ids_raw or "[]")
            parsed.append((task_id, job_id, worker_id, error, run_ids, attempt, max_retries))
        return parsed

    @staticmethod
    async def transition_pending_cleanup(
        session: AsyncSession,
        task_id: int,
        *,
        has_retries: bool,
        attempt: int,
        retry_after: datetime,
    ) -> None:
        if has_retries:
            await session.execute(
                text(
                    "UPDATE tasks SET status = 'PENDING', "
                    "attempt = :attempt, retry_after = :retry_after, "
                    "worker_id = NULL, claimed_at = NULL, "
                    "started_at = NULL, completed_at = NULL "
                    "WHERE id = :task_id"
                ),
                {"task_id": task_id, "attempt": attempt, "retry_after": retry_after},
            )
        else:
            await session.execute(
                text(
                    "UPDATE tasks SET status = 'FAILED', "
                    "completed_at = :now "
                    "WHERE id = :task_id"
                ),
                {"task_id": task_id, "now": datetime.utcnow()},
            )
