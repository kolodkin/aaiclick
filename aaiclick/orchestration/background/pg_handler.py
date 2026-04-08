"""PostgreSQL-specific background cleanup SQL: ANY() array operators."""

from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .handler import BackgroundHandler


class PgBackgroundHandler(BackgroundHandler):
    """PostgreSQL: batch operations with ANY() array operator."""

    @staticmethod
    async def clear_job_pins(session: AsyncSession, job_ids: list[int]) -> None:
        await session.execute(
            text("UPDATE table_context_refs SET job_id = NULL WHERE job_id = ANY(:job_ids)"),
            {"job_ids": job_ids},
        )

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
        run_ids: list[int] = []
        for (run_ids_json,) in result.fetchall():
            ids = run_ids_json if isinstance(run_ids_json, list) else json.loads(run_ids_json or "[]")
            if ids:
                run_ids.append(ids[-1])
        return run_ids

    @staticmethod
    async def clean_task_run(session: AsyncSession, run_id: str) -> None:
        await session.execute(
            text(
                "UPDATE table_context_refs SET run_ids = ("
                "  SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb)"
                "  FROM jsonb_array_elements(run_ids::jsonb) AS elem"
                "  WHERE elem #>> '{}' != :run_id"
                ") WHERE run_ids::jsonb @> jsonb_build_array(:run_id)"
            ),
            {"run_id": run_id},
        )
