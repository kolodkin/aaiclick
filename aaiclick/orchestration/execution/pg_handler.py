"""PostgreSQL-specific SQL operations: writable CTEs, FOR UPDATE SKIP LOCKED."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from ..models import Task
from ..runner_config import ENTRY_TYPES
from .db_handler import DbHandler

# Shared with the Java worker (java/aaiclick-worker embeds the same file as a
# build-time resource); capability differences between workers are bound
# values, not query edits. See the file's header comment.
CLAIM_NEXT_TASK_SQL = (Path(__file__).parent / "sql" / "claim_next_task.sql").read_text()


class PgDbHandler(DbHandler):
    """PostgreSQL: writable CTEs, FOR UPDATE SKIP LOCKED."""

    @staticmethod
    async def claim_next_task(session: AsyncSession, execution_worker_id: int, now: datetime) -> Task | None:
        result = await session.execute(
            text(CLAIM_NEXT_TASK_SQL),
            {
                "execution_worker_id": execution_worker_id,
                "now": now,
                # The Python worker executes every entry type and dispatches
                # image-based tasks itself, so its capability set is total.
                "entry_types": list(ENTRY_TYPES),
                "allow_image_tasks": True,
            },
        )

        row = result.mappings().fetchone()
        if row is None:
            return None

        task_data = dict(row)
        return Task(**task_data)

    @staticmethod
    def lock_query(query: Select) -> Select:
        return query.with_for_update()
