"""One-time migration of CH ``table_registry`` rows into SQL.

Lives in the orchestration layer (not oplog) so the oplog module does not
have to import from orchestration — that would create an import cycle
via ``aaiclick.orchestration`` package init. The backfill is triggered
from ``orch_context.task_scope`` right after ``init_oplog_tables``.
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from aaiclick.data.data_context import ChClient

from .sql_context import get_sql_session

logger = logging.getLogger(__name__)


async def migrate_table_registry_to_sql(ch_client: ChClient) -> None:
    """Copy any CH ``table_registry`` rows into SQL, then drop the CH table.

    Idempotent:
    - If the CH table does not exist, this is a no-op (fresh install).
    - If the SQL table already has rows, skip the copy but still drop the
      CH side to finish the migration.
    - ``ON CONFLICT DO NOTHING`` on each insert makes concurrent workers safe.

    Runs at startup (not in Alembic) because migrations execute in a sync
    context and the CH client is async (chdb has no sync path).
    """
    try:
        exists = await ch_client.query("EXISTS TABLE table_registry")
        if not exists.result_rows or not exists.result_rows[0][0]:
            return
    except Exception:
        logger.debug("EXISTS TABLE table_registry check failed", exc_info=True)
        return

    async with get_sql_session() as session:
        sql_count = await session.execute(text("SELECT COUNT(*) FROM table_registry"))
        if sql_count.scalar() != 0:
            try:
                await ch_client.command("DROP TABLE IF EXISTS table_registry")
            except Exception:
                logger.debug("Failed to drop CH table_registry after SQL already populated", exc_info=True)
            return

        try:
            ch_rows = await ch_client.query(
                "SELECT table_name, job_id, task_id, run_id, created_at FROM table_registry"
            )
        except Exception:
            logger.debug("Failed to read CH table_registry for backfill", exc_info=True)
            return

        for table_name, job_id, task_id, run_id, created_at in ch_rows.result_rows:
            await session.execute(
                text(
                    "INSERT INTO table_registry "
                    "(table_name, job_id, task_id, run_id, created_at) "
                    "VALUES (:table_name, :job_id, :task_id, :run_id, :created_at) "
                    "ON CONFLICT (table_name) DO NOTHING"
                ),
                {
                    "table_name": table_name,
                    "job_id": job_id,
                    "task_id": task_id,
                    "run_id": run_id,
                    "created_at": created_at,
                },
            )
        await session.commit()
        logger.info("Migrated %d table_registry rows from ClickHouse to SQL", len(ch_rows.result_rows))

    try:
        await ch_client.command("DROP TABLE IF EXISTS table_registry")
    except Exception:
        logger.debug("Failed to drop CH table_registry after backfill", exc_info=True)
