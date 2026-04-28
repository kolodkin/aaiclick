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

_migration_done = False


async def migrate_table_registry_to_sql(ch_client: ChClient) -> None:
    """Copy any CH ``table_registry`` rows into SQL, then drop the CH table.

    Idempotent:
    - If the CH table does not exist, this is a no-op (fresh install).
    - If the SQL table already has rows, skip the copy but still drop the
      CH side to finish the migration.
    - ``ON CONFLICT DO NOTHING`` on each insert makes concurrent workers safe.

    Guarded by a module-level flag so only the first call per process does
    CH I/O; subsequent calls return immediately. This keeps the
    ``task_scope`` hot path free of a CH round-trip after the first task.

    Runs at startup (not in Alembic) because migrations execute in a sync
    context and the CH client is async (chdb has no sync path).
    """
    global _migration_done
    if _migration_done:
        return

    try:
        exists = await ch_client.query("EXISTS TABLE table_registry")
        if not exists.result_rows or not exists.result_rows[0][0]:
            _migration_done = True
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
            _migration_done = True
            return

        try:
            ch_rows = await ch_client.query("SELECT table_name, job_id, task_id, created_at FROM table_registry")
        except Exception:
            logger.debug("Failed to read CH table_registry for backfill", exc_info=True)
            return

        if ch_rows.result_rows:
            await session.execute(
                text(
                    "INSERT INTO table_registry "
                    "(table_name, job_id, task_id, created_at, schema_doc) "
                    "VALUES (:table_name, :job_id, :task_id, :created_at, :schema_doc) "
                    "ON CONFLICT (table_name) DO NOTHING"
                ),
                [
                    {
                        "table_name": row[0],
                        "job_id": row[1],
                        "task_id": row[2],
                        "created_at": row[3],
                        # Legacy CH source had no schema_doc; backfilled rows surface a
                        # LookupError on read until they're re-created.
                        "schema_doc": None,
                    }
                    for row in ch_rows.result_rows
                ],
            )
            await session.commit()
            logger.info("Migrated %d table_registry rows from ClickHouse to SQL", len(ch_rows.result_rows))

    try:
        await ch_client.command("DROP TABLE IF EXISTS table_registry")
    except Exception:
        logger.debug("Failed to drop CH table_registry after backfill", exc_info=True)

    _migration_done = True
