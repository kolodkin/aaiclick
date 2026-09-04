"""One-time backfills of SQL ``table_registry``.

- ``migrate_table_registry_to_sql`` copies CH ``table_registry`` rows into
  SQL (the registry used to live in ClickHouse).
- ``backfill_registry_names`` recovers ``name`` for global rows registered
  before names moved out of the table name.

Lives in the orchestration layer (not oplog) so the oplog module does not
have to import from orchestration — that would create an import cycle
via ``aaiclick.orchestration`` package init. The CH copy runs from
``orch_context.task_scope`` right after ``init_oplog_tables``; the name
backfill runs once per owned engine on ``orch_context`` entry.
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from aaiclick.data.data_context import ChClient
from aaiclick.data.scope import legacy_global_name
from aaiclick.tenancy import DEFAULT_TENANT_ID

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
            ch_rows = await ch_client.query(
                "SELECT table_name, job_id, task_id, run_id, created_at FROM table_registry"
            )
        except Exception:
            logger.debug("Failed to read CH table_registry for backfill", exc_info=True)
            return

        if ch_rows.result_rows:
            await session.execute(
                text(
                    "INSERT INTO table_registry "
                    "(table_name, job_id, task_id, run_id, created_at, schema_doc) "
                    "VALUES (:table_name, :job_id, :task_id, :run_id, :created_at, :schema_doc) "
                    "ON CONFLICT (table_name) DO NOTHING"
                ),
                [
                    {
                        "table_name": row[0],
                        "job_id": row[1],
                        "task_id": row[2],
                        "run_id": row[3],
                        "created_at": row[4],
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


async def backfill_registry_names() -> None:
    """Populate ``table_registry.name`` for legacy global rows.

    Global tables used to be named ``p_<name>`` / ``p_<tenant_id>_<name>``;
    the name now lives only in the registry. Rows from before that change
    have ``name IS NULL``, which would make their objects invisible to
    ``open_object`` and listing. Parse the name back out once, keeping the
    physical table untouched — nothing in ClickHouse has to be renamed.

    Idempotent: only rows still missing a name are considered, so after the
    first pass it is one empty ``SELECT``. Rows that do not parse (an opaque
    ``p_<snowflake>`` without a name) are left alone. Best effort — a
    database that predates ``table_registry`` (``aaiclick migrate`` not yet
    run) is logged, not fatal.
    """
    try:
        async with get_sql_session() as session:
            result = await session.execute(
                text("SELECT table_name, tenant_id FROM table_registry WHERE name IS NULL AND table_name LIKE 'p%'")
            )
            rows = result.all()
    except Exception:
        logger.debug("Skipping table_registry name backfill", exc_info=True)
        return
    updates = []
    for table_name, tenant_id in rows:
        name = legacy_global_name(table_name, tenant_id, DEFAULT_TENANT_ID)
        if name is not None:
            updates.append({"table_name": table_name, "name": name})
    if not updates:
        return
    async with get_sql_session() as session:
        for update in updates:
            # One statement per row: a duplicate (tenant_id, name) among
            # legacy rows should skip that row, not abort the whole pass.
            try:
                await session.execute(
                    text("UPDATE table_registry SET name = :name WHERE table_name = :table_name AND name IS NULL"),
                    update,
                )
            except Exception:
                logger.warning("Could not backfill registry name for %s", update["table_name"], exc_info=True)
        await session.commit()
        logger.info("Backfilled %d legacy table_registry names", len(updates))
