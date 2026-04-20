"""Shared test helpers — per-test state reset for CH and SQL backends.

Kept here (not inside a conftest) so any test module — orchestration,
oplog, ai, data — can import the same primitives. conftests stay small
and compose these helpers into fixtures.
"""

from sqlalchemy import text

from aaiclick.backend import is_local
from aaiclick.data.data_context import get_ch_client
from aaiclick.orchestration.orch_context import get_sql_session


async def reset_sql_tables() -> None:
    """Truncate all user tables in the active SQL database (distributed mode).

    Local mode (SQLite) is expected to use tempdir-per-test, so the DB is
    already empty at test start — this is a no-op there.
    """
    if is_local():
        return
    async with get_sql_session() as session:
        result = await session.execute(
            text("SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename != 'alembic_version'")
        )
        tables = [r[0] for r in result.all()]
        if tables:
            quoted = ", ".join(f'"{t}"' for t in tables)
            await session.execute(text(f"TRUNCATE {quoted} RESTART IDENTITY CASCADE"))
        await session.commit()


async def drop_all_ch_tables() -> None:
    """Drop every table in the active CH database.

    Singletons (operation_log) are recreated lazily by init_oplog_tables
    on next task_scope entry. Safe against real CH because
    ``_ch_worker_setup`` gives each xdist worker its own database, so
    this never touches another worker's tables.
    """
    ch = get_ch_client()
    result = await ch.query("SELECT name FROM system.tables WHERE database = currentDatabase()")
    for row in result.result_rows:
        await ch.command(f"DROP TABLE IF EXISTS `{row[0]}`")
