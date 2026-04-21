"""Shared test helpers — per-test state reset for CH and SQL backends.

Kept here (not inside a conftest) so any test module — orchestration,
oplog, ai, data — can import the same primitives. conftests stay small
and compose these helpers into fixtures.
"""

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from sqlalchemy import text

from aaiclick.backend import is_local
from aaiclick.data.data_context import get_ch_client
from aaiclick.orchestration.orch_context import get_sql_session


async def reset_sql_tables() -> None:
    """Delete rows from every user table in the active SQL database.

    Uses ``DELETE FROM`` on SQLite and ``TRUNCATE ... RESTART IDENTITY
    CASCADE`` on Postgres. Alembic's ``alembic_version`` is preserved so
    migrations don't have to re-run between tests.
    """
    async with get_sql_session() as session:
        if is_local():
            result = await session.execute(
                text("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
            )
            tables = [r[0] for r in result.all() if r[0] != "alembic_version"]
            for name in tables:
                await session.execute(text(f'DELETE FROM "{name}"'))
        else:
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


@asynccontextmanager
async def module_orch_scope(ctx: AbstractAsyncContextManager[None]) -> AsyncIterator[None]:
    """Enter an orch context once for the lifetime of a module-scoped fixture.

    Used in pair with ``per_test_reset()``: the orch context (and its chdb
    session) is set up once per test module, and individual tests wipe
    state via ``per_test_reset()``. This lets the chdb Session live
    across many tests inside a module — avoiding the per-test Session
    teardown that triggers a chdb ThreadPool race (see
    ``docs/technical_debt.md``).
    """
    async with ctx:
        yield


async def per_test_reset(*, reset_ch: bool = True, reset_sql: bool = True) -> None:
    """Wipe CH and/or SQL state between tests sharing a module orch scope.

    Must be called while the enclosing orch context is active. When
    ``reset_ch`` is true, requires ``with_ch=True`` in the enclosing
    ``orch_context``.
    """
    if reset_ch:
        await drop_all_ch_tables()
    if reset_sql:
        await reset_sql_tables()


@asynccontextmanager
async def reset_test_state(
    ctx: AbstractAsyncContextManager[None],
    *,
    reset_ch: bool = True,
    reset_sql: bool = False,
) -> AsyncIterator[None]:
    """Enter ``ctx``, reset the requested backend state, then yield.

    Function-scoped alternative to ``module_orch_scope`` + ``per_test_reset``
    — kept for tests/fixtures that genuinely need their own orch context
    per function.
    """
    async with ctx:
        await per_test_reset(reset_ch=reset_ch, reset_sql=reset_sql)
        yield
