"""Tests for aaiclick.locks — advisory lock acquisition and id resolution."""

from sqlalchemy import text

from aaiclick.locks import _LOCK_ONLY_CONTEXT_ID, _advisory_id_cache, load_advisory_id, table_insert_lock
from aaiclick.orchestration.sql_context import get_sql_session


async def test_load_advisory_id_no_orch_context_returns_none():
    """Without an active orch_context the SQL engine is unavailable."""
    _advisory_id_cache.clear()
    assert await load_advisory_id("p_nonexistent") is None


async def test_load_advisory_id_returns_existing_row(orch_ctx):
    """Reads advisory_id back from a pre-existing table_context_refs row."""
    _advisory_id_cache.clear()
    async with get_sql_session() as session:
        await session.execute(
            text(
                "INSERT INTO table_context_refs (table_name, context_id, advisory_id) "
                "VALUES ('t_test_existing', 42, 999111)"
            )
        )
        await session.commit()

    assert await load_advisory_id("t_test_existing") == 999111


async def test_load_advisory_id_mints_sentinel_row_when_missing(orch_ctx):
    """First read of an unregistered table mints a sentinel row."""
    _advisory_id_cache.clear()
    advisory_id = await load_advisory_id("p_test_mint")

    assert advisory_id is not None and advisory_id > 0

    async with get_sql_session() as session:
        result = await session.execute(
            text(
                "SELECT context_id, advisory_id FROM table_context_refs "
                "WHERE table_name = 'p_test_mint'"
            )
        )
        rows = result.fetchall()

    assert rows == [(_LOCK_ONLY_CONTEXT_ID, advisory_id)]


async def test_load_advisory_id_caches_per_process(orch_ctx):
    """Repeated calls return the same id without re-querying SQL."""
    _advisory_id_cache.clear()
    first = await load_advisory_id("p_test_cache")

    # Mutate the row out-of-band — cached lookup should still see the original.
    async with get_sql_session() as session:
        await session.execute(
            text("UPDATE table_context_refs SET advisory_id = 12345 WHERE table_name = 'p_test_cache'")
        )
        await session.commit()

    second = await load_advisory_id("p_test_cache")
    assert second == first


async def test_table_insert_lock_none_advisory_id_is_noop():
    """A None key short-circuits without touching any SQL connection."""
    async with table_insert_lock(None):
        pass


async def test_table_insert_lock_local_mode_is_noop(orch_ctx):
    """In local (SQLite) mode the lock yields without acquiring anything."""
    # No assertions on PG state — local mode never opens a PG connection.
    # The test passes as long as the context manager yields cleanly.
    async with table_insert_lock(123):
        pass
