"""Tests for aaiclick.locks — advisory lock acquisition and id resolution."""

import importlib
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import text

from aaiclick import create_object_from_value
from aaiclick.data.object import ingest as ingest_mod
from aaiclick.locks import _advisory_id_cache, load_advisory_id, table_insert_lock
from aaiclick.orchestration.sql_context import get_sql_session

# Module-object import bypasses aaiclick.data.data_context.__init__'s
# re-export of data_context() (the function), which shadows the module name.
_data_context_mod = importlib.import_module("aaiclick.data.data_context.data_context")


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
            text("SELECT context_id, advisory_id FROM table_context_refs WHERE table_name = 'p_test_mint'")
        )
        rows = result.fetchall()

    # Sentinel context_id is negative (out of band vs positive Snowflake IDs);
    # exact value is an implementation detail.
    assert len(rows) == 1
    assert rows[0][0] < 0
    assert rows[0][1] == advisory_id


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


# ---------------------------------------------------------------------------
# Mock tests for the distributed-mode lock path.
#
# Local tests cannot execute `SELECT pg_advisory_lock(...)` against SQLite,
# so we stub `is_distributed()` + inject a recording engine. These verify
# the exact SQL call sequence: acquire → body → release.
# ---------------------------------------------------------------------------


class _RecordingConn:
    """Mock async connection that records every executed statement."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def __aenter__(self) -> "_RecordingConn":
        return self

    async def __aexit__(self, *_exc_info) -> None:
        return None

    async def execute(self, stmt, params=None):
        self.calls.append((str(stmt), params or {}))


class _RecordingEngine:
    def __init__(self) -> None:
        self.conn = _RecordingConn()

    def connect(self) -> _RecordingConn:
        return self.conn


async def test_table_insert_lock_distributed_issues_lock_unlock(monkeypatch):
    """Distributed mode: acquires then releases the advisory lock around yield."""
    from aaiclick.orchestration import sql_context

    monkeypatch.setattr("aaiclick.locks.is_distributed", lambda: True)
    engine = _RecordingEngine()
    token = sql_context._sql_engine_var.set(engine)  # type: ignore[arg-type]
    try:
        async with table_insert_lock(777):
            engine.conn.calls.append(("<body>", {}))
    finally:
        sql_context._sql_engine_var.reset(token)

    sqls = [c[0] for c in engine.conn.calls]
    assert "pg_advisory_lock" in sqls[0]
    assert sqls[1] == "<body>"
    assert "pg_advisory_unlock" in sqls[2]
    assert engine.conn.calls[0][1] == {"k": 777}
    assert engine.conn.calls[2][1] == {"k": 777}


async def test_table_insert_lock_releases_on_body_exception(monkeypatch):
    """Unlock fires even when the wrapped code raises."""
    from aaiclick.orchestration import sql_context

    monkeypatch.setattr("aaiclick.locks.is_distributed", lambda: True)
    engine = _RecordingEngine()
    token = sql_context._sql_engine_var.set(engine)  # type: ignore[arg-type]
    try:
        with pytest.raises(RuntimeError, match="boom"):
            async with table_insert_lock(42):
                raise RuntimeError("boom")
    finally:
        sql_context._sql_engine_var.reset(token)

    sqls = [c[0] for c in engine.conn.calls]
    assert "pg_advisory_lock" in sqls[0]
    assert "pg_advisory_unlock" in sqls[-1]


# ---------------------------------------------------------------------------
# Wrap-site coverage: spy on table_insert_lock to verify every insert code
# path that should serialize actually routes through it. Catches "someone
# added a new insert path and forgot to wrap it" regressions without needing
# a real Postgres server.
# ---------------------------------------------------------------------------


def _install_lock_spy(monkeypatch, module) -> list[int | None]:
    """Replace ``table_insert_lock`` on ``module`` with a recording no-op.

    Returns the list of advisory_ids the code under test tried to lock on.
    """
    calls: list[int | None] = []

    @asynccontextmanager
    async def spy(advisory_id):
        calls.append(advisory_id)
        yield

    monkeypatch.setattr(module, "table_insert_lock", spy)
    return calls


async def test_insert_objects_db_acquires_lock(orch_ctx, monkeypatch):
    """Object.insert goes through insert_objects_db which locks its target."""
    calls = _install_lock_spy(monkeypatch, ingest_mod)

    target = await create_object_from_value([1, 2, 3], name="lock_insert_target")
    source = await create_object_from_value([4, 5, 6])
    await target.insert(source)

    assert len(calls) == 1
    assert calls[0] is not None


async def test_concat_objects_db_acquires_lock(orch_ctx, monkeypatch):
    """Object.concat goes through concat_objects_db which locks the result."""
    calls = _install_lock_spy(monkeypatch, ingest_mod)

    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([4, 5, 6])
    await a.concat(b)

    assert len(calls) == 1
    assert calls[0] is not None


async def test_create_object_from_value_named_acquires_lock(orch_ctx, monkeypatch):
    """Persistent destination (name=...) locks; unnamed destination does not."""
    calls = _install_lock_spy(monkeypatch, _data_context_mod)

    await create_object_from_value([1, 2, 3], name="lock_named_target")
    assert len(calls) == 1 and calls[0] is not None

    calls.clear()
    await create_object_from_value([1, 2, 3])  # name=None
    assert calls == []


async def test_copy_db_is_lock_free(orch_ctx, monkeypatch):
    """copy() is documented as not serialized — must NOT acquire the lock."""
    calls = _install_lock_spy(monkeypatch, ingest_mod)

    src = await create_object_from_value([1, 2, 3])
    await src.copy()

    assert calls == []
