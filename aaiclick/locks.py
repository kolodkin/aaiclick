"""
aaiclick.locks - Cross-process insert serialization for shared CH tables.

This module exposes ``table_insert_lock(advisory_id)``, a per-table session-
level PostgreSQL advisory lock taken around the CH INSERT. Concurrent
workers inserting into the same shared ClickHouse table need this to keep
each INSERT's rows from interleaving with another worker's. In local mode
(chdb + SQLite) it is a no-op — chdb's single-process constraint already
prevents the race.

The ``advisory_id`` is the 64-bit Snowflake stored on
``table_registry.advisory_id`` and minted lazily by ``load_advisory_id``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from sqlalchemy import text

from .backend import is_distributed
from .snowflake import get_snowflake_id

# Per-process cache: table_name -> advisory_id. Safe because advisory_id
# never changes for a table's lifetime; entries become stale only after the
# table is dropped, in which case nothing else uses the lock anyway.
_advisory_id_cache: dict[str, int] = {}


async def lookup_advisory_id(session, table_name: str) -> int | None:
    """Return the advisory_id bound to ``table_name``, or None if unregistered.

    In distributed mode, first acquires a transient
    ``pg_advisory_xact_lock(hashtext(table_name))`` so two concurrent callers
    cannot bind different advisory_ids to the same table_name.
    """
    if is_distributed():
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:n))"),
            {"n": table_name},
        )
    return (
        await session.execute(
            text("SELECT advisory_id FROM table_registry WHERE table_name = :n"),
            {"n": table_name},
        )
    ).scalar_one_or_none()


async def load_advisory_id(table_name: str) -> int | None:
    """Return the advisory_id bound to ``table_name`` in ``table_registry``.

    Mints + UPSERTs an advisory_id on first call for tables that don't yet
    have one (e.g. ``p_*`` globals registered outside ``register_table``).
    Returns None when no active orch_context — caller treats that as
    "no lock needed". Cached per-process once read.
    """
    cached = _advisory_id_cache.get(table_name)
    if cached is not None:
        return cached

    # Lazy import — aaiclick.orchestration.sql_context pulls in the
    # orchestration package, which transitively imports data_context. Top-
    # level import here would create a cycle.
    from .orchestration.sql_context import _sql_engine_var, get_sql_session

    if _sql_engine_var.get() is None:
        return None

    async with get_sql_session() as session:
        existing = await lookup_advisory_id(session, table_name)
        if existing is not None:
            advisory_id = existing
        else:
            advisory_id = get_snowflake_id()
            await session.execute(
                text(
                    "INSERT INTO table_registry "
                    "(table_name, advisory_id, created_at) "
                    "VALUES (:n, :a, :now) "
                    "ON CONFLICT (table_name) DO UPDATE "
                    "SET advisory_id = COALESCE(table_registry.advisory_id, EXCLUDED.advisory_id)"
                ),
                {"n": table_name, "a": advisory_id, "now": _utcnow()},
            )
            await session.commit()
            advisory_id = (
                await session.execute(
                    text("SELECT advisory_id FROM table_registry WHERE table_name = :n"),
                    {"n": table_name},
                )
            ).scalar_one()

    _advisory_id_cache[table_name] = advisory_id
    return advisory_id


def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


@asynccontextmanager
async def table_insert_lock(advisory_id: int | None) -> AsyncIterator[None]:
    """Serialize concurrent inserts targeting the same advisory_id.

    Acquires a session-level PostgreSQL advisory lock on the given key for
    the lifetime of the ``async with`` block. PG releases session-level
    locks on backend disconnect, so a worker crash mid-INSERT cannot strand
    the lock.

    Yields immediately (no lock) when:
    - ``advisory_id`` is None — caller could not resolve a key.
    - ``is_distributed()`` is False — local mode is single-process.
    - No active orch_context — the SQL engine is not available.
    """
    if advisory_id is None or not is_distributed():
        yield
        return

    from .orchestration.sql_context import _sql_engine_var

    engine = _sql_engine_var.get()
    if engine is None:
        yield
        return
    async with engine.connect() as conn:
        await conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": advisory_id})
        try:
            yield
        finally:
            await conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": advisory_id})
