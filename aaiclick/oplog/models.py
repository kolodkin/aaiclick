"""
aaiclick.oplog.models - startup entry point for the internal ClickHouse
schema (``operation_log``, ``task_logs``), driven by the migration runner
in ``aaiclick.oplog.migrate``. DDL lives in ``aaiclick/oplog/migrations/``.
"""

from __future__ import annotations

from functools import lru_cache

from aaiclick.backend import is_local
from aaiclick.data.data_context import ChClient

from .migrate import ch_pending, ch_upgrade

INTERNAL_TABLES = ("operation_log", "task_logs")


@lru_cache(maxsize=1)
def _schema_cache() -> dict[str, dict[str, str]]:
    """Process-wide store for column types read from ``system.columns``.

    ``lru_cache`` cannot wrap the async reader itself, so it holds the
    mutable store instead — same once-per-process semantics, and
    ``cache_clear()`` resets it (used by ``aaiclick.testing`` between tests).
    """
    return {}


def clear_schema_cache() -> None:
    """Drop the cached column types so the next read hits ``system.columns``."""
    _schema_cache.cache_clear()


async def get_column_types(ch_client: ChClient, table: str) -> dict[str, str]:
    """Column name → ClickHouse type for an internal table, in schema order.

    Reads ``system.columns`` once per process (all internal tables in one
    query) and serves every later call from the cache. The tables must
    exist — call after ``init_oplog_tables``.
    """
    cache = _schema_cache()
    if set(cache) != set(INTERNAL_TABLES):
        tables_in = ", ".join(f"'{t}'" for t in INTERNAL_TABLES)
        result = await ch_client.query(
            f"SELECT table, name, type FROM system.columns"
            f" WHERE database = currentDatabase() AND table IN ({tables_in})"
            f" ORDER BY table, position"
        )
        for table_name, column, ch_type in result.result_rows:
            cache.setdefault(table_name, {})[column] = ch_type
    if table not in cache:
        raise RuntimeError(f"No columns found for ClickHouse table '{table}' — run init_oplog_tables first.")
    return cache[table]


async def init_oplog_tables(ch_client: ChClient) -> None:
    """Bring the internal CH schema up to date, or fail asking for a migrate.

    Local mode (chdb + SQLite) applies pending migrations directly —
    single-process, zero-ops, mirrors SQLite's create-on-setup. Distributed
    mode never writes: the operator runs ``aaiclick migrate upgrade``.
    """
    if is_local():
        await ch_upgrade(ch_client)
        return

    pending = await ch_pending(ch_client)
    if pending:
        raise RuntimeError(
            f"ClickHouse schema is behind (pending: {', '.join(pending)}). Run: aaiclick migrate upgrade"
        )
