"""
aaiclick.data.ch_client - ClickHouse client protocol and factory dispatch.

Defines ChClient Protocol for type-safe usage across the codebase,
and lazy-imports the appropriate concrete client based on AAICLICK_CH_URL.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextvars import ContextVar
from typing import Protocol, cast
from urllib.parse import urlparse

from aaiclick.backend import is_chdb


class QueryResult(Protocol):
    """Protocol for ClickHouse query results."""

    result_rows: list[tuple]
    column_names: list[str]


class ChClient(Protocol):
    """Protocol for async ClickHouse client operations.

    Both ChdbClient and clickhouse-connect AsyncClient satisfy this protocol.
    """

    async def command(self, query: str, settings: dict | None = None) -> object: ...
    async def query(self, query: str, settings: dict | None = None) -> QueryResult: ...
    async def insert(
        self,
        table: str,
        data: Sequence[Sequence],
        column_names: Sequence[str] | None = None,
        column_oriented: bool = False,
        column_type_names: Sequence[str] | None = None,
    ) -> None: ...


_ch_client_var: ContextVar[ChClient | None] = ContextVar('ch_client', default=None)


def get_ch_client() -> ChClient:
    """Return the ClickHouse client for the active data context."""
    client = _ch_client_var.get()
    if client is None:
        raise RuntimeError(
            "No active data or orch context — "
            "use 'async with data_context()' or 'async with orch_context()'"
        )
    return client


async def create_ch_client() -> ChClient:
    """Create a ClickHouse client from AAICLICK_CH_URL."""
    if is_chdb():
        from .chdb_client import create_chdb_client

        return create_chdb_client()

    from .clickhouse_client import create_clickhouse_client

    return cast(ChClient, await create_clickhouse_client())


def create_sync_client(connection_string: str) -> object:
    """Create a sync ClickHouse client from a connection string.

    Supports:
    - chdb:///path/to/data → ChdbSyncClient
    - clickhouse://user:pass@host:port/db → clickhouse-connect sync client
    """
    if connection_string.startswith("chdb://"):
        from .chdb_client import create_chdb_sync_client

        return create_chdb_sync_client(connection_string)

    from clickhouse_connect import get_client

    parsed = urlparse(connection_string)
    return get_client(
        host=parsed.hostname or "localhost",
        port=parsed.port or 8123,
        username=parsed.username or "default",
        password=parsed.password or "",
        database=parsed.path.lstrip("/") or "default",
    )
