"""
aaiclick.data.clickhouse_client - clickhouse-connect async client factory.

Creates an AsyncClient for distributed ClickHouse servers using
clickhouse-connect with a shared urllib3 connection pool.
"""

from __future__ import annotations

from typing import Optional, Sequence
from urllib.parse import urlparse

from urllib3 import PoolManager

from aaiclick.backend import get_ch_url

# Global connection pool shared across all contexts
_pool: list = [None]


def get_pool() -> PoolManager:
    """Get or create the global urllib3 connection pool."""
    if _pool[0] is None:
        _pool[0] = PoolManager(num_pools=10, maxsize=10)
    return _pool[0]


class ClickHouseClient:
    """Thin wrapper around clickhouse-connect AsyncClient adding insert_columns.

    Delegates command/query/insert to the underlying AsyncClient and adds
    insert_columns for efficient columnar inserts using column_oriented=True.
    """

    def __init__(self, client: object):
        self._client = client

    async def command(self, query: str, settings: Optional[dict] = None) -> object:
        return await self._client.command(query, settings=settings)

    async def query(self, query: str, settings: Optional[dict] = None) -> object:
        return await self._client.query(query, settings=settings)

    async def insert(
        self,
        table: str,
        data: Sequence[Sequence],
        column_names: Optional[Sequence[str]] = None,
    ) -> None:
        await self._client.insert(table, data, column_names=column_names)

    async def insert_columns(
        self,
        table: str,
        column_data: dict[str, list],
    ) -> None:
        """Insert columnar data using clickhouse-connect's column_oriented mode."""
        names = list(column_data.keys())
        columns = list(column_data.values())
        await self._client.insert(
            table, columns, column_names=names, column_oriented=True,
        )


async def create_clickhouse_client() -> ClickHouseClient:
    """Create a clickhouse-connect AsyncClient from AAICLICK_CH_URL."""
    try:
        from clickhouse_connect import get_async_client
    except ImportError as e:
        raise ImportError(
            "Remote ClickHouse requires the aaiclick[distributed] extra. "
            "Install with: pip install aaiclick[distributed]"
        ) from e

    parsed = urlparse(get_ch_url())
    client = await get_async_client(
        pool_mgr=get_pool(),
        host=parsed.hostname or "localhost",
        port=parsed.port or 8123,
        username=parsed.username or "default",
        password=parsed.password or "",
        database=parsed.path.lstrip("/") or "default",
    )
    return ClickHouseClient(client)
