"""
aaiclick.data.clickhouse_client - clickhouse-connect async client factory.

Creates an AsyncClient for distributed ClickHouse servers using
clickhouse-connect with a shared urllib3 connection pool.
"""

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


async def create_clickhouse_client():
    """Create a clickhouse-connect AsyncClient from AAICLICK_CH_URL."""
    from clickhouse_connect import get_async_client

    parsed = urlparse(get_ch_url())
    return await get_async_client(
        pool_mgr=get_pool(),
        host=parsed.hostname or "localhost",
        port=parsed.port or 8123,
        username=parsed.username or "default",
        password=parsed.password or "",
        database=parsed.path.lstrip("/") or "default",
    )
