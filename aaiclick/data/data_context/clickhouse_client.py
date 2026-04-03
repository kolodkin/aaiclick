"""
aaiclick.data.clickhouse_client - clickhouse-connect async client factory.

Creates an AsyncClient for distributed ClickHouse servers using
clickhouse-connect with a shared urllib3 connection pool.
"""

import warnings
from urllib.parse import urlparse

from urllib3 import PoolManager

from aaiclick.backend import get_ch_url

# clickhouse-connect >=0.15 emits a FutureWarning about the async client
# being a thread-pool wrapper. Safe to ignore until 1.0 ships native async.
warnings.filterwarnings("ignore", message="The current async client", category=FutureWarning)

# Global connection pool shared across all contexts
_pool: list = [None]


def get_pool() -> PoolManager:
    """Get or create the global urllib3 connection pool."""
    if _pool[0] is None:
        _pool[0] = PoolManager(num_pools=10, maxsize=10)
    return _pool[0]


async def create_clickhouse_client():
    """Create a clickhouse-connect AsyncClient from AAICLICK_CH_URL."""
    try:
        from clickhouse_connect import get_async_client
    except ImportError as e:
        raise ImportError(
            "Remote ClickHouse requires the aaiclick[distributed] extra. "
            "Install with: pip install aaiclick[distributed]"
        ) from e

    parsed = urlparse(get_ch_url())
    return await get_async_client(
        pool_mgr=get_pool(),
        host=parsed.hostname or "localhost",
        port=parsed.port or 8123,
        username=parsed.username or "default",
        password=parsed.password or "",
        database=parsed.path.lstrip("/") or "default",
    )
