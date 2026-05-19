"""
aaiclick.data.clickhouse_client - clickhouse-connect async client factory.

Creates an AsyncClient for distributed ClickHouse servers using
clickhouse-connect. As of clickhouse-connect 1.0.0 the async client is a
native aiohttp implementation and manages its own connection pool via
``aiohttp.ClientSession`` / ``TCPConnector`` — the urllib3-based
``pool_mgr`` argument is no longer accepted on the async path.
"""

import warnings
from urllib.parse import urlparse

from aaiclick.backend import get_ch_url


def _ignore_async_wrapper_warning():
    """clickhouse-connect 0.15.x emits a FutureWarning about the thread-pool
    async wrapper. The warning is gone in 1.0+; filter kept while the floor
    pin still permits 0.15.x."""
    warnings.filterwarnings("ignore", message="The current async client", category=FutureWarning)


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
    with warnings.catch_warnings():
        _ignore_async_wrapper_warning()
        return await get_async_client(
            host=parsed.hostname or "localhost",
            port=parsed.port or 8123,
            username=parsed.username or "default",
            password=parsed.password or "",
            database=parsed.path.lstrip("/") or "default",
        )
