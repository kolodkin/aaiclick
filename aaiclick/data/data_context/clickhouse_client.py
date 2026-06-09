"""
aaiclick.data.clickhouse_client - clickhouse-connect async client factory.

Creates an AsyncClient for distributed ClickHouse servers using
clickhouse-connect's native aiohttp async client (>=1.0.0). Connection
pooling is owned by ``aiohttp.ClientSession`` / ``TCPConnector`` per
client.
"""

from urllib.parse import urlparse

from aaiclick.backend import get_ch_url


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
        host=parsed.hostname or "localhost",
        port=parsed.port or 8123,
        username=parsed.username or "default",
        password=parsed.password or "",
        database=parsed.path.lstrip("/") or "default",
    )
