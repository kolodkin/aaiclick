"""
aaiclick.data.clickhouse_client - clickhouse-connect async client factory.

Creates an AsyncClient for distributed ClickHouse servers using
clickhouse-connect's native aiohttp async client (>=1.0.0). Connection
pooling is owned by ``aiohttp.ClientSession`` / ``TCPConnector`` per
client.
"""

from aaiclick.backend import parse_ch_url


async def create_clickhouse_client():
    """Create a clickhouse-connect AsyncClient from AAICLICK_CH_URL."""
    try:
        # Distributed extra only — keep inline so local-only installs can import this module.
        from clickhouse_connect import get_async_client  # noqa: PLC0415
    except ImportError as e:
        raise ImportError(
            "Remote ClickHouse requires the aaiclick[distributed] extra. "
            "Install with: pip install aaiclick[distributed]"
        ) from e

    return await get_async_client(**parse_ch_url())
