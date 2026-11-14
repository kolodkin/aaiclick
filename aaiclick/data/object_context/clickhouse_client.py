"""
aaiclick.data.clickhouse_client - clickhouse-connect async client factory.

See docs/user_guide/object_context.md — "Deployment Modes".

Creates an AsyncClient for distributed ClickHouse servers using
clickhouse-connect's native aiohttp async client (>=1.0.0). Connection
pooling is owned by ``aiohttp.ClientSession`` / ``TCPConnector`` per
client.
"""

from clickhouse_connect import get_async_client

from aaiclick.backend import parse_ch_url


async def create_clickhouse_client():
    """Create a clickhouse-connect AsyncClient from AAICLICK_CH_URL."""
    return await get_async_client(**parse_ch_url())
