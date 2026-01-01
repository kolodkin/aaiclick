"""
aaiclick.client - Global ClickHouse client.

This module provides a simple global client for ClickHouse connections.
Connection parameters are read from env.py.
"""

from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect import get_async_client
from .env import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DB,
)


# Global client instance holder (list with single element)
_client = [None]


async def get_client() -> AsyncClient:
    """
    Get the global ClickHouse client instance.

    Automatically connects using configuration from env.py if not already connected.

    Connection parameters are read from environment variables:
    - CLICKHOUSE_HOST (default: "localhost")
    - CLICKHOUSE_PORT (default: 8123)
    - CLICKHOUSE_USER (default: "default")
    - CLICKHOUSE_PASSWORD (default: "")
    - CLICKHOUSE_DB (default: "default")

    Returns:
        clickhouse-connect AsyncClient instance
    """
    if _client[0] is None:
        _client[0] = await get_async_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB,
        )

    return _client[0]


def is_connected() -> bool:
    """Check if the global client is connected."""
    return _client[0] is not None
