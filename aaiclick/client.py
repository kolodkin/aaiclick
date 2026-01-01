"""
aaiclick.client - Global ClickHouse client.

This module provides a simple global client for ClickHouse connections.
Connection parameters default to environment variables.
"""

import os
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect import get_async_client


# Global client instance holder (list with single element)
_client = [None]


async def get_client() -> AsyncClient:
    """
    Get the global ClickHouse client instance.

    Automatically connects using environment variables if not already connected:

    Connection parameters:
    - CLICKHOUSE_HOST (default: "localhost")
    - CLICKHOUSE_PORT (default: 8123)
    - CLICKHOUSE_USER (default: "default")
    - CLICKHOUSE_PASSWORD (default: "")
    - CLICKHOUSE_DB (default: "default")

    Returns:
        clickhouse-connect AsyncClient instance
    """
    if _client[0] is None:
        host = os.getenv("CLICKHOUSE_HOST", "localhost")
        port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        username = os.getenv("CLICKHOUSE_USER", "default")
        password = os.getenv("CLICKHOUSE_PASSWORD", "")
        database = os.getenv("CLICKHOUSE_DB", "default")

        _client[0] = await get_async_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        )

    return _client[0]


def is_connected() -> bool:
    """Check if the global client is connected."""
    return _client[0] is not None
