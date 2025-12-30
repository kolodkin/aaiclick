"""
aaiclick.client - Global ClickHouse client.

This module provides a simple global client for ClickHouse connections.
Connection parameters default to environment variables.
"""

import os
from typing import Any
from clickhouse_connect import get_async_client


# Global client instance holder (list with single element)
_client = [None]


async def get_client() -> Any:
    """
    Get the global ClickHouse client instance.

    Automatically connects using environment variables if not already connected:
    - CLICKHOUSE_HOST (default: "localhost")
    - CLICKHOUSE_PORT (default: 8123)
    - CLICKHOUSE_USER (default: None)
    - CLICKHOUSE_PASSWORD (default: None)
    - CLICKHOUSE_DATABASE (default: "default")

    Returns:
        ClickHouse async client
    """
    if _client[0] is None:
        host = os.getenv("CLICKHOUSE_HOST", "localhost")
        port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        username = os.getenv("CLICKHOUSE_USER")
        password = os.getenv("CLICKHOUSE_PASSWORD")
        database = os.getenv("CLICKHOUSE_DATABASE", "default")

        _client[0] = await get_async_client(
            host=host, port=port, username=username, password=password, database=database
        )

    return _client[0]


def is_connected() -> bool:
    """Check if the global client is connected."""
    return _client[0] is not None
