"""
aaiclick.client - Global ClickHouse client.

This module provides a simple global client for ClickHouse connections.
Connection parameters default to environment variables.
"""

from typing import Any
from .adapter import ClientAdapter, create_client


# Global client instance holder (list with single element)
_client = [None]


async def get_client() -> ClientAdapter:
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
        ClientAdapter instance with unified interface
    """
    if _client[0] is None:
        _client[0] = await create_client()

    return _client[0]


def is_connected() -> bool:
    """Check if the global client is connected."""
    return _client[0] is not None
