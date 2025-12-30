"""
aaiclick.client - Global ClickHouse client.

This module provides a simple global client for ClickHouse connections.
Connection parameters default to environment variables.
"""

import os
from typing import Optional, Any
from clickhouse_connect import get_async_client


# Global client instance
_client: Optional[Any] = None


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
    global _client

    if _client is None:
        host = os.getenv("CLICKHOUSE_HOST", "localhost")
        port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        username = os.getenv("CLICKHOUSE_USER")
        password = os.getenv("CLICKHOUSE_PASSWORD")
        database = os.getenv("CLICKHOUSE_DATABASE", "default")

        _client = await get_async_client(
            host=host, port=port, username=username, password=password, database=database
        )

    return _client


def is_connected() -> bool:
    """Check if the global client is connected."""
    return _client is not None
