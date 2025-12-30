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


async def connect(
    host: Optional[str] = None,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    **kwargs,
) -> None:
    """
    Connect to ClickHouse server.

    Connection parameters default to environment variables:
    - CLICKHOUSE_HOST (default: "localhost")
    - CLICKHOUSE_PORT (default: 8123)
    - CLICKHOUSE_USER (default: None)
    - CLICKHOUSE_PASSWORD (default: None)
    - CLICKHOUSE_DATABASE (default: "default")

    Args:
        host: ClickHouse server host
        port: ClickHouse server port
        username: Optional username
        password: Optional password
        database: Database name
        **kwargs: Additional arguments for get_async_client
    """
    global _client

    if _client is not None:
        await close()

    # Use provided values or fall back to environment variables
    host = host or os.getenv("CLICKHOUSE_HOST", "localhost")
    port = port or int(os.getenv("CLICKHOUSE_PORT", "8123"))
    username = username or os.getenv("CLICKHOUSE_USER")
    password = password or os.getenv("CLICKHOUSE_PASSWORD")
    database = database or os.getenv("CLICKHOUSE_DATABASE", "default")

    _client = await get_async_client(
        host=host, port=port, username=username, password=password, database=database, **kwargs
    )


async def close() -> None:
    """Close the global client connection."""
    global _client
    if _client is not None:
        await _client.close()
        _client = None


def get_client() -> Any:
    """
    Get the global ClickHouse client instance.

    Returns:
        ClickHouse async client

    Raises:
        RuntimeError: If client is not connected
    """
    if _client is None:
        raise RuntimeError("Client not connected. Call await connect() first.")
    return _client


def is_connected() -> bool:
    """Check if the global client is connected."""
    return _client is not None
