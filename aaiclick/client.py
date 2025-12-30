"""
aaiclick.client - Global ClickHouse client manager.

This module provides a singleton client manager for ClickHouse connections,
allowing Object instances to share a single client connection.
"""

from typing import Optional, Any
from clickhouse_connect import get_async_client


class ClientManager:
    """
    Singleton manager for ClickHouse client connections.
    """

    _instance: Optional["ClientManager"] = None
    _client: Optional[Any] = None

    def __new__(cls) -> "ClientManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def connect(
        self,
        host: str = "localhost",
        port: int = 8123,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: str = "default",
        **kwargs,
    ) -> None:
        """
        Connect to ClickHouse server.

        Args:
            host: ClickHouse server host
            port: ClickHouse server port
            username: Optional username
            password: Optional password
            database: Database name
            **kwargs: Additional arguments for get_async_client
        """
        if self._client is not None:
            await self.close()

        self._client = await get_async_client(
            host=host, port=port, username=username, password=password, database=database, **kwargs
        )

    async def close(self) -> None:
        """Close the current client connection."""
        if self._client is not None:
            await self._client.close()
            self._client = None

    @property
    def client(self) -> Any:
        """
        Get the current client instance.

        Returns:
            ClickHouse async client

        Raises:
            RuntimeError: If client is not connected
        """
        if self._client is None:
            raise RuntimeError("Client not connected. Call await connect() first.")
        return self._client

    @property
    def is_connected(self) -> bool:
        """Check if client is currently connected."""
        return self._client is not None


# Global client manager instance
_client_manager = ClientManager()


async def connect(
    host: str = "localhost",
    port: int = 8123,
    username: Optional[str] = None,
    password: Optional[str] = None,
    database: str = "default",
    **kwargs,
) -> None:
    """
    Connect to ClickHouse server using the global client manager.

    Args:
        host: ClickHouse server host
        port: ClickHouse server port
        username: Optional username
        password: Optional password
        database: Database name
        **kwargs: Additional arguments for get_async_client
    """
    await _client_manager.connect(host=host, port=port, username=username, password=password, database=database, **kwargs)


async def close() -> None:
    """Close the global client connection."""
    await _client_manager.close()


def get_client() -> Any:
    """
    Get the global ClickHouse client instance.

    Returns:
        ClickHouse async client

    Raises:
        RuntimeError: If client is not connected
    """
    return _client_manager.client


def is_connected() -> bool:
    """Check if the global client is connected."""
    return _client_manager.is_connected
