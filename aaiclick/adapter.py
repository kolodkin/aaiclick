"""Adapter for clickhouse-connect client."""

from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Optional
import os


class QueryResult:
    """Unified result wrapper compatible with clickhouse-connect."""

    def __init__(self, result_rows: List[Tuple[Any, ...]]):
        """Initialize with result rows.

        Args:
            result_rows: List of tuples containing query results
        """
        self._result_rows = result_rows

    @property
    def result_rows(self) -> List[Tuple[Any, ...]]:
        """Return result rows as list of tuples (clickhouse-connect compatible)."""
        return self._result_rows


class ClientAdapter(ABC):
    """Abstract base class for database client adapters."""

    @abstractmethod
    async def query(self, sql: str) -> QueryResult:
        """Execute a query and return results.

        Args:
            sql: SQL query string

        Returns:
            QueryResult with result_rows property
        """
        pass

    @abstractmethod
    async def command(self, sql: str) -> None:
        """Execute a command without returning results.

        Args:
            sql: SQL command string
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close the client connection."""
        pass


class ClickHouseConnectAdapter(ClientAdapter):
    """Adapter for clickhouse-connect async client."""

    def __init__(self, client):
        """Initialize with clickhouse-connect async client.

        Args:
            client: clickhouse-connect async client instance
        """
        self._client = client

    async def query(self, sql: str) -> QueryResult:
        """Execute query using clickhouse-connect client."""
        result = await self._client.query(sql)
        return QueryResult(result.result_rows)

    async def command(self, sql: str) -> None:
        """Execute command using clickhouse-connect client."""
        await self._client.command(sql)

    async def close(self) -> None:
        """Close clickhouse-connect client."""
        await self._client.close()


async def create_client(backend: Optional[str] = None) -> ClientAdapter:
    """Create a client adapter for clickhouse-connect.

    Args:
        backend: Backend type (only 'clickhouse-connect' is supported).
                If None, uses environment variable AAICLICK_BACKEND or defaults to clickhouse-connect

    Returns:
        ClientAdapter instance
    """
    backend = backend or os.getenv("AAICLICK_BACKEND", "clickhouse-connect")

    if backend == "clickhouse-connect":
        # Use clickhouse-connect for remote server connections
        from clickhouse_connect import get_async_client

        host = os.getenv("CLICKHOUSE_HOST", "localhost")
        port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        username = os.getenv("CLICKHOUSE_USER", "default")
        password = os.getenv("CLICKHOUSE_PASSWORD", "")
        database = os.getenv("CLICKHOUSE_DB", "default")

        client = await get_async_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        )

        return ClickHouseConnectAdapter(client)

    else:
        raise ValueError(f"Unknown backend: {backend}. Only 'clickhouse-connect' is supported")
