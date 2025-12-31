"""Unified adapter for chdb and clickhouse-connect clients."""

from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Optional
import os


class QueryResult:
    """Unified result wrapper compatible with both chdb and clickhouse-connect."""

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


class ChDBSessionAdapter(ClientAdapter):
    """Adapter for chdb session (embedded ClickHouse)."""

    def __init__(self, session_path: Optional[str] = None):
        """Initialize chdb session.

        Args:
            session_path: Path to .chdb file for persistent storage (MergeTree only)
                         If None, uses default path
        """
        from chdb import session as chs

        self._session = chs.Session(session_path or "aaiclick.chdb")
        self._session_path = session_path

    async def query(self, sql: str) -> QueryResult:
        """Execute query using chdb session.

        Uses DataFrame format for reliable conversion to tuples.
        """
        # Use DataFrame format for reliable conversion
        result = self._session.query(sql, "DataFrame")

        # Convert DataFrame to list of tuples (compatible with clickhouse-connect)
        # result is a pandas DataFrame
        result_rows = [tuple(row) for row in result.itertuples(index=False, name=None)]

        return QueryResult(result_rows)

    async def command(self, sql: str) -> None:
        """Execute command using chdb session."""
        # Commands don't return results, just execute
        self._session.query(sql)

    async def close(self) -> None:
        """Close chdb session."""
        # chdb session cleanup happens automatically
        pass


async def create_client(backend: Optional[str] = None) -> ClientAdapter:
    """Create a client adapter based on backend selection.

    Args:
        backend: Either 'chdb' or 'clickhouse-connect'.
                If None, uses environment variable AAICLICK_BACKEND or defaults to clickhouse-connect

    Returns:
        ClientAdapter instance
    """
    backend = backend or os.getenv("AAICLICK_BACKEND", "clickhouse-connect")

    if backend == "chdb":
        # Use chdb session for embedded/local workloads
        session_path = os.getenv("CHDB_SESSION_PATH")
        return ChDBSessionAdapter(session_path)

    elif backend == "clickhouse-connect":
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
        raise ValueError(f"Unknown backend: {backend}. Use 'chdb' or 'clickhouse-connect'")
