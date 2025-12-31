"""
aaiclick.client - Global ClickHouse client.

This module provides a simple global client for ClickHouse connections.
Connection parameters default to environment variables.

When USE_CHDB=1 is set, uses local chdb (clickhouse-local) instead of
connecting to a remote ClickHouse server.
"""

import json
import os
from typing import Any, List, Tuple

# Global client instance holder (list with single element)
_client = [None]


def _use_chdb() -> bool:
    """Check if chdb (local clickhouse) should be used."""
    return os.getenv("USE_CHDB", "0") == "1"


class ChdbQueryResult:
    """Wrapper around chdb query result to provide clickhouse-connect compatible interface."""

    def __init__(self, chdb_result: Any):
        self._raw = chdb_result
        self._parsed = None

    def _parse(self) -> dict:
        """Parse the JSON result from chdb."""
        if self._parsed is None:
            data = self._raw.data()
            if isinstance(data, bytes):
                data = data.decode("utf-8")
            self._parsed = json.loads(data) if data else {"data": [], "meta": []}
        return self._parsed

    @property
    def result_rows(self) -> List[Tuple]:
        """Get result rows as a list of tuples (clickhouse-connect compatible)."""
        parsed = self._parse()
        data = parsed.get("data", [])
        meta = parsed.get("meta", [])
        column_names = [col["name"] for col in meta]

        # Convert list of dicts to list of tuples in column order
        rows = []
        for row in data:
            rows.append(tuple(row.get(col) for col in column_names))
        return rows

    @property
    def column_names(self) -> List[str]:
        """Get column names."""
        parsed = self._parse()
        return [col["name"] for col in parsed.get("meta", [])]


class ChdbClientWrapper:
    """Wrapper around chdb Session to provide a compatible interface."""

    def __init__(self, session: Any):
        self._session = session

    async def query(self, query: str, *args, **kwargs) -> ChdbQueryResult:
        """Execute a query and return results."""
        result = self._session.query(query, "JSON")
        return ChdbQueryResult(result)

    async def command(self, query: str, *args, **kwargs) -> Any:
        """Execute a command (DDL/DML statement)."""
        return self._session.query(query)

    async def raw_query(self, query: str, *args, **kwargs) -> ChdbQueryResult:
        """Execute a raw query (sync under the hood for chdb)."""
        result = self._session.query(query, "JSON")
        return ChdbQueryResult(result)


async def get_client() -> Any:
    """
    Get the global ClickHouse client instance.

    If USE_CHDB=1 environment variable is set, uses local chdb (clickhouse-local)
    with data stored in .chdb directory.

    Otherwise, automatically connects using environment variables:
    - CLICKHOUSE_HOST (default: "localhost")
    - CLICKHOUSE_PORT (default: 8123)
    - CLICKHOUSE_USER (default: None)
    - CLICKHOUSE_PASSWORD (default: None)
    - CLICKHOUSE_DATABASE (default: "default")

    Returns:
        ClickHouse async client or chdb session wrapper
    """
    if _client[0] is None:
        if _use_chdb():
            from chdb import session as chs

            data_dir = os.getenv("CHDB_DATA_DIR", ".chdb")
            sess = chs.Session(data_dir)
            _client[0] = ChdbClientWrapper(sess)
        else:
            from clickhouse_connect import get_async_client

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


def reset_client() -> None:
    """Reset the global client (useful for testing)."""
    _client[0] = None
