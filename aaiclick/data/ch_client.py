"""
aaiclick.data.client - ClickHouse client factory dispatch.

Single entry point that lazy-imports the appropriate client getter
based on the AAICLICK_CH_URL connection string.
"""

from __future__ import annotations

from typing import Union
from urllib.parse import urlparse

from aaiclick.backend import is_chdb

# Type alias — concrete types are conditionally available,
# string literals deferred by `from __future__ import annotations`.
ChClient = Union["ChdbClient", "AsyncClient"]


async def create_ch_client() -> ChClient:
    """Create a ClickHouse client from AAICLICK_CH_URL."""
    if is_chdb():
        from .chdb_client import create_chdb_client

        return create_chdb_client()

    from .clickhouse_client import create_clickhouse_client

    return await create_clickhouse_client()


def create_sync_client(connection_string: str) -> object:
    """Create a sync ClickHouse client from a connection string.

    Supports:
    - chdb:///path/to/data → ChdbSyncClient
    - clickhouse://user:pass@host:port/db → clickhouse-connect sync client
    """
    if connection_string.startswith("chdb://"):
        from .chdb_client import create_chdb_sync_client

        return create_chdb_sync_client(connection_string)

    from clickhouse_connect import get_client

    parsed = urlparse(connection_string)
    return get_client(
        host=parsed.hostname or "localhost",
        port=parsed.port or 8123,
        username=parsed.username or "default",
        password=parsed.password or "",
        database=parsed.path.lstrip("/") or "default",
    )
