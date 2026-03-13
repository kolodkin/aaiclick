"""
aaiclick.data.client - ClickHouse client factory dispatch.

Single entry point that lazy-imports the appropriate client getter
based on the AAICLICK_CH_URL connection string.
"""

from __future__ import annotations

from typing import Union

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
