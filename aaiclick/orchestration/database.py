"""
aaiclick.orchestration.database - PostgreSQL connection pool management.

This module provides a global asyncpg connection pool for orchestration backend,
following the same pattern as ClickHouse connection pool in data_context.py.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg


# Global connection pool (similar to ClickHouse _pool pattern)
_postgres_pool: list[Optional[asyncpg.Pool]] = [None]


async def get_postgres_pool() -> asyncpg.Pool:
    """
    Get or create the global PostgreSQL connection pool.

    Connection parameters are read from environment variables:
    - POSTGRES_HOST (default: "localhost")
    - POSTGRES_PORT (default: 5432)
    - POSTGRES_USER (default: "aaiclick")
    - POSTGRES_PASSWORD (default: "secret")
    - POSTGRES_DB (default: "aaiclick")

    Returns:
        asyncpg.Pool: Shared connection pool for PostgreSQL
    """
    if _postgres_pool[0] is None:
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = int(os.getenv("POSTGRES_PORT", "5432"))
        user = os.getenv("POSTGRES_USER", "aaiclick")
        password = os.getenv("POSTGRES_PASSWORD", "secret")
        database = os.getenv("POSTGRES_DB", "aaiclick")

        _postgres_pool[0] = await asyncpg.create_pool(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            min_size=2,
            max_size=10,
        )

    return _postgres_pool[0]


@asynccontextmanager
async def get_postgres_connection():
    """
    Async context manager to acquire a connection from the global pool.

    Yields:
        asyncpg.Connection: Database connection from the pool

    Example:
        >>> async with get_postgres_connection() as conn:
        ...     result = await conn.fetch("SELECT * FROM jobs")
    """
    pool = await get_postgres_pool()
    async with pool.acquire() as conn:
        yield conn
