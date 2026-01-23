"""PostgreSQL database connection pool for orchestration backend."""

from __future__ import annotations

import os
from typing import Optional

import asyncpg


_pool: Optional[asyncpg.Pool] = None


async def get_postgres_pool() -> asyncpg.Pool:
    """Get or create the global PostgreSQL connection pool.

    Pool is initialized on first call using environment variables:
    - POSTGRES_HOST (default: "localhost")
    - POSTGRES_PORT (default: 5432)
    - POSTGRES_USER (default: "aaiclick")
    - POSTGRES_PASSWORD (default: "secret")
    - POSTGRES_DB (default: "aaiclick")

    Returns:
        Global asyncpg connection pool
    """
    global _pool

    if _pool is None:
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = int(os.getenv("POSTGRES_PORT", "5432"))
        user = os.getenv("POSTGRES_USER", "aaiclick")
        password = os.getenv("POSTGRES_PASSWORD", "secret")
        database = os.getenv("POSTGRES_DB", "aaiclick")

        _pool = await asyncpg.create_pool(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            min_size=2,
            max_size=10,
        )

    return _pool


async def get_postgres_connection():
    """Acquire a connection from the global pool.

    Use as async context manager:
        async with get_postgres_connection() as conn:
            result = await conn.fetch("SELECT * FROM jobs")

    Returns:
        Connection context manager from pool
    """
    pool = await get_postgres_pool()
    return pool.acquire()
