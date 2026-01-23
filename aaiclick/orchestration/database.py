"""PostgreSQL database connection pool for orchestration backend."""

from __future__ import annotations

import os
from typing import Optional

import asyncpg


_pool: list[Optional[asyncpg.Pool]] = [None]


async def get_postgres_pool() -> asyncpg.Pool:
    """Get or create the PostgreSQL connection pool.

    Pool is initialized on first call using environment variables:
    - POSTGRES_HOST (default: "localhost")
    - POSTGRES_PORT (default: 5432)
    - POSTGRES_USER (default: "aaiclick")
    - POSTGRES_PASSWORD (default: "secret")
    - POSTGRES_DB (default: "aaiclick")

    Returns:
        asyncpg connection pool

    Example:
        pool = await get_postgres_pool()
        async with pool.acquire() as conn:
            result = await conn.fetch("SELECT * FROM jobs")
    """
    if _pool[0] is None:
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = int(os.getenv("POSTGRES_PORT", "5432"))
        user = os.getenv("POSTGRES_USER", "aaiclick")
        password = os.getenv("POSTGRES_PASSWORD", "secret")
        database = os.getenv("POSTGRES_DB", "aaiclick")

        _pool[0] = await asyncpg.create_pool(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            min_size=2,
            max_size=10,
        )

    return _pool[0]


async def reset_postgres_pool():
    """Reset the PostgreSQL connection pool.

    Closes the existing pool and sets it to None, forcing
    a new pool to be created on next get_postgres_pool() call.

    Used primarily for test cleanup to ensure test isolation.
    """
    if _pool[0] is not None:
        await _pool[0].close()
        _pool[0] = None
