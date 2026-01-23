"""PostgreSQL database connection pool for orchestration backend.

This module re-exports pool functions from context.py for backward compatibility.
"""

from __future__ import annotations

import asyncpg

from .context import _get_postgres_pool, _reset_postgres_pool


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
    return await _get_postgres_pool()


async def reset_postgres_pool():
    """Reset the PostgreSQL connection pool.

    Closes the existing pool and sets it to None, forcing
    a new pool to be created on next get_postgres_pool() call.

    Used primarily for test cleanup to ensure test isolation.
    """
    await _reset_postgres_pool()
