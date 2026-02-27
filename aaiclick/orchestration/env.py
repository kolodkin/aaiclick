"""
aaiclick.orchestration.env - Environment variable configuration for orchestration.

This module centralizes PostgreSQL environment variable reading with sensible defaults.
"""

import os


def get_pg_url() -> str:
    """Build PostgreSQL async connection URL from environment variables.

    Returns:
        Async SQLAlchemy database URL string
    """
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "aaiclick")
    password = os.getenv("POSTGRES_PASSWORD", "secret")
    database = os.getenv("POSTGRES_DB", "aaiclick")
    return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"
