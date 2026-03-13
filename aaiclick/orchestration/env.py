"""
aaiclick.orchestration.env - Environment variable configuration for orchestration.

This module centralizes database URL construction with sensible defaults.
Supports both PostgreSQL (distributed) and SQLite (local) backends.
"""

import os
from pathlib import Path

from aaiclick.backend import is_local


def get_db_url() -> str:
    """Build async database URL based on active backend.

    Returns:
        Async SQLAlchemy database URL string.
        - Local: sqlite+aiosqlite:/// file path
        - Distributed: postgresql+asyncpg:// connection string
    """
    if is_local():
        db_path = os.getenv(
            "AAICLICK_SQLITE_PATH",
            str(Path.home() / ".aaiclick" / "local.db"),
        )
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite+aiosqlite:///{db_path}"

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "aaiclick")
    password = os.getenv("POSTGRES_PASSWORD", "secret")
    database = os.getenv("POSTGRES_DB", "aaiclick")
    return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"


def get_pg_url() -> str:
    """Build PostgreSQL async connection URL from environment variables.

    Alias for get_db_url() — kept for backward compatibility.
    """
    return get_db_url()
