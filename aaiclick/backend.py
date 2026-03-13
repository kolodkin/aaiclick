"""
aaiclick.backend - Backend detection from connection URLs.

Two independent connection strings control what aaiclick connects to:

- AAICLICK_SQL_URL: SQLAlchemy async URL for orchestration state
    Default: sqlite+aiosqlite:///<home>/.aaiclick/local.db
    Example: postgresql+asyncpg://user:pass@host:5432/aaiclick

- AAICLICK_CH_URL: ClickHouse connection URL for data operations
    Default: chdb:///<home>/.aaiclick/chdb_data
    Example: clickhouse://user:pass@host:8123/default

Helper functions detect the backend type from the URL scheme.
"""

import os
from pathlib import Path


def get_sql_url() -> str:
    """Return the async SQL URL for orchestration state."""
    url = os.getenv("AAICLICK_SQL_URL")
    if url:
        return url
    db_path = str(Path.home() / ".aaiclick" / "local.db")
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite+aiosqlite:///{db_path}"


def get_ch_url() -> str:
    """Return the ClickHouse connection URL for data operations."""
    url = os.getenv("AAICLICK_CH_URL")
    if url:
        return url
    chdb_path = str(Path.home() / ".aaiclick" / "chdb_data")
    return f"chdb://{chdb_path}"


def is_sqlite() -> bool:
    """True when orchestration uses SQLite."""
    return get_sql_url().startswith("sqlite")


def is_chdb() -> bool:
    """True when data operations use embedded chdb."""
    return get_ch_url().startswith("chdb://")
