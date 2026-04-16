"""
aaiclick.backend - Backend detection from connection URLs.

Two independent connection strings control what aaiclick connects to:

- AAICLICK_SQL_URL: SQLAlchemy async URL for orchestration state
    Default: sqlite+aiosqlite:///<root>/local.db
    Example: postgresql+asyncpg://user:pass@host:5432/aaiclick

- AAICLICK_CH_URL: ClickHouse connection URL for data operations
    Default: chdb:///<root>/chdb_data
    Example: clickhouse://user:pass@host:8123/default

All local-mode paths derive from a single root directory:

- AAICLICK_LOCAL_ROOT: Base directory for local state (default: ~/.aaiclick)

Helper functions detect the backend type from the URL scheme.
"""

import os
from pathlib import Path
from urllib.parse import urlparse


def get_root() -> Path:
    """Return the base directory for all local-mode state.

    Reads AAICLICK_LOCAL_ROOT env var, defaulting to ``~/.aaiclick``.
    All local paths (SQLite DB, chdb data, logs) derive from this.
    """
    return Path(os.getenv("AAICLICK_LOCAL_ROOT", Path.home() / ".aaiclick"))


def get_sql_url() -> str:
    """Return the async SQL URL for orchestration state."""
    url = os.getenv("AAICLICK_SQL_URL")
    if url:
        return url
    db_path = str(get_root() / "local.db")
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite+aiosqlite:///{db_path}"


def get_ch_url() -> str:
    """Return the ClickHouse connection URL for data operations."""
    url = os.getenv("AAICLICK_CH_URL")
    if url:
        return url
    chdb_path = str(get_root() / "chdb_data")
    return f"chdb://{chdb_path}"


def is_sqlite() -> bool:
    """True when orchestration uses SQLite."""
    return get_sql_url().startswith("sqlite")


def is_chdb() -> bool:
    """True when data operations use embedded chdb."""
    return get_ch_url().startswith("chdb://")


def is_postgres() -> bool:
    """True when orchestration uses PostgreSQL."""
    return get_sql_url().startswith("postgresql")


def is_local() -> bool:
    """True when running in local mode (chdb + SQLite).

    Local mode runs everything in a single process to avoid chdb
    file-lock conflicts between multiple OS processes.
    """
    return is_chdb() and is_sqlite()


def parse_ch_url() -> dict:
    """Parse AAICLICK_CH_URL into clickhouse-connect connection parameters.

    Returns dict with keys: host, port, username, password, database.
    Only meaningful for non-chdb URLs (clickhouse://user:pass@host:port/db).
    """
    parsed = urlparse(get_ch_url())
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 8123,
        "username": parsed.username or "default",
        "password": parsed.password or "",
        "database": parsed.path.lstrip("/") or "default",
    }
