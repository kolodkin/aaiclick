"""
aaiclick.backend - Backend detection from connection URLs.

See docs/designs/orchestration.md — "Deployment Modes", "Configuration".

Two independent connection strings control what aaiclick connects to:

- AAICLICK_SQL_URL: SQLAlchemy async URL for orchestration state
    Default: sqlite+aiosqlite:///<root>/local.db
    Example: postgresql+asyncpg://user:pass@host:5432/aaiclick

- AAICLICK_CH_URL: ClickHouse connection URL for data operations
    Default: chdb:///<root>/chdb_data
    Example: clickhouse://user:pass@host:8123/default

All local-mode paths derive from a single root directory:

- AAICLICK_LOCAL_ROOT: Base directory for local state (default: ~/.aaiclick)

- AAICLICK_S3_CREDENTIALS: Where a remote ClickHouse gets bucket credentials.
    Unset: public buckets only. ``server``: the server's own identity.

Helper functions detect the backend type from the URL scheme.
"""

import os
from pathlib import Path
from typing import Literal
from urllib.parse import unquote, urlsplit, urlunsplit

LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1", "[::1]"})
"""Loopback names, as a bind address (``::1``) or a ``Host`` header (``[::1]``)."""

S3_CREDENTIALS_SERVER = "server"
S3CredentialsMode = Literal["server"]


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


def is_distributed() -> bool:
    """True when orchestration uses PostgreSQL (multi-process backend).

    Used to gate features that require cross-process coordination — e.g.
    advisory locks for serializing concurrent inserts into shared CH tables.
    Local mode (chdb + SQLite) is single-process and needs no such gating.
    """
    return is_postgres()


def parse_ch_url() -> dict:
    """Parse AAICLICK_CH_URL into clickhouse-connect connection parameters.

    Returns dict with keys: host, port, username, password, database.
    Only meaningful for non-chdb URLs (clickhouse://user:pass@host:port/db).
    """
    parsed = urlsplit(get_ch_url())
    # Userinfo is percent-encoded like the SQL URL's, so a password holding
    # ``@`` / ``:`` / ``/`` can be written at all.
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 8123,
        "username": unquote(parsed.username or "") or "default",
        "password": unquote(parsed.password or ""),
        "database": parsed.path.lstrip("/") or "default",
    }


def get_s3_credentials() -> S3CredentialsMode | None:
    """Return the AAICLICK_S3_CREDENTIALS mode, or ``None`` when unset."""
    mode = os.getenv("AAICLICK_S3_CREDENTIALS")
    if not mode:
        return None
    if mode != S3_CREDENTIALS_SERVER:
        raise ValueError(f"AAICLICK_S3_CREDENTIALS must be unset or '{S3_CREDENTIALS_SERVER}', got {mode!r}")
    return S3_CREDENTIALS_SERVER


def s3_client_settings() -> dict[str, int]:
    """ClickHouse settings a remote connection sends with every query.

    A ClickHouse server refuses to use its own S3 identity (environment keys,
    IAM role) for user queries unless the session opts in, and the check runs
    on every statement that touches an S3 table — hence a connection default.
    """
    if get_s3_credentials() == S3_CREDENTIALS_SERVER:
        return {"s3_allow_server_credentials_in_user_queries": 1}
    return {}


def redact_url(url: str) -> str:
    """``url`` with its password, if any, replaced by ``***`` — safe to print or return."""
    parsed = urlsplit(url)
    if parsed.password is None:
        return url
    userinfo, host = parsed.netloc.rsplit("@", 1)
    username = userinfo.split(":", 1)[0]
    return urlunsplit(parsed._replace(netloc=f"{username}:***@{host}"))
