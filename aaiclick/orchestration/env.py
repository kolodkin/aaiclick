"""
aaiclick.orchestration.env - Environment variable configuration for orchestration.

Delegates to aaiclick.backend for the SQL URL.
"""

from __future__ import annotations

from aaiclick.backend import get_sql_url


def get_db_url() -> str:
    """Return the async SQL URL for orchestration.

    Delegates to backend.get_sql_url() which reads AAICLICK_SQL_URL.
    """
    return get_sql_url()
