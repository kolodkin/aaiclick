"""
aaiclick.orchestration.env - Environment variable configuration for orchestration.

Delegates to aaiclick.backend for the SQL URL, and centralizes parsing of
the orchestration-level env vars (preservation mode default).
"""

from __future__ import annotations

import os

from aaiclick.backend import get_sql_url

from .models import PreservationMode


def get_db_url() -> str:
    """Return the async SQL URL for orchestration.

    Delegates to backend.get_sql_url() which reads AAICLICK_SQL_URL.
    """
    return get_sql_url()


def get_default_preservation_mode() -> PreservationMode:
    """Read ``AAICLICK_DEFAULT_PRESERVATION_MODE`` (or return ``NONE``).

    Accepted values (case-insensitive): ``NONE``, ``FULL``.
    An unset env var yields ``PreservationMode.NONE``. An invalid value
    raises ``ValueError`` with the list of accepted keywords.
    """
    raw = os.environ.get("AAICLICK_DEFAULT_PRESERVATION_MODE")
    if raw is None or raw == "":
        return PreservationMode.NONE
    try:
        return PreservationMode(raw.upper())
    except ValueError:
        accepted = ", ".join(m.value for m in PreservationMode)
        raise ValueError(f"Invalid AAICLICK_DEFAULT_PRESERVATION_MODE={raw!r}. Accepted values: {accepted}") from None
