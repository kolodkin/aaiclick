"""
aaiclick.orchestration.env - Environment variable configuration for orchestration.

Delegates to aaiclick.backend for the SQL URL, and centralizes parsing of
the orchestration-level env vars (preservation mode default).
"""

from __future__ import annotations

import os
from typing import cast, get_args

from aaiclick.backend import get_sql_url

from .models import PRESERVATION_NONE, PreservationMode

ENV_JOB_WAIT_TIMEOUT = "AAICLICK_JOB_WAIT_TIMEOUT"
# An hour, not minutes: the timeout bounds how long a caller *watches* a job,
# never how long the job may run, so a short budget reports a healthy
# long-running job as a failure instead of protecting anything.
DEFAULT_JOB_WAIT_TIMEOUT = 3600.0


def job_wait_timeout() -> float:
    """Seconds to wait for a job to reach a terminal status.

    Read per call rather than frozen into a module constant, so a test or an
    in-process caller can change it without re-importing. CI sets
    ``AAICLICK_JOB_WAIT_TIMEOUT`` lower to stay inside its own job cap.
    """
    return float(os.getenv(ENV_JOB_WAIT_TIMEOUT, DEFAULT_JOB_WAIT_TIMEOUT))


def get_db_url() -> str:
    """Return the async SQL URL for orchestration.

    Delegates to backend.get_sql_url() which reads AAICLICK_SQL_URL.
    """
    return get_sql_url()


def get_default_preservation_mode() -> PreservationMode:
    """Read ``AAICLICK_DEFAULT_PRESERVATION_MODE`` (or return ``NONE``).

    Accepted values (case-insensitive): ``NONE``, ``FULL``.
    An unset env var yields ``"NONE"``. An invalid value
    raises ``ValueError`` with the list of accepted keywords.
    """
    raw = os.environ.get("AAICLICK_DEFAULT_PRESERVATION_MODE")
    if raw is None or raw == "":
        return PRESERVATION_NONE
    upper = raw.upper()
    if upper not in get_args(PreservationMode):
        accepted = ", ".join(get_args(PreservationMode))
        raise ValueError(f"Invalid AAICLICK_DEFAULT_PRESERVATION_MODE={raw!r}. Accepted values: {accepted}")
    return cast(PreservationMode, upper)
