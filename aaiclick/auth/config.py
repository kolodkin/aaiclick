"""Environment configuration for auth. Inline env reads, mirroring backend.py.

Auth enforcement is a *deployment-mode* convention, not a flag: it is always
disabled in local mode (single-process chdb + SQLite dev) and always enforced
in distributed mode. See ``docs/designs/auth.md``.
"""

from __future__ import annotations

import os
from typing import NamedTuple

from ..backend import is_local

ENV_SECRET = "AAICLICK_JWT_SECRET"
ENV_ACCESS_TTL = "AAICLICK_JWT_ACCESS_TTL"
ENV_REFRESH_TTL = "AAICLICK_JWT_REFRESH_TTL"
ENV_ADMIN_USERNAME = "AAICLICK_ADMIN_USERNAME"
ENV_ADMIN_PASSWORD = "AAICLICK_ADMIN_PASSWORD"

DEFAULT_ACCESS_TTL = 1800
DEFAULT_REFRESH_TTL = 1209600
DEFAULT_ADMIN_USERNAME = "superadmin"


class AdminSeed(NamedTuple):
    username: str
    password: str


def auth_enabled() -> bool:
    """Enforced in distributed mode, disabled in local mode (hardcoded convention)."""
    return not is_local()


def jwt_secret() -> str | None:
    return os.getenv(ENV_SECRET) or None


def require_jwt_secret() -> str:
    """Return the signing secret, raising if distributed mode left it unset."""
    secret = jwt_secret()
    if secret is None:
        raise RuntimeError(f"distributed mode requires {ENV_SECRET} to be set")
    return secret


def access_ttl() -> int:
    return int(os.getenv(ENV_ACCESS_TTL, DEFAULT_ACCESS_TTL))


def refresh_ttl() -> int:
    return int(os.getenv(ENV_REFRESH_TTL, DEFAULT_REFRESH_TTL))


def admin_seed() -> AdminSeed | None:
    """First-startup superadmin seed; the username defaults to ``superadmin``.

    The password has no default — without ``AAICLICK_ADMIN_PASSWORD`` nothing
    is seeded, so a deployment never ships a well-known credential.
    """
    username = os.getenv(ENV_ADMIN_USERNAME) or DEFAULT_ADMIN_USERNAME
    password = os.getenv(ENV_ADMIN_PASSWORD)
    if password:
        return AdminSeed(username, password)
    return None
