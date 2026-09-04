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
ENV_PUBLIC_URL = "AAICLICK_PUBLIC_URL"

ENV_OIDC_ISSUER = "AAICLICK_OIDC_ISSUER"
ENV_OIDC_CLIENT_ID = "AAICLICK_OIDC_CLIENT_ID"
ENV_OIDC_CLIENT_SECRET = "AAICLICK_OIDC_CLIENT_SECRET"
ENV_OIDC_SCOPES = "AAICLICK_OIDC_SCOPES"
ENV_OIDC_USERNAME_CLAIM = "AAICLICK_OIDC_USERNAME_CLAIM"
ENV_OIDC_AUTO_PROVISION = "AAICLICK_OIDC_AUTO_PROVISION"
ENV_OIDC_LABEL = "AAICLICK_OIDC_LABEL"

DEFAULT_ACCESS_TTL = 1800
DEFAULT_REFRESH_TTL = 1209600
DEFAULT_ADMIN_USERNAME = "superadmin"
DEFAULT_OIDC_SCOPES = "openid profile email"
DEFAULT_OIDC_USERNAME_CLAIM = "preferred_username"
DEFAULT_OIDC_LABEL = "SSO"
OIDC_STATE_TTL = 600
"""Seconds an SSO login may take between ``/auth/oidc/start`` and the callback."""


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


def public_url() -> str | None:
    """Browser-facing origin, without a trailing slash."""
    value = os.getenv(ENV_PUBLIC_URL)
    return value.rstrip("/") if value else None


def require_public_url() -> str:
    value = public_url()
    if value is None:
        raise RuntimeError(f"{ENV_PUBLIC_URL} must be set")
    return value


class OidcSettings(NamedTuple):
    issuer: str
    client_id: str
    client_secret: str | None
    scopes: str
    username_claim: str
    auto_provision: bool
    label: str


def oidc_settings() -> OidcSettings | None:
    """SSO configuration, or ``None`` when the issuer / client id are unset."""
    issuer = os.getenv(ENV_OIDC_ISSUER)
    client_id = os.getenv(ENV_OIDC_CLIENT_ID)
    if not issuer or not client_id:
        return None
    return OidcSettings(
        issuer=issuer.rstrip("/"),
        client_id=client_id,
        client_secret=os.getenv(ENV_OIDC_CLIENT_SECRET) or None,
        scopes=os.getenv(ENV_OIDC_SCOPES) or DEFAULT_OIDC_SCOPES,
        username_claim=os.getenv(ENV_OIDC_USERNAME_CLAIM) or DEFAULT_OIDC_USERNAME_CLAIM,
        auto_provision=os.getenv(ENV_OIDC_AUTO_PROVISION, "1") not in ("0", "false", "no", ""),
        label=os.getenv(ENV_OIDC_LABEL) or DEFAULT_OIDC_LABEL,
    )
