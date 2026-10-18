"""Scaffold placeholder credentials and the startup warning that flags them.

The compose file and helm chart ship *working* credentials so a first
``docker compose up`` / ``helm install`` succeeds with no edits. Every one of
them carries the ``change-me`` marker, which is the whole contract between the
templates and this check: a value still carrying it was never replaced, so the
server says so on every startup. The JWT signing secret is the exception:
it alone forges admin tokens, so the scaffolds swap its placeholder for a
fresh random value on write, and the server refuses to start with it.

The user-facing replacement list lives in ``docs/user_guide/deployment.md``.
"""

from __future__ import annotations

import logging
import os
import secrets

logger = logging.getLogger(__name__)

PLACEHOLDER_MARKER = "change-me"
JWT_SECRET_PLACEHOLDER = f"{PLACEHOLDER_MARKER}-jwt-secret-at-least-32-bytes-long"

CREDENTIAL_ENV_VARS = (
    "AAICLICK_JWT_SECRET",
    "AAICLICK_ADMIN_PASSWORD",
    "AAICLICK_SQL_URL",
    "AAICLICK_CH_URL",
)


def placeholder_env_vars() -> list[str]:
    """Credential env vars whose value still carries the scaffold marker."""
    return [name for name in CREDENTIAL_ENV_VARS if PLACEHOLDER_MARKER in (os.getenv(name) or "")]


def warn_if_placeholder_credentials() -> None:
    """Log one WARNING naming every credential left at its scaffold default."""
    names = placeholder_env_vars()
    if names:
        logger.warning(
            "scaffold placeholder credentials still in use: %s — replace them before "
            "exposing this deployment (docs/user_guide/deployment.md)",
            ", ".join(names),
        )


def fill_jwt_secret(text: str) -> str:
    """Replace the template's JWT secret placeholder with a fresh random secret."""
    return text.replace(JWT_SECRET_PLACEHOLDER, secrets.token_urlsafe(32))
