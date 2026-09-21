"""Scaffold placeholder credentials and the startup warning that flags them.

The compose file and helm chart ship *working* credentials so a first
``docker compose up`` / ``helm install`` succeeds with no edits. Every one of
them carries the ``change-me`` marker, which is the whole contract between the
templates and this check: a value still carrying it was never replaced, so the
server says so on every startup.

The user-facing replacement list lives in ``docs/user_guide/deployment.md``.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

PLACEHOLDER_MARKER = "change-me"

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
