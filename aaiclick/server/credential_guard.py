"""Refuse to start while a credential is still a public template value.

See docs/designs/deployment.md — "Startup guard".

The compose ``.env.template`` spells every credential as a ``change-me-...``
token; ``env.init.sh`` replaces each with a random value. A value still
carrying the marker was copied from the raw template by hand.
"""

from __future__ import annotations

import os

PLACEHOLDER_MARKER = "change-me"

CREDENTIAL_ENV_VARS = (
    "AAICLICK_JWT_SECRET",
    "AAICLICK_ADMIN_PASSWORD",
    "AAICLICK_SQL_URL",
    "AAICLICK_CH_URL",
)


def require_no_placeholder_credentials() -> None:
    """Refuse to start while any credential is still a public template value."""
    names = [name for name in CREDENTIAL_ENV_VARS if PLACEHOLDER_MARKER in (os.getenv(name) or "")]
    if names:
        raise RuntimeError(
            f"template placeholder credentials still in use: {', '.join(names)}. "
            "Run `sh env.init.sh` or replace them in .env (docs/user_guide/deployment.md)"
        )
