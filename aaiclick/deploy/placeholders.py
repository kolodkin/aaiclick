"""Scaffold placeholder credentials: filled on write, refused at startup.

The compose and helm templates spell every credential as a ``change-me-...``
token. ``init`` replaces each distinct token with one random value, so a first
``docker compose up`` / ``helm install`` works with no edits and nothing
public. A value still carrying the marker was copied from the raw template,
and the server refuses to start with it.

The user-facing list lives in ``docs/user_guide/deployment.md``.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import NamedTuple

from aaiclick.auth.security import generate_secret

PLACEHOLDER_MARKER = "change-me"
ADMIN_PASSWORD_PLACEHOLDER = f"{PLACEHOLDER_MARKER}-admin-password"
_PLACEHOLDER_RE = re.compile(rf"{PLACEHOLDER_MARKER}-[A-Za-z0-9-]+")

CREDENTIAL_ENV_VARS = (
    "AAICLICK_JWT_SECRET",
    "AAICLICK_ADMIN_PASSWORD",
    "AAICLICK_SQL_URL",
    "AAICLICK_CH_URL",
)


class Scaffold(NamedTuple):
    """What ``init`` wrote, and the generated admin password to show once."""

    path: Path
    admin_password: str


def fill_placeholders(text: str, values: dict[str, str]) -> str:
    """Replace each ``change-me-...`` token with a random secret.

    ``values`` maps token → secret; pass the same dict for every file of one
    scaffold so a token repeated across files (a password and the URL that
    dials it) gets the same value."""
    return _PLACEHOLDER_RE.sub(lambda m: values.setdefault(m.group(), generate_secret()), text)


def placeholder_env_vars() -> list[str]:
    """Credential env vars whose value still carries the scaffold marker."""
    return [name for name in CREDENTIAL_ENV_VARS if PLACEHOLDER_MARKER in (os.getenv(name) or "")]


def require_no_placeholder_credentials() -> None:
    """Refuse to start while any credential is still a public template value."""
    names = placeholder_env_vars()
    if names:
        raise RuntimeError(
            f"scaffold placeholder credentials still in use: {', '.join(names)}. "
            "Run `compose init` / `k8s init` to generate them, or set your own "
            "(docs/user_guide/deployment.md)"
        )
