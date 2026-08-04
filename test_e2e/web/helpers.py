"""Shared browser helpers for the web e2e suite."""

from __future__ import annotations

import os

# Seeded admin credentials. In distributed mode the server enforces auth, so
# tests log in before the SPA shell renders; the server seeds this admin on
# startup from the same env vars. In local mode auth is off and login is skipped.
ADMIN_USER = os.getenv("AAICLICK_ADMIN_USERNAME", "admin")
ADMIN_PASS = os.getenv("AAICLICK_ADMIN_PASSWORD", "admin")


def login_if_needed(page) -> None:
    """Authenticate through the SPA login form when auth is enforced.

    Waits for either the login form (auth on) or the prompt input (auth off or
    already authenticated), then logs in only if the form is present.
    """
    page.wait_for_selector("#login-username, #prompt")
    if page.query_selector("#login-username"):
        page.fill("#login-username", ADMIN_USER)
        page.fill("#login-password", ADMIN_PASS)
        page.click("#login-submit")
        page.wait_for_selector("#prompt")


def open_page(page, url: str) -> None:
    """Navigate, wait for the SPA shell, and authenticate if the server asks."""
    page.goto(url)
    page.wait_for_selector("#root")
    login_if_needed(page)
