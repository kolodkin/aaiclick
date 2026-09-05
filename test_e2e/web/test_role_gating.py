"""Playwright coverage for the admin-only gating of mutating UI controls.

Viewers get the controls disabled with an explanatory tooltip rather than
hidden — see ``src/components/AdminButton.tsx`` and the role matrix in
``docs/designs/auth.md``.

Both the role probe (``/auth/me``) and the list call are stubbed, so the
assertions are about the SPA's gating alone and hold in either backend mode:
local mode would otherwise always report the synthetic admin, and distributed
mode would 401 the stubbed session out to the login screen.

Run with::

    pytest test_e2e/web/test_role_gating.py -v -p no:cov
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from helpers import open_page

STATIC = Path(__file__).resolve().parents[2] / "aaiclick" / "server" / "static" / "index.html"

pytest.importorskip("playwright.sync_api")

_spa_built = pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")

_REGISTERED_JOB = {
    "id": "1",
    "name": "nightly_report",
    "entrypoint": "jobs.report.build",
    "enabled": True,
    "schedule": None,
    "next_run_at": None,
    "created_at": "2026-01-01T00:00:00Z",
}


def _stub_session(page, role: str) -> None:
    """Serve a fixed principal and one registered job, whatever the backend.

    ``admin`` maps to the superadmin flag: until the tenant switcher lands
    (tenant RBAC phase 3) the SPA's ``isAdmin`` gate reads ``me.superadmin``.
    """
    me = {
        "id": 1,
        "username": f"{role}_user",
        "superadmin": role == "admin",
        "tenants": [{"tenant_id": "4611686018427387904", "slug": "aaiclick", "name": "aaiclick", "role": role}],
    }
    page.route(
        "**/api/v0/auth/me",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(me),
        ),
    )
    page.route(
        "**/api/v0/registered-jobs*",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"items": [_REGISTERED_JOB], "total": 1}),
        ),
    )


def _open_registered(page, base_url: str, role: str) -> None:
    """Stub the session, then navigate via the suite's shared helper so this
    file picks up SPA-shell changes like the other web tests do."""
    _stub_session(page, role)
    open_page(page, f"{base_url}/?p=@registered")
    page.wait_for_selector("table")


@_spa_built
def test_viewer_sees_mutating_controls_disabled(page, base_url: str) -> None:
    _open_registered(page, base_url, "viewer")

    for name in ("+ Register new job", "Run", "Run…"):
        button = page.get_by_role("button", name=name, exact=True).first
        assert button.is_disabled(), f"{name!r} should be disabled for a viewer"
        assert "admin role" in (button.get_attribute("title") or ""), f"{name!r} needs an explanatory tooltip"

    toggle = page.locator("button.toggle").first
    assert toggle.is_disabled(), "the enabled/disabled toggle should be disabled for a viewer"


@_spa_built
def test_admin_sees_mutating_controls_enabled(page, base_url: str) -> None:
    """The gate must not leak onto admins — this is what would catch an
    inverted condition that disables the controls for everyone."""
    _open_registered(page, base_url, "admin")

    for name in ("+ Register new job", "Run", "Run…"):
        button = page.get_by_role("button", name=name, exact=True).first
        assert button.is_enabled(), f"{name!r} should be enabled for an admin"

    assert page.locator("button.toggle").first.is_enabled()
