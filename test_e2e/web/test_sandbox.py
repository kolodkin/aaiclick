"""Playwright coverage for the sandbox page: submit a file, see its row, and
the inline error for a bad file. The server fixture points ``AAICLICK_SANDBOX``
at an empty bare repo, so a submission really commits; in local mode the
in-server background worker then fails the row (a build source needs the
distributed backends), which is why the status assertion admits ``failed``.

Run with::

    pytest test_e2e/web/test_sandbox.py -v -p no:cov
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from helpers import open_page

STATIC = Path(__file__).resolve().parents[2] / "aaiclick" / "server" / "static" / "index.html"

pytest.importorskip("playwright.sync_api")

_spa_built = pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")


@_spa_built
def test_sandbox_command_hidden_when_disabled(page, base_url: str) -> None:
    page.route(
        "**/api/v0/sandbox/config",
        lambda route: route.fulfill(
            status=200, content_type="application/json", body=json.dumps({"enabled": False, "remote": None})
        ),
    )
    open_page(page, base_url)
    page.wait_for_selector(".cmd-list")
    assert page.locator(".cmd code", has_text="@sandbox").count() == 0


@_spa_built
def test_sandbox_command_shown_when_enabled(page, base_url: str) -> None:
    open_page(page, base_url)
    page.locator(".cmd code", has_text="@sandbox").wait_for()


@_spa_built
def test_submit_shows_row_with_sha(page, base_url: str, shot) -> None:
    open_page(page, f"{base_url}/?p=%40sandbox")
    page.fill("#sandbox-name", "hello")
    page.click("#sandbox-submit")
    row = page.locator("tr[data-sandbox-id]").first
    row.wait_for()
    assert re.fullmatch(r"[0-9a-f]{7}", row.locator("td.sha").inner_text())
    assert row.locator("td.status").inner_text() in {"pending", "failed"}
    shot("sandbox-submitted")


@_spa_built
def test_syntax_error_renders_inline(page, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=%40sandbox")
    page.fill("#sandbox-name", "bad")
    page.fill("#sandbox-source", "def a(:")
    page.click("#sandbox-submit")
    page.wait_for_selector(".err")
    assert "line 1" in page.locator(".err").inner_text()
