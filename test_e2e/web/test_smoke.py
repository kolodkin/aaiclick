"""Playwright golden-path smoke test for the operator UI.

Requires:
- SPA build present at ``aaiclick/server/static/index.html``
  (produced by ``npm run build``).
- ``playwright`` Python package installed
  (skipped automatically with ``pytest.importorskip`` if absent).

Run with::

    pytest test_e2e/web/test_smoke.py -v -p no:cov

The ``base_url`` and ``page`` fixtures are provided by
``test_e2e/web/conftest.py``, which launches a real uvicorn server
against the default chdb + SQLite backend on a free port.

This suite is excluded from the default ``pytest`` testpaths and only
runs when the path is passed explicitly or in a dedicated CI workflow."""

from __future__ import annotations

from pathlib import Path

import pytest

# Guard 1: the SPA build must exist.
STATIC = Path(__file__).resolve().parents[2] / "aaiclick" / "server" / "static" / "index.html"

# Guard 2: Playwright must be installed.  pytest.importorskip records a SKIP
# reason that appears in the pytest output — do not raise ImportError here.
pytest.importorskip("playwright.sync_api")


@pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")
def test_home_loads(page, base_url: str) -> None:
    """Root URL renders the SPA shell (header + content area)."""
    page.goto(f"{base_url}/")
    page.wait_for_selector("#root")
    # The header prompt input is present.
    page.wait_for_selector("#prompt")


@pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")
def test_jobs_view_loads(page, base_url: str) -> None:
    """Navigating to /?p=@jobs shows the jobs view."""
    page.goto(f"{base_url}/?p=@jobs")
    page.wait_for_selector("#root")
    # The prompt input is populated with the value from the URL.
    prompt_val = page.input_value("#prompt")
    assert prompt_val == "@jobs"


@pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")
def test_prompt_updates_url(page, base_url: str) -> None:
    """Typing into the prompt input updates the URL query parameter."""
    page.goto(f"{base_url}/")
    page.wait_for_selector("#prompt")
    page.fill("#prompt", "@registered")
    # After typing, the URL should contain ?p=@registered.
    page.wait_for_url(lambda url: "p=%40registered" in url or "p=@registered" in url)
