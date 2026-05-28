"""Pytest configuration for the browser-based web smoke e2e suite.

Registers the ``web_e2e`` marker (keeping ``--strict-markers`` happy),
launches the FastAPI server on a free port, and yields a ``base_url``
string fixture plus Playwright fixtures. Playwright is optional — tests
guard with ``pytest.importorskip`` and skip automatically when the
package is absent.

The suite sits under ``test_e2e/web/`` which is excluded from the
default ``pytest`` testpaths; it only runs when the path is passed
explicitly (or in a dedicated CI workflow)."""

from __future__ import annotations

import socket
import subprocess
import sys
import time

import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "web_e2e: browser smoke tests requiring the SPA build and Playwright",
    )


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def base_url() -> str:  # type: ignore[return]
    """Start the FastAPI server and yield its base URL.

    Uses the default chdb + SQLite backend (AAICLICK_LOCAL_ROOT unchanged).
    The server process is killed after the session.
    """
    port = _free_port()
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "aaiclick.server.app:app",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    url = f"http://127.0.0.1:{port}"

    # Poll until the server accepts connections (up to 10 s).
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.5)
            s.close()
            break
        except OSError:
            time.sleep(0.25)
    else:
        proc.kill()
        proc.wait()
        pytest.fail("Server did not come up within 10 s")

    yield url

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@pytest.fixture(scope="session")
def playwright_sync():  # type: ignore[return]
    """Yield a synchronous Playwright instance (session-scoped)."""
    playwright_mod = pytest.importorskip("playwright.sync_api")
    with playwright_mod.sync_playwright() as pw:
        yield pw


@pytest.fixture(scope="session")
def browser(playwright_sync):  # type: ignore[return]
    """Yield a Chromium browser (session-scoped, headless)."""
    br = playwright_sync.chromium.launch(headless=True)
    yield br
    br.close()


@pytest.fixture()
def page(browser):  # type: ignore[return]
    """Yield a fresh Playwright page for each test."""
    pg = browser.new_page()
    yield pg
    pg.close()
