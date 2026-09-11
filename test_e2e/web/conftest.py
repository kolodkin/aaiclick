"""Pytest configuration for the browser-based web smoke e2e suite.

Launches the FastAPI server on a free port, and yields a ``base_url``
string fixture plus Playwright fixtures. Playwright is optional — tests
guard with ``pytest.importorskip`` and skip automatically when the
package is absent.

``shot`` saves full-page screenshots under ``test-results/shots/`` at the
repo root, where the ``/screenshots`` skill looks.

The suite sits under ``test_e2e/web/`` which is excluded from the
default ``pytest`` testpaths; it only runs when the path is passed
explicitly (or in a dedicated CI workflow)."""

from __future__ import annotations

import itertools
import shutil
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from aaiclick.backend import is_local

SEED = Path(__file__).with_name("seed.py")
SHOTS = Path(__file__).resolve().parents[2] / "test-results" / "shots"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def base_url() -> Iterator[str]:
    """Start the FastAPI server and yield its base URL.

    Uses the default chdb + SQLite backend (AAICLICK_LOCAL_ROOT unchanged).
    The server process is killed after the session.
    """
    # Viewer fixtures need ClickHouse tables, and chdb's session is a
    # per-process singleton holding the data-directory lock — so the seed runs
    # in its own process and must finish before the server takes the lock.
    if is_local():
        subprocess.run([sys.executable, str(SEED), "viewer"], check=True)

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
        stderr=subprocess.PIPE,
    )

    url = f"http://127.0.0.1:{port}"

    # Poll until the server accepts connections (up to 30 s — chdb's
    # cold start can take a few seconds on slow CI containers).
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            stderr = proc.stderr.read().decode("utf-8", "replace") if proc.stderr else ""
            pytest.fail(f"Server exited early (code={proc.returncode}):\n{stderr}")
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.5)
            s.close()
            break
        except OSError:
            time.sleep(0.25)
    else:
        proc.kill()
        proc.wait()
        pytest.fail("Server did not come up within 30 s")

    yield url

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    if proc.stderr is not None:
        proc.stderr.close()


@pytest.fixture(scope="session")
def playwright_sync() -> Iterator[Any]:
    """Yield a synchronous Playwright instance (session-scoped)."""
    playwright_mod = pytest.importorskip("playwright.sync_api")
    with playwright_mod.sync_playwright() as pw:
        yield pw


@pytest.fixture(scope="session")
def browser(playwright_sync: Any) -> Iterator[Any]:
    """Yield a Chromium browser (session-scoped, headless)."""
    br = playwright_sync.chromium.launch(headless=True)
    yield br
    br.close()


@pytest.fixture()
def page(browser: Any) -> Iterator[Any]:
    """Yield a fresh Playwright page for each test."""
    pg = browser.new_page()
    yield pg
    pg.close()


@pytest.fixture(scope="session", autouse=True)
def _shots_dir() -> None:
    """Start each run with an empty ``test-results/shots/`` so the directory
    only ever holds this run's screenshots."""
    shutil.rmtree(SHOTS, ignore_errors=True)
    SHOTS.mkdir(parents=True, exist_ok=True)


@pytest.fixture(scope="session")
def _shot_counter() -> Iterator[int]:
    return itertools.count(1)


@pytest.fixture()
def shot(page: Any, _shot_counter: Iterator[int]) -> Callable[[str], Path]:
    """Save a curated full-page screenshot to ``test-results/shots/NN-<name>.png``.

    The NN prefix is a run-wide counter, so filenames sort in the order the
    screenshots were taken."""

    def _shot(name: str) -> Path:
        path = SHOTS / f"{next(_shot_counter):02d}-{name}.png"
        page.screenshot(path=str(path), full_page=True)
        return path

    return _shot
