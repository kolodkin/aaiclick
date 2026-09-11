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
import os
import shutil
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

SHOTS = Path(__file__).resolve().parents[2] / "test-results" / "shots"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def base_url(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """Start the FastAPI server and yield its base URL.

    Uses the default chdb + SQLite backend, rooted at a per-session temp dir
    via ``AAICLICK_LOCAL_ROOT`` so the suite never reads or writes the
    developer's ``~/.aaiclick``. That keeps assertions about *which* jobs
    exist honest — a previous run's rows would otherwise still be in the
    list — and makes the run immune to a half-initialised local install
    (a ``setup_done`` marker beside an empty ``local.db`` skips bootstrap
    and every query then fails with "no such table").

    The server process is killed after the session.
    """
    port = _free_port()
    root = tmp_path_factory.mktemp("aaiclick-root")
    # Server stderr goes to a file, not a PIPE: nothing reads the pipe during
    # the run, so a chatty failure fills the 64 KB buffer and blocks the
    # server's event loop inside logging — turning a clean error into a
    # mystery hang on the next navigation.
    log_path = root / "server.log"
    log_file = log_path.open("wb")
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
        stderr=log_file,
        env={**os.environ, "AAICLICK_LOCAL_ROOT": str(root)},
    )

    url = f"http://127.0.0.1:{port}"

    # Poll until the server accepts connections. A fresh root means first
    # boot also runs setup() — schema creation plus a chdb cold start — so the
    # window is generous.
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            pytest.fail(f"Server exited early (code={proc.returncode}):\n{log_path.read_text(errors='replace')}")
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.5)
            s.close()
            break
        except OSError:
            time.sleep(0.25)
    else:
        proc.kill()
        proc.wait()
        pytest.fail(f"Server did not come up in time:\n{log_path.read_text(errors='replace')}")

    yield url

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    log_file.close()


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
