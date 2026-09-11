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
from typing import Any, BinaryIO

import pytest

from aaiclick.backend import is_local

SHOTS = Path(__file__).resolve().parents[2] / "test-results" / "shots"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _launch(root: Path, name: str, args: list[str], env: dict[str, str]) -> tuple[subprocess.Popen, BinaryIO]:
    """Start one ``python -m`` process with stderr captured to ``<root>/<name>.log``.

    A file, not a PIPE: nothing drains a pipe during the run, so a chatty
    failure fills its 64 KB buffer and blocks the process's event loop inside
    logging — a clean error becomes a hang on the next navigation.
    """
    log_file = (root / f"{name}.log").open("wb")
    proc = subprocess.Popen([sys.executable, "-m", *args], stdout=subprocess.DEVNULL, stderr=log_file, env=env)
    return proc, log_file


def _stop(proc: subprocess.Popen, log_file: BinaryIO) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    log_file.close()


@pytest.fixture(scope="session")
def base_url(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """Start the server — plus both workers in distributed mode — and yield its URL.

    Rooted at a per-session temp dir via ``AAICLICK_LOCAL_ROOT`` so the suite
    never reads or writes the developer's ``~/.aaiclick``: assertions about
    *which* jobs exist are only honest if a previous run's rows are not still
    in the list.

    Local mode runs the execution and background workers inside the server
    (``local_runtime``). Distributed mode does not — they are separate
    processes in production — so they are launched here too. That is what
    lets a job a test creates actually run, and the same test body then holds
    in both modes.
    """
    port = _free_port()
    root = tmp_path_factory.mktemp("aaiclick-root")
    # The test process must point at the same root: tests create jobs
    # in-process (helpers.submit_job), and in local mode that is a SQLite file
    # under this directory — a different root is a different database.
    mp = pytest.MonkeyPatch()
    mp.setenv("AAICLICK_LOCAL_ROOT", str(root))
    env = dict(os.environ)
    server_args = ["uvicorn", "aaiclick.server.app:app", "--port", str(port), "--log-level", "warning"]
    procs = [_launch(root, "server", server_args, env)]
    if not is_local():
        procs.append(_launch(root, "execution-worker", ["aaiclick", "execution-worker", "start"], env))
        # Job rows reach COMPLETED on this worker's poll; the default 10 s
        # would dominate every test that waits for one.
        procs.append(_launch(root, "background", ["aaiclick", "background", "start", "--poll-interval", "1"], env))
    server, _ = procs[0]
    server_log = root / "server.log"

    url = f"http://127.0.0.1:{port}"

    # Poll until the server accepts connections. A fresh root means first
    # boot also runs setup() — schema creation plus a chdb cold start — so the
    # window is generous.
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if server.poll() is not None:
            pytest.fail(f"Server exited early (code={server.returncode}):\n{server_log.read_text(errors='replace')}")
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.5)
            s.close()
            break
        except OSError:
            time.sleep(0.25)
    else:
        for proc, log_file in procs:
            _stop(proc, log_file)
        pytest.fail(f"Server did not come up in time:\n{server_log.read_text(errors='replace')}")

    yield url

    for proc, log_file in reversed(procs):
        _stop(proc, log_file)
    mp.undo()


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
