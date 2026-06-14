"""Pytest configuration for the Docker-runner end-to-end suite.

This conftest registers the ``docker_e2e`` marker (so ``--strict-markers``
keeps passing when this directory is the test target) and skips the suite
unless a docker daemon is reachable. Workflows opt in by passing the
``test_e2e/docker/`` path to pytest explicitly; the default
``pyproject.toml`` ``testpaths`` setting ignores this directory."""

from __future__ import annotations

import shutil
import socket
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

# Re-import the fixture symbols from the project's testing plugin so pytest
# registers them at this conftest. ``pytest_plugins`` is the obvious mechanism
# but pytest forbids it in non-top-level conftests; importing the fixture
# functions has the same effect because pytest discovers fixtures by walking
# the conftest module's namespace.
from aaiclick.testing import (  # noqa: F401 - re-exported as pytest fixtures
    ch_worker_setup,
    orch_ctx,
    orch_ctx_no_ch,
    orch_module_ctx,
    orch_module_ctx_no_ch,
    sql_worker_setup,
)


def _free_port() -> int:
    """Pick an unused loopback port for the throwaway git daemon."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_port(host: str, port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"git daemon did not start on {host}:{port} within {timeout}s")


@pytest.fixture(scope="session")
def docker_e2e_user_repo(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[str, str, Path]]:
    """Serve the ``sample_job`` fixture as a standalone git repo over a
    local ``git daemon`` — a real remote, no monorepo reuse, no network.

    The docker-runner build clones ``--git-remote`` host-side (the build
    task runs on the runner, not in a container), so the remote only needs
    runner-local reachability: ``git daemon`` bundled with stock git is
    enough — no service container, no auth, no ``host.docker.internal``.

    Yields ``(remote_url, commit_sha, worktree)``. ``worktree`` is the
    user-repo checkout the host CLI runs from (its root is on ``sys.path``
    so ``register-job`` resolves the entrypoint exactly as an external user
    standing in their project would); ``remote_url`` is what the build
    clones at ``commit_sha``."""
    fixture = Path(__file__).parent / "fixtures" / "sample_job"
    base = tmp_path_factory.mktemp("gitsrv")
    worktree = tmp_path_factory.mktemp("user_repo")
    shutil.copytree(fixture, worktree, dirs_exist_ok=True)

    def git(cwd: Path, *args: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True
        )
        return result.stdout.strip()

    git(worktree, "init", "-q", "-b", "main")
    git(worktree, "add", "-A")
    # ``-c …`` overrides keep the commit independent of the runner's global
    # git config (identity, and any ambient ``commit.gpgsign`` that would
    # otherwise demand a signing key the CI runner doesn't have).
    git(
        worktree,
        "-c",
        "user.email=e2e@example.com",
        "-c",
        "user.name=e2e",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "-qm",
        "fixture",
    )
    sha = git(worktree, "rev-parse", "HEAD")

    bare = base / "sample_job.git"
    git(worktree, "clone", "-q", "--bare", str(worktree), str(bare))
    # The build fetches a raw SHA over the smart transport; upload-pack
    # rejects that unless the serving repo opts in.
    git(bare, "config", "uploadpack.allowAnySHA1InWant", "true")

    port = _free_port()
    daemon = subprocess.Popen(
        [
            "git",
            "daemon",
            "--reuseaddr",
            f"--base-path={base}",
            "--export-all",
            "--listen=127.0.0.1",
            f"--port={port}",
        ]
    )
    try:
        _wait_for_port("127.0.0.1", port)
        yield f"git://127.0.0.1:{port}/sample_job.git", sha, worktree
    finally:
        daemon.terminate()
        try:
            daemon.wait(timeout=10)
        except subprocess.TimeoutExpired:
            daemon.kill()


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "docker_e2e: end-to-end docker runner tests requiring a real daemon",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip docker_e2e tests when a docker daemon isn't available."""
    if not items:
        return

    docker_bin = shutil.which("docker")
    daemon_ok = False
    if docker_bin is not None:
        try:
            subprocess.run(
                [docker_bin, "info"],
                capture_output=True,
                timeout=10,
                check=False,
            )
            daemon_ok = (
                subprocess.run(
                    [docker_bin, "version"],
                    capture_output=True,
                    timeout=10,
                ).returncode
                == 0
            )
        except (subprocess.TimeoutExpired, OSError):
            daemon_ok = False

    if daemon_ok:
        return

    skip_reason = "docker daemon not reachable"
    skipper = pytest.mark.skip(reason=skip_reason)
    for item in items:
        if "docker_e2e" in item.keywords:
            item.add_marker(skipper)
