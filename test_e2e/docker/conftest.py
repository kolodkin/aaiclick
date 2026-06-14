"""Pytest configuration for the Docker-runner end-to-end suite.

This conftest registers the ``docker_e2e`` marker (so ``--strict-markers``
keeps passing when this directory is the test target) and skips the suite
unless a docker daemon is reachable. Workflows opt in by passing the
``test_e2e/docker/`` path to pytest explicitly; the default
``pyproject.toml`` ``testpaths`` setting ignores this directory."""

from __future__ import annotations

import os
import shutil
import subprocess
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


@pytest.fixture(scope="session")
def docker_e2e_user_repo(tmp_path_factory: pytest.TempPathFactory) -> tuple[str, str, Path]:
    """Publish the ``sample_job`` fixture as a standalone git repo into the
    CI ``git daemon`` and return ``(remote_url, commit_sha, worktree)``.

    The daemon is workflow infrastructure — a fixed-port loopback service
    started by the ``Start git daemon`` step in ``_docker-e2e-reusable.yaml``,
    alongside the pypiserver and registry. This fixture only *publishes* a bare
    repo into the daemon's base-path (mirroring the wheel-upload step), exactly
    as a user's repo would already live on a remote git host; the build clones
    it host-side, so no container reachability / auth is needed.

    ``worktree`` is the user-repo checkout the host CLI runs from (its root is
    on ``sys.path`` so ``register-job`` resolves the entrypoint exactly as an
    external user standing in their project would); ``remote_url`` is what the
    build clones at ``commit_sha``."""
    base = os.environ.get("AAICLICK_E2E_GIT_DAEMON_BASE")
    port = os.environ.get("AAICLICK_E2E_GIT_DAEMON_PORT")
    if not base or not port:
        pytest.skip(
            "git daemon not configured; the docker e2e is workflow-driven — see the "
            "'Start git daemon' step in .github/workflows/_docker-e2e-reusable.yaml"
        )

    fixture = Path(__file__).parent / "fixtures" / "sample_job"
    worktree = tmp_path_factory.mktemp("user_repo")
    shutil.copytree(fixture, worktree, dirs_exist_ok=True)

    def git(cwd: Path, *args: str) -> str:
        result = subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True)
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

    # Publish a bare repo into the running daemon's base-path; the daemon serves
    # repos created after startup (upload-pack is spawned per connection).
    bare = Path(base) / "sample_job.git"
    git(worktree, "clone", "-q", "--bare", str(worktree), str(bare))
    # The build fetches a raw SHA over the smart transport; upload-pack rejects
    # that unless the serving repo opts in.
    git(bare, "config", "uploadpack.allowAnySHA1InWant", "true")

    return f"git://127.0.0.1:{port}/sample_job.git", sha, worktree


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
