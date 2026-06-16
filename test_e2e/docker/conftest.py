"""Pytest configuration for the Docker-runner end-to-end suite.

This conftest registers the ``docker_e2e`` marker (so ``--strict-markers``
keeps passing when this directory is the test target) and skips the suite
unless a docker daemon is reachable. Workflows opt in by passing the
``test_e2e/docker/`` path to pytest explicitly; the default
``pyproject.toml`` ``testpaths`` setting ignores this directory."""

from __future__ import annotations

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
    publish_user_repo,
    sql_worker_setup,
)

_SAMPLE_JOB = Path(__file__).parent.parent / "fixtures" / "sample_job"


@pytest.fixture(scope="session")
def docker_e2e_user_repo(tmp_path_factory: pytest.TempPathFactory) -> tuple[str, str, Path]:
    """Publish the shared ``sample_job`` fixture into the CI git daemon.

    See ``aaiclick.testing.publish_user_repo`` — the daemon is workflow
    infrastructure started by ``_docker-e2e-reusable.yaml``."""
    return publish_user_repo(tmp_path_factory, _SAMPLE_JOB)


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
            subprocess.run([docker_bin, "info"], capture_output=True, timeout=10, check=False)
            daemon_ok = subprocess.run([docker_bin, "version"], capture_output=True, timeout=10).returncode == 0
        except (subprocess.TimeoutExpired, OSError):
            daemon_ok = False

    if daemon_ok:
        return

    skipper = pytest.mark.skip(reason="docker daemon not reachable")
    for item in items:
        if "docker_e2e" in item.keywords:
            item.add_marker(skipper)
