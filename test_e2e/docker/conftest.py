"""Pytest configuration for the Docker-runner end-to-end suite.

This conftest registers the ``docker_e2e`` marker (so ``--strict-markers``
keeps passing when this directory is the test target) and skips the suite
unless a docker daemon is reachable. Workflows opt in by passing the
``test_e2e/docker/`` path to pytest explicitly; the default
``pyproject.toml`` ``testpaths`` setting ignores this directory."""

from __future__ import annotations

import shutil
import subprocess

import pytest

# Re-use the `orch_ctx` family from the package's testing plugin instead
# of duplicating fixtures here.
pytest_plugins = ["aaiclick.testing"]


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "docker_e2e: end-to-end docker runner tests requiring a real daemon",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
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
