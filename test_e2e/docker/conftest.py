"""Pytest configuration for the Docker-runner end-to-end suite.

This conftest registers the ``docker_e2e`` marker (so ``--strict-markers``
keeps passing when this directory is the test target) and skips the suite
unless a docker daemon is reachable. Workflows opt in by passing the
``test_e2e/docker/`` path to pytest explicitly; the default
``pyproject.toml`` ``testpaths`` setting ignores this directory."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from aaiclick.orchestration.docker_config import get_registry

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

_FIXTURES = Path(__file__).parent.parent / "fixtures"
_SAMPLE_JOB = _FIXTURES / "sample_job"
_JVM_TASK = _FIXTURES / "jvm_task"
_JAVA_SDK = Path(__file__).parent.parent.parent / "java"
_JVM_TASK_IMAGE = "aaiclick-e2e-jvm-task:local"


@pytest.fixture(scope="session")
def docker_e2e_user_repo(tmp_path_factory: pytest.TempPathFactory) -> tuple[str, str, Path]:
    """Publish the shared ``sample_job`` fixture into the CI git daemon.

    See ``aaiclick.testing.publish_user_repo`` — the daemon is workflow
    infrastructure started by ``_docker-e2e-reusable.yaml``."""
    return publish_user_repo(tmp_path_factory, _SAMPLE_JOB)


@pytest.fixture(scope="session")
def jvm_task_image(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Build the ``jvm_task`` fixture image and return its tag.

    The build context pairs the checkout's ``java/`` SDK with the fixture's
    user project (see the fixture Dockerfile), so the shim under test is the
    one at this commit. With ``AAICLICK_REGISTRY`` set the image is pushed
    there under the registry prefix, matching how a prebuilt image reaches
    the runner in production (the worker pulls before ``docker run``)."""
    context = tmp_path_factory.mktemp("jvm_task_ctx")
    shutil.copytree(_JAVA_SDK, context / "sdk", ignore=shutil.ignore_patterns("target", ".flattened-pom.xml"))
    shutil.copytree(_JVM_TASK, context / "app")

    registry = get_registry()
    tag = f"{registry}/{_JVM_TASK_IMAGE}" if registry else _JVM_TASK_IMAGE
    subprocess.run(
        ["docker", "build", "-f", str(context / "app" / "Dockerfile"), "-t", tag, str(context)],
        check=True,
        stdout=sys.stderr,
    )
    if registry:
        subprocess.run(["docker", "push", tag], check=True, stdout=sys.stderr)
    return tag


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
