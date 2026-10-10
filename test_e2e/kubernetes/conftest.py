"""Pytest configuration for the Kubernetes-runner end-to-end suite.

Workflow-driven: ``test_e2e/kubernetes/`` is outside the default
``pyproject.toml`` ``testpaths``; ``_kubernetes-e2e-reusable.yaml`` passes the
path explicitly. Skips unless a cluster is reachable (``kubectl cluster-info``)."""

from __future__ import annotations

import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

from aaiclick.testing import (  # noqa: F401 - re-exported as pytest fixtures
    UserRepo,
    orch_ctx,
    orch_ctx_no_ch,
    orch_module_ctx,
    orch_module_ctx_no_ch,
    publish_user_repo,
    worker_databases,
)


@pytest.fixture(autouse=True, scope="session")
def _worker_databases() -> Iterator[None]:
    """Give this xdist worker its own CH and SQL databases for the session."""
    with worker_databases():
        yield


_SAMPLE_JOB = Path(__file__).parent.parent / "fixtures" / "sample_job"


@pytest.fixture(scope="session")
def kubernetes_e2e_user_repo(tmp_path_factory: pytest.TempPathFactory) -> UserRepo:
    """Publish the shared ``sample_job`` fixture into the CI git daemon.

    See ``aaiclick.testing.publish_user_repo`` — the daemon is workflow
    infrastructure started by ``_kubernetes-e2e-reusable.yaml``."""
    return publish_user_repo(tmp_path_factory, _SAMPLE_JOB)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "kubernetes_e2e: end-to-end kubernetes runner tests requiring a real cluster",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip kubernetes_e2e tests when no cluster is reachable."""
    if not items:
        return

    cluster_ok = False
    if shutil.which("kubectl"):
        try:
            cluster_ok = subprocess.run(["kubectl", "cluster-info"], capture_output=True, timeout=15).returncode == 0
        except (subprocess.TimeoutExpired, OSError):
            cluster_ok = False

    if cluster_ok:
        return

    skipper = pytest.mark.skip(reason="kubernetes cluster not reachable")
    for item in items:
        if "kubernetes_e2e" in item.keywords:
            item.add_marker(skipper)
