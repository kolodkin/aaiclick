"""Shared e2e fixtures, and present so pytest puts ``test_e2e/`` on ``sys.path``.

The suites under it have no ``__init__.py``, so without this file each suite
directory is importable but their shared modules (``job_wait``) are not.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from aaiclick.orchestration.docker_config import get_registry

_JVM_TASK = Path(__file__).parent / "fixtures" / "jvm_task"
_JAVA_SDK = Path(__file__).parent.parent / "java"
_JVM_TASK_IMAGE = "aaiclick-e2e-jvm-task:local"


@pytest.fixture(scope="session")
def jvm_task_image(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Build the ``jvm_task`` fixture image and return its tag.

    The build context pairs the checkout's ``java/`` SDK with the fixture's
    user project (see the fixture Dockerfile), so the shim under test is the
    one at this commit. With ``AAICLICK_REGISTRY`` set the image is pushed
    there under the registry prefix, matching how a prebuilt image reaches
    the runner in production (the worker pulls before running it)."""
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
