"""End-to-end smoke of the scaffolded compose stack.

Covers the release-gate contract: the stack the user gets from
``compose init`` comes up healthy, migrations applied, and a job
round-trips through the server container's CLI to the worker container."""

from __future__ import annotations

import json
import subprocess
import time
import urllib.request
from pathlib import Path

import pytest

pytestmark = pytest.mark.compose_e2e

JOB_TIMEOUT_S = 300
EXEC_TIMEOUT_S = 60

# Must be a shipped ``@task`` callable: ``register-job`` resolves the
# entrypoint via ``import_callback`` at registration time even for shell-entry
# runs, and ``--image`` is no workaround (it forces a prebuilt DockerRunner
# instead of the subprocess mode this smoke test exercises).
_SMOKE_ENTRYPOINT = "aaiclick.orchestration.examples.orchestration_basic.simple_arithmetic"


def _compose_exec(compose_dir: Path, service: str, *args: str) -> str:
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", service, *args],
        cwd=compose_dir,
        capture_output=True,
        text=True,
        timeout=EXEC_TIMEOUT_S,
    )
    assert result.returncode == 0, f"exec {service} {args} failed:\n{result.stdout}\n{result.stderr}"
    return result.stdout


def _job_by_name(compose_dir: Path, name: str) -> dict | None:
    out = _compose_exec(compose_dir, "server", "python", "-m", "aaiclick", "job", "list", "--json")
    items = json.loads(out)["items"]
    matches = [j for j in items if j["name"] == name]
    return matches[0] if matches else None


def test_server_healthy(server_url):
    # stdlib on purpose: the CI venv installs only the test+distributed
    # extras, which do not ship an HTTP client library.
    with urllib.request.urlopen(f"{server_url}/health", timeout=10) as response:
        assert response.status == 200


def test_worker_registered(compose_dir):
    out = _compose_exec(compose_dir, "server", "python", "-m", "aaiclick", "execution-worker", "list", "--json")
    items = json.loads(out)["items"]
    assert items, "no execution worker registered — worker service did not come up"


def test_shell_job_round_trip(compose_dir):
    _compose_exec(
        compose_dir,
        "server",
        "python", "-m", "aaiclick", "register-job", _SMOKE_ENTRYPOINT,
        "--name", "compose-smoke",
        "--runner", "subprocess",
    )
    _compose_exec(
        compose_dir,
        "server",
        "python", "-m", "aaiclick", "run-job", "compose-smoke",
        "--entry-type", "shell",
        "--command", "python -c 'print(42)'",
    )

    deadline = time.monotonic() + JOB_TIMEOUT_S
    while time.monotonic() < deadline:
        job = _job_by_name(compose_dir, "compose-smoke")
        if job and job["status"] == "COMPLETED":
            return
        if job and job["status"] in ("FAILED", "CANCELLED"):
            pytest.fail(f"compose-smoke ended {job['status']}: {job}")
        time.sleep(5)
    pytest.fail(f"compose-smoke did not complete within {JOB_TIMEOUT_S}s")
