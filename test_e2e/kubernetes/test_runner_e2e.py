"""End-to-end smoke test for the Kubernetes runner.

Drives the full ``register-job`` → ``run-job`` → build → Pod → result path
against a real minikube cluster, a local registry, a test pypi serving the
wheel under test, and the CI ``git daemon`` (the ``kubernetes_e2e_user_repo``
fixture publishes the user repo into it). Both registration and submission go
through the ``python -m aaiclick`` CLI from the user-repo working tree, exactly
as an external user would.

Marked ``kubernetes_e2e`` so it opts out of the default test run; the workflow
passes ``test_e2e/kubernetes/`` with ``-m kubernetes_e2e``."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from job_wait import run_worker_until_done

from aaiclick.orchestration.docker_config import compute_image_tag
from aaiclick.orchestration.execution.kubernetes_worker import _pod_name
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import JOB_COMPLETED, TASK_COMPLETED
from aaiclick.orchestration.runner_config import ImageBuild, parse_image_source


def _aaiclick(*args: str, cwd: Path) -> subprocess.CompletedProcess:
    """Run a ``python -m aaiclick`` CLI invocation from ``cwd`` (the user repo,
    so the entrypoint module is importable). Echoes the captured output
    (visible under ``pytest -s``) so a silent submit failure is diagnosable,
    then raises on a non-zero exit."""
    proc = subprocess.run(
        [sys.executable, "-m", "aaiclick", *args],
        check=False,
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    print(
        f"$ aaiclick {' '.join(args)} [exit {proc.returncode}]\n--stdout--\n{proc.stdout}\n--stderr--\n{proc.stderr}",
        file=sys.stderr,
    )
    proc.check_returncode()
    return proc


@pytest.mark.kubernetes_e2e
async def test_kubernetes_runner_smoke(orch_ctx, kubernetes_e2e_user_repo):
    """Build the fixture image, run the entry task's chain as Pods, assert it
    completes and the Objects flowed through ClickHouse across Pods."""
    remote, sha, worktree = kubernetes_e2e_user_repo
    job_name = "k8s_e2e_smoke"

    _aaiclick(
        "register-job",
        "sample_jobs.entry_task",
        "--name",
        job_name,
        "--build",
        "--git-remote",
        remote,
        cwd=worktree,
    )

    _aaiclick("run-job", job_name, "--git-sha", sha, cwd=worktree)

    completed = await run_worker_until_done(job_name)

    assert completed.status == JOB_COMPLETED, completed.error

    tasks = await get_tasks_for_job(completed.id)
    # The image is a task property now: the entry task carries the build source.
    entry = next(t for t in tasks if t.entrypoint == "sample_jobs.entry_task")
    assert entry.image_source is not None
    source = parse_image_source(entry.image_source)
    assert isinstance(source, ImageBuild)
    assert source.git_sha == sha
    assert compute_image_tag(source.git_sha).endswith(f":{sha}")
    entrypoints = [t.entrypoint for t in tasks]
    assert "sample_jobs.entry_task" in entrypoints
    assert "sample_jobs.compute_sum" in entrypoints
    non_terminal = [t for t in tasks if t.status != TASK_COMPLETED]
    assert not non_terminal, [(t.entrypoint, t.status, t.error) for t in non_terminal]

    # produce([10,20,30]) → double → compute_sum → (10+20+30)*2 = 120, read back
    # from ClickHouse — confirms Objects passed across Pods.
    summed = next(t for t in tasks if t.entrypoint == "sample_jobs.compute_sum")
    assert summed.result == {"native_value": {"total": 120}}, summed.result


@pytest.mark.kubernetes_e2e
async def test_kubernetes_runner_shell_command_env(orch_ctx, tmp_path):
    """Run a shell command in a prebuilt image as a ``kubectl run`` Pod.

    The command exits 0 only if ``command_env`` arrived, which proves the
    per-attempt Secret path end to end; afterwards the Secret must be gone
    (``cleanup_argv`` deletes it with the Pod)."""
    job_name = "k8s_e2e_shell_command_env"

    _aaiclick(
        "register-job",
        "shell.placeholder",
        "--name",
        job_name,
        "--image",
        "python:3.12",
        cwd=tmp_path,
    )
    _aaiclick(
        "run-job",
        job_name,
        "--entry-type",
        "shell",
        "--command",
        """python -c "import os, sys; sys.exit(0 if os.environ.get('K') == 'v' else 3)" """,
        "--command-env",
        "K=v",
        cwd=tmp_path,
    )

    completed = await run_worker_until_done(job_name)

    assert completed.status == JOB_COMPLETED, completed.error
    tasks = await get_tasks_for_job(completed.id)
    assert len(tasks) == 1, [t.entrypoint for t in tasks]
    assert tasks[0].status == TASK_COMPLETED, tasks[0].error

    secret = _pod_name(tasks[0].id, tasks[0].run_epoch)
    probe = subprocess.run(["kubectl", "get", "secret", secret], capture_output=True, text=True, check=False)
    assert probe.returncode != 0 and "NotFound" in probe.stderr, probe
