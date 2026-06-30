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

import asyncio
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pytest
from sqlmodel import col, select

from aaiclick.datetime_utils import utc_now
from aaiclick.orchestration.docker_config import effective_image_tag
from aaiclick.orchestration.execution.mp_worker import mp_worker_main_loop
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import JOB_COMPLETED, JOB_FAILED, TASK_COMPLETED, Job
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.runner_config import ImageBuild, KubernetesRunner, parse_runner_config


def _aaiclick(*args: str, cwd: Path) -> subprocess.CompletedProcess:
    """Run a ``python -m aaiclick`` CLI invocation from ``cwd`` (the user repo,
    so the entrypoint module is importable). Captures output for the log."""
    return subprocess.run(
        [sys.executable, "-m", "aaiclick", *args],
        check=True,
        capture_output=True,
        text=True,
        cwd=cwd,
    )


async def _wait_for_job(job_name: str, timeout: float = 600.0) -> Job:
    """Poll the most recent Job with this name until it reaches a terminal
    status. On timeout, dump per-task states so a stuck or failing task is
    diagnosable from the CI log."""
    deadline = utc_now() + timedelta(seconds=timeout)
    job = None
    while utc_now() < deadline:
        async with get_sql_session() as session:
            result = await session.execute(
                select(Job).where(Job.name == job_name).order_by(col(Job.id).desc()).limit(1)
            )
            job = result.scalar_one_or_none()
        if job is not None and job.status in (JOB_COMPLETED, JOB_FAILED):
            return job
        await asyncio.sleep(1.0)
    lines = [f"Job {job_name!r} did not complete within {timeout}s; job_status={getattr(job, 'status', None)}"]
    if job is not None:
        for t in await get_tasks_for_job(job.id):
            lines.append(f"  task entrypoint={t.entrypoint!r} status={t.status} attempt={t.attempt} error={t.error!r}")
    raise TimeoutError("\n".join(lines))


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
        "--runner",
        "kubernetes",
        "--git-remote",
        remote,
        cwd=worktree,
    )

    _aaiclick("run-job", job_name, "--git-sha", sha, cwd=worktree)

    worker_task = asyncio.create_task(
        mp_worker_main_loop(max_tasks=10, install_signal_handlers=False, max_empty_polls=10)
    )
    try:
        completed = await _wait_for_job(job_name)
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

    assert completed.status == JOB_COMPLETED, completed.error
    assert completed.runner is not None
    runner = parse_runner_config(completed.runner)
    assert isinstance(runner, KubernetesRunner)
    assert isinstance(runner.image, ImageBuild)
    tag = effective_image_tag(runner)
    assert tag is not None and tag.endswith(f":{sha}")
    assert runner.image.git_sha == sha

    tasks = await get_tasks_for_job(completed.id)
    entrypoints = [t.entrypoint for t in tasks]
    assert "sample_jobs.entry_task" in entrypoints
    assert "sample_jobs.compute_sum" in entrypoints
    non_terminal = [t for t in tasks if t.status != TASK_COMPLETED]
    assert not non_terminal, [(t.entrypoint, t.status, t.error) for t in non_terminal]

    # produce([10,20,30]) → double → compute_sum → (10+20+30)*2 = 120, read back
    # from ClickHouse — confirms Objects passed across Pods.
    summed = next(t for t in tasks if t.entrypoint == "sample_jobs.compute_sum")
    assert summed.result == {"native_value": {"total": 120}}, summed.result
