"""End-to-end smoke test for the Docker runner.

Drives the full ``register-job`` → ``run-job`` → build → run → result
path against a real docker daemon, a real local registry, a real test
pypi serving the wheel under test, and a real git remote (the CI
``git daemon`` service the ``docker_e2e_user_repo`` fixture publishes the
user repo into). Both the registration and the job submission go through
the ``python -m aaiclick`` CLI as a real user would, run from the
user-repo working tree so the entrypoint resolves from it — exactly as an
external user standing in their project.

Marked ``docker_e2e`` so it opts out of the default test run; both the
nightly workflow and the publish-time release gate pass
``test_e2e/docker/`` to pytest with ``-m docker_e2e`` to pick it up.

The "user repo" is decoupled from the aaiclick checkout: aaiclick-under-
test arrives via the test pypi wheel, so the served repo only carries the
user's project. Like the registry and pypiserver, the git daemon is a
fixed-port loopback service stood up by the workflow, so nothing leaves
the runner."""

from __future__ import annotations

import asyncio
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pytest
from sqlmodel import col, select

from aaiclick.datetime_utils import utc_now
from aaiclick.orchestration.execution.mp_worker import mp_worker_main_loop
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import JOB_COMPLETED, JOB_FAILED, TASK_COMPLETED, Job
from aaiclick.orchestration.orch_context import get_sql_session


def _aaiclick(*args: str, cwd: Path) -> subprocess.CompletedProcess:
    """Run a `python -m aaiclick` CLI invocation from ``cwd`` (the user
    repo, so the entrypoint module is on ``sys.path``). Captures output
    so failures surface in the pytest log."""
    return subprocess.run(
        [sys.executable, "-m", "aaiclick", *args],
        check=True,
        capture_output=True,
        text=True,
        cwd=cwd,
    )


async def _wait_for_job(job_name: str, timeout: float = 600.0) -> Job:
    """Poll the most recent Job with this name until it reaches a
    terminal status, or fail."""
    deadline = utc_now() + timedelta(seconds=timeout)
    while utc_now() < deadline:
        async with get_sql_session() as session:
            result = await session.execute(
                select(Job).where(Job.name == job_name).order_by(col(Job.id).desc()).limit(1)
            )
            job = result.scalar_one_or_none()
        if job is not None and job.status in (JOB_COMPLETED, JOB_FAILED):
            return job
        await asyncio.sleep(1.0)
    raise TimeoutError(f"Job {job_name!r} did not complete within {timeout}s")


@pytest.mark.docker_e2e
async def test_docker_runner_smoke(orch_ctx, docker_e2e_user_repo):
    """Build the fixture image from the standalone user repo, run the
    entry task in a container, assert it completes."""
    remote, sha, worktree = docker_e2e_user_repo
    job_name = "docker_e2e_smoke"

    _aaiclick(
        "register-job",
        "sample_jobs.entry_task",
        "--name",
        job_name,
        "--runner",
        "docker",
        "--git-remote",
        remote,
        cwd=worktree,
    )

    _aaiclick("run-job", job_name, "--git-sha", sha, cwd=worktree)

    # Drive the worker loop in the background while we poll for completion.
    worker_task = asyncio.create_task(
        mp_worker_main_loop(
            max_tasks=10,
            install_signal_handlers=False,
            max_empty_polls=10,
        )
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
    assert completed.image_tag and completed.image_tag.endswith(f":{sha}")
    assert completed.git_sha == sha

    tasks = await get_tasks_for_job(completed.id)
    entrypoints = [t.entrypoint for t in tasks]
    assert "sample_jobs.entry_task" in entrypoints
    assert "sample_jobs.produce" in entrypoints
    assert "sample_jobs.double" in entrypoints
    assert "sample_jobs.compute_sum" in entrypoints
    non_terminal = [t for t in tasks if t.status != TASK_COMPLETED]
    assert not non_terminal, [(t.entrypoint, t.status, t.error) for t in non_terminal]

    # The chain is produce([10, 20, 30]) → double → compute_sum, so the
    # final scalar is (10+20+30) * 2 = 120. Reading it confirms Objects
    # passed correctly across containers via ClickHouse. Native return
    # values are wrapped as ``{"native_value": ...}`` in Task.result.
    summed = next(t for t in tasks if t.entrypoint == "sample_jobs.compute_sum")
    assert summed.result == {"native_value": {"total": 120}}, summed.result
