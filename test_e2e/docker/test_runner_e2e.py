"""End-to-end smoke test for the Docker runner.

Drives the full ``register-job`` → ``run-job`` → build → run → result
path against a real docker daemon, a real local registry, and a real
test pypi serving the wheel under test. Both the registration and the
job submission go through the ``python -m aaiclick`` CLI as a real
user would, so this exercises the CLI plumbing alongside the runtime.

Marked ``docker_e2e`` so it opts out of the default test run; both the
nightly workflow and the publish-time release gate pass
``test_e2e/docker/`` to pytest with ``-m docker_e2e`` to pick it up.

The test reuses the aaiclick checkout itself as its "user repo" via a
``file://`` remote — the build runs on the CI host which already has
the checkout, so going through GitHub would be a pointless network
round-trip with extra flake surface."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from datetime import datetime, timedelta

import pytest
from sqlmodel import col, select

from aaiclick.orchestration.execution.mp_worker import mp_worker_main_loop
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import JOB_COMPLETED, JOB_FAILED, TASK_COMPLETED, Job
from aaiclick.orchestration.orch_context import get_sql_session


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"{name} not set; e2e suite is workflow-driven")
    return value


def _aaiclick(*args: str) -> subprocess.CompletedProcess:
    """Run a `python -m aaiclick` CLI invocation in the same env as the
    test process. Captures output so failures surface in the pytest log."""
    return subprocess.run(
        [sys.executable, "-m", "aaiclick", *args],
        check=True,
        capture_output=True,
        text=True,
    )


async def _wait_for_job(job_name: str, timeout: float = 600.0) -> Job:
    """Poll the most recent Job with this name until it reaches a
    terminal status, or fail."""
    deadline = datetime.utcnow() + timedelta(seconds=timeout)
    while datetime.utcnow() < deadline:
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
async def test_docker_runner_smoke(orch_ctx):
    """Build the fixture image from the current checkout, run the entry
    task in a container, assert it completes."""
    workspace = _required_env("GITHUB_WORKSPACE")
    sha = _required_env("GITHUB_SHA")
    job_name = "docker_e2e_smoke"

    _aaiclick(
        "register-job",
        "sample_jobs.entry_task",
        "--name",
        job_name,
        "--runner",
        "docker",
        "--git-remote",
        f"file://{workspace}/.git",
        "--build-context",
        "test_e2e/docker/fixtures/sample_job",
    )

    run_args = ["run-job", job_name, "--git-sha", sha]
    if branch := os.environ.get("GITHUB_REF_NAME"):
        run_args.extend(["--git-branch", branch])
    _aaiclick(*run_args)

    # Drive the worker loop in the background while we poll for completion.
    # max_tasks accommodates: docker_build + entry_task + 2 dynamic children.
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
    # docker_build (host) + entry_task + add + square = 4 tasks
    assert "sample_jobs.entry_task" in entrypoints
    assert "sample_jobs.add" in entrypoints
    assert "sample_jobs.square" in entrypoints
    non_terminal = [t for t in tasks if t.status != TASK_COMPLETED]
    assert not non_terminal, [(t.entrypoint, t.status, t.error) for t in non_terminal]
