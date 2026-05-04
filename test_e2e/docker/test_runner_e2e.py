"""End-to-end smoke test for the Docker runner.

Exercises the full ``register_job`` → ``run_job`` → build → run → result
path against a real docker daemon, a real local registry, and a real
test pypi serving the wheel under test. Marked ``docker_e2e`` so it
opts out of the default test run; both the nightly workflow and the
publish-time release gate pass ``test_e2e/docker/`` to pytest with
``-m docker_e2e`` to pick it up.

The test reuses the aaiclick checkout itself as its "user repo" via a
``file://`` remote — the build runs on the CI host which already has
the checkout, so going through GitHub would be a pointless network
round-trip with extra flake surface."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta

import pytest
from sqlmodel import select

from aaiclick.orchestration.execution.mp_worker import mp_worker_main_loop
from aaiclick.orchestration.models import (
    JOB_COMPLETED,
    JOB_FAILED,
    RUNNER_DOCKER,
    Job,
)
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.registered_jobs import register_job, run_job


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"{name} not set; e2e suite is workflow-driven")
    return value


async def _wait_for_job(job_id: int, timeout: float = 600.0) -> Job:
    """Poll the Job until it reaches a terminal status, or fail."""
    deadline = datetime.utcnow() + timedelta(seconds=timeout)
    while datetime.utcnow() < deadline:
        async with get_sql_session() as session:
            result = await session.execute(select(Job).where(Job.id == job_id))
            job = result.scalar_one()
        if job.status in (JOB_COMPLETED, JOB_FAILED):
            return job
        await asyncio.sleep(1.0)
    raise TimeoutError(f"Job {job_id} did not complete within {timeout}s")


@pytest.mark.docker_e2e
async def test_docker_runner_smoke(orch_ctx):
    """Build the fixture image from the current checkout, run the entry
    task in a container, assert it completes and returns the build-args
    that the framework forwarded."""
    workspace = _required_env("GITHUB_WORKSPACE")
    sha = _required_env("GITHUB_SHA")

    await register_job(
        name="docker_e2e_smoke",
        entrypoint="sample_jobs.entry_task",
        runner_mode=RUNNER_DOCKER,
        git_remote=f"file://{workspace}/.git",
        build_context="test_e2e/docker/fixtures/sample_job",
    )

    job = await run_job(
        "docker_e2e_smoke",
        "sample_jobs.entry_task",
        git_sha=sha,
        git_branch=os.environ.get("GITHUB_REF_NAME"),
    )

    # Drive the worker loop in the background while we poll for completion.
    worker_task = asyncio.create_task(
        mp_worker_main_loop(
            max_tasks=5,
            install_signal_handlers=False,
            max_empty_polls=10,
        )
    )

    try:
        completed = await _wait_for_job(job.id)
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

    assert completed.status == JOB_COMPLETED, completed.error
    assert completed.image_tag and completed.image_tag.endswith(f":{sha}")
    assert completed.git_sha == sha
