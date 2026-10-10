"""End-to-end tests for the Docker runner.

Drives the full ``register-job`` → ``run-job`` → build → run → result
path against a real docker daemon, a real local registry, a real test
pypi serving the wheel under test, and a real git remote (the CI
``git daemon`` service the ``docker_e2e_user_repo`` fixture publishes the
user repo into). Both the registration and the job submission go through
the ``python -m aaiclick`` CLI as a real user would, run from the
user-repo working tree so the entrypoint resolves from it — exactly as an
external user standing in their project.

Shared flows live in ``runner_flow``.

Marked ``docker_e2e`` so it opts out of the default test run; both the
nightly workflow and the publish-time release gate pass
``test_e2e/docker/`` to pytest with ``-m docker_e2e`` to pick it up.

The "user repo" is decoupled from the aaiclick checkout: aaiclick-under-
test arrives via the test pypi wheel, so the served repo only carries the
user's project. Like the registry and pypiserver, the git daemon is a
fixed-port loopback service stood up by the workflow, so nothing leaves
the runner."""

from __future__ import annotations

import json

import pytest
from job_wait import run_worker_until_done
from runner_flow import run_build_job, run_shell_command_env_flow, run_smoke_flow, submit_shell_job
from sandbox_e2e import run_sandbox_submission

from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.models import JOB_FAILED
from aaiclick.orchestration.runner_config import ENTRY_JVM, ImagePrebuilt, parse_image_source


@pytest.mark.docker_e2e
async def test_docker_runner_smoke(orch_ctx, docker_e2e_user_repo):
    """Build the fixture image from the standalone user repo, run the
    entry task's chain in containers, assert it completes."""
    await run_smoke_flow("docker_e2e_smoke", docker_e2e_user_repo)


@pytest.mark.docker_e2e
async def test_docker_runner_default_dockerfile(orch_ctx, docker_e2e_bare_repo):
    """A user repo with no Dockerfile builds on the default layer over ``AAICLICK_BASE_IMAGE``."""
    tasks = await run_build_job("docker_e2e_default_dockerfile", "bare_jobs.entry_task", docker_e2e_bare_repo)

    entry = next(t for t in tasks if t.entrypoint == "bare_jobs.entry_task")
    # cwd is the copied repo and the non-root user could write into it.
    assert entry.result == {"native_value": {"cwd": "/src", "user": "aaiclick"}}, (entry.result, entry.error)


@pytest.mark.docker_e2e
async def test_docker_runner_jvm_task(orch_ctx, docker_e2e_user_repo, jvm_task_image):
    """Run a ``jvm`` task between two Python tasks, each in its own container.

    Covers what the SDK's SQLite unit suite cannot: the runner env handoff
    into a JVM image, the shim reading an upstream Python result and writing
    its own result row over JDBC against PostgreSQL, and a Python task
    consuming the jvm return value downstream."""
    tasks = await run_build_job(
        "docker_e2e_jvm",
        "sample_jobs.jvm_entry_task",
        docker_e2e_user_repo,
        "--kwargs",
        json.dumps({"jvm_image": jvm_task_image}),
    )

    # The jvm task ran in the prebuilt fixture image, not the job's build image.
    summed = next(t for t in tasks if t.entry_type == ENTRY_JVM)
    assert summed.entrypoint == "io.github.kolodkin.aaiclick.e2e.SumTask"
    assert summed.image_source is not None
    source = parse_image_source(summed.image_source)
    assert isinstance(source, ImagePrebuilt)
    assert source.image_tag == jvm_task_image

    # The shim resolved produce_values' [10, 20, 30] through the upstream
    # ref and wrote its map back in the shared ``native_value`` shape.
    expected = {"native_value": {"total": 60.0, "count": 3}}
    assert summed.result == expected, summed.result

    # And the Python consumer read that jvm result as a plain dict.
    reported = next(t for t in tasks if t.entrypoint == "sample_jobs.report")
    assert reported.result == expected, reported.result


@pytest.mark.docker_e2e
async def test_docker_runner_shell_prebuilt(orch_ctx, tmp_path):
    """A shell command in a prebuilt image: no build task, and the
    ``--env-file`` delivery of ``command_env`` is exercised."""
    await run_shell_command_env_flow("docker_e2e_shell_prebuilt", tmp_path)


@pytest.mark.docker_e2e
async def test_docker_runner_shell_nonzero_fails(orch_ctx, tmp_path):
    """A shell command that exits non-zero fails the job."""
    job_name = "docker_e2e_shell_nonzero"
    submit_shell_job(job_name, 'python -c "import sys; sys.exit(7)"', tmp_path)

    # A failed task lands in PENDING_FAILURE_CLEANUP; the BackgroundWorker is what
    # transitions it to FAILED and then fails the job (the success path is
    # finalized inline by the mp worker, but the failure path is not). Run a
    # real one alongside the worker so the job reaches its terminal state.
    bg_worker = BackgroundWorker(poll_interval=1.0)
    await bg_worker.start()
    try:
        completed = await run_worker_until_done(job_name)
    finally:
        await bg_worker.stop()

    assert completed.status == JOB_FAILED, completed.status


@pytest.mark.docker_e2e
async def test_docker_runner_sandbox_submission(orch_ctx, sandbox_remote, tmp_path):
    """A sandbox submission is committed into an empty remote and every ``@job``
    in it runs on the docker runner, built on the default Dockerfile."""
    await run_sandbox_submission(sandbox_remote, tmp_path / "clone", max_tasks=10)
