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

import pytest
from job_wait import run_worker_until_done
from runner_flow import (
    run_build_job,
    run_jvm_fanout_flow,
    run_jvm_flow,
    run_shell_command_env_flow,
    run_smoke_flow,
    submit_shell_job,
)
from sandbox_e2e import run_sandbox_submission

from aaiclick.data.object.refs import native_ref
from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.models import JOB_FAILED


@pytest.mark.docker_e2e
async def test_docker_runner_smoke(orch_ctx, docker_e2e_user_repo):
    """Build the fixture image from the standalone user repo, run the
    entry task's chain in containers, assert it completes."""
    await run_smoke_flow("docker_e2e_smoke", docker_e2e_user_repo)


@pytest.mark.docker_e2e
async def test_docker_runner_default_dockerfile(orch_ctx, docker_e2e_bare_repo):
    """A user repo with no Dockerfile builds on the default layer over
    ``AAICLICK_BASE_IMAGE``, with its ``requirements.txt`` installed."""
    tasks = await run_build_job("docker_e2e_default_dockerfile", "bare_jobs.entry_task", docker_e2e_bare_repo)

    entry = next(t for t in tasks if t.entrypoint == "bare_jobs.entry_task")
    # cwd is the copied repo, the non-root user could write into it, and the manifest's package is importable.
    assert entry.result == native_ref({"cwd": "/src", "user": "aaiclick", "tabulate": "0.9.0"}), (
        entry.result,
        entry.error,
    )


@pytest.mark.docker_e2e
async def test_docker_runner_build_sees_no_worker_secrets(orch_ctx, docker_e2e_leak_repo):
    """A Dockerfile step — even one declaring ``ARG PIP_INDEX_URL`` — sees no
    worker env and no index URL: those reach builds only as mounted secrets."""
    tasks = await run_build_job("docker_e2e_build_env_leak", "leak_jobs.read_build_env", docker_e2e_leak_repo)
    entry = next(t for t in tasks if t.entrypoint == "leak_jobs.read_build_env")
    build_env = entry.result["value"]
    assert "AAICLICK_" not in build_env
    assert "8080/simple" not in build_env


@pytest.mark.docker_e2e
async def test_docker_runner_jvm_task(orch_ctx, docker_e2e_user_repo, jvm_task_image):
    """A ``jvm`` task between two Python tasks, each in its own container."""
    await run_jvm_flow("docker_e2e_jvm", docker_e2e_user_repo, jvm_task_image)


@pytest.mark.docker_e2e
async def test_docker_runner_jvm_fanout(orch_ctx, docker_e2e_user_repo, jvm_task_image):
    """A jvm task returning a typed ``TaskResult`` of jvm children."""
    await run_jvm_fanout_flow("docker_e2e_jvm_fanout", docker_e2e_user_repo, jvm_task_image)


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
