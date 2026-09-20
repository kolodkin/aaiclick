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
import json
import subprocess
import sys
from pathlib import Path

import pytest
from job_wait import wait_for_job_by_name

from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.docker_config import compute_image_tag
from aaiclick.orchestration.execution.mp_worker import mp_worker_main_loop
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import JOB_COMPLETED, JOB_FAILED, TASK_COMPLETED
from aaiclick.orchestration.runner_config import (
    ENTRY_JVM,
    DockerRunner,
    ImageBuild,
    ImagePrebuilt,
    parse_image_source,
    parse_runner_config,
)


def _aaiclick(*args: str, cwd: Path) -> subprocess.CompletedProcess:
    """Run a `python -m aaiclick` CLI invocation from ``cwd`` (the user
    repo, so the entrypoint module is on ``sys.path``). Echoes the captured
    output (visible under ``pytest -s``) so a silent submit failure is
    diagnosable, then raises on a non-zero exit."""
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
        completed = await wait_for_job_by_name(job_name)
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

    assert completed.status == JOB_COMPLETED, completed.error
    assert completed.runner is not None
    runner = parse_runner_config(completed.runner)
    assert isinstance(runner, DockerRunner)

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


@pytest.mark.docker_e2e
async def test_docker_runner_jvm_task(orch_ctx, docker_e2e_user_repo, jvm_task_image):
    """Run a ``jvm`` task between two Python tasks, each in its own container.

    Covers what the SDK's SQLite unit suite cannot: the runner env handoff
    into a JVM image, the shim reading an upstream Python result and writing
    its own result row over JDBC against PostgreSQL, and a Python task
    consuming the jvm return value downstream."""
    remote, sha, worktree = docker_e2e_user_repo
    job_name = "docker_e2e_jvm"

    _aaiclick(
        "register-job",
        "sample_jobs.jvm_entry_task",
        "--name",
        job_name,
        "--runner",
        "docker",
        "--git-remote",
        remote,
        cwd=worktree,
    )

    _aaiclick(
        "run-job",
        job_name,
        "--git-sha",
        sha,
        "--kwargs",
        json.dumps({"jvm_image": jvm_task_image}),
        cwd=worktree,
    )

    worker_task = asyncio.create_task(
        mp_worker_main_loop(
            max_tasks=10,
            install_signal_handlers=False,
            max_empty_polls=10,
        )
    )
    try:
        completed = await wait_for_job_by_name(job_name)
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

    assert completed.status == JOB_COMPLETED, completed.error

    tasks = await get_tasks_for_job(completed.id)
    non_terminal = [t for t in tasks if t.status != TASK_COMPLETED]
    assert not non_terminal, [(t.entrypoint, t.status, t.error) for t in non_terminal]

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
    """Run a shell command in a prebuilt image (no git repo, no build).

    The prebuilt ``python:3.12`` image runs an exit-0 shell command, so the
    job completes with no auto-injected build task."""
    job_name = "docker_e2e_shell_prebuilt"

    _aaiclick(
        "register-job",
        "shell.placeholder",
        "--name",
        job_name,
        "--runner",
        "docker",
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
        'python -c "print(123)"',
        cwd=tmp_path,
    )

    worker_task = asyncio.create_task(
        mp_worker_main_loop(
            max_tasks=10,
            install_signal_handlers=False,
            max_empty_polls=10,
        )
    )
    try:
        completed = await wait_for_job_by_name(job_name)
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

    assert completed.status == JOB_COMPLETED, completed.error

    tasks = await get_tasks_for_job(completed.id)
    entrypoints = [t.entrypoint for t in tasks]
    # Prebuilt image → no build task is injected; the single shell task ran
    # the exit-0 command and completed.
    assert len(tasks) == 1, entrypoints
    assert tasks[0].status == TASK_COMPLETED, tasks[0].error


@pytest.mark.docker_e2e
async def test_docker_runner_shell_nonzero_fails(orch_ctx, tmp_path):
    """A shell command that exits non-zero fails the job."""
    job_name = "docker_e2e_shell_nonzero"

    _aaiclick(
        "register-job",
        "shell.placeholder",
        "--name",
        job_name,
        "--runner",
        "docker",
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
        'python -c "import sys; sys.exit(7)"',
        cwd=tmp_path,
    )

    # A failed task lands in PENDING_CLEANUP; the BackgroundWorker is what
    # transitions it to FAILED and then fails the job (the success path is
    # finalized inline by the mp worker, but the failure path is not). Run a
    # real one alongside the worker so the job reaches its terminal state.
    bg_worker = BackgroundWorker(poll_interval=1.0)
    await bg_worker.start()
    worker_task = asyncio.create_task(
        mp_worker_main_loop(
            max_tasks=10,
            install_signal_handlers=False,
            max_empty_polls=10,
        )
    )
    try:
        completed = await wait_for_job_by_name(job_name)
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        await bg_worker.stop()

    assert completed.status == JOB_FAILED, completed.status
