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
from pathlib import Path

import pytest
from job_wait import wait_for_job_by_name
from sqlmodel import select

from aaiclick.internal_api.sandbox import submit_sandbox_file
from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.docker_config import compute_image_tag
from aaiclick.orchestration.execution.kubernetes_worker import _pod_name
from aaiclick.orchestration.execution.mp_worker import mp_worker_main_loop
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import JOB_COMPLETED, SANDBOX_SUBMITTED, TASK_COMPLETED, SandboxFile
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.runner_config import ImageBuild, parse_image_source
from aaiclick.sandbox.repo import SandboxRepo, module_parts, sandbox_repo_override
from aaiclick.view_models import SubmitSandboxRequest


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


_FIXTURES = Path(__file__).parent.parent / "fixtures"


def _remote_head(remote: str) -> str:
    out = subprocess.run(
        ["git", "ls-remote", "--", remote, "refs/heads/main"], check=True, capture_output=True, text=True
    )
    return out.stdout.split()[0]


async def _sandbox_row(file_id: int) -> SandboxFile:
    async with get_sql_session() as session:
        return (await session.execute(select(SandboxFile).where(SandboxFile.id == file_id))).scalar_one()


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

    worker_task = asyncio.create_task(
        mp_worker_main_loop(max_tasks=10, install_signal_handlers=False, max_empty_polls=10)
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

    worker_task = asyncio.create_task(
        mp_worker_main_loop(max_tasks=10, install_signal_handlers=False, max_empty_polls=10)
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
    assert len(tasks) == 1, [t.entrypoint for t in tasks]
    assert tasks[0].status == TASK_COMPLETED, tasks[0].error

    secret = _pod_name(tasks[0].id, tasks[0].run_epoch)
    probe = subprocess.run(["kubectl", "get", "secret", secret], capture_output=True, text=True, check=False)
    assert probe.returncode != 0 and "NotFound" in probe.stderr, probe


@pytest.mark.kubernetes_e2e
async def test_kubernetes_runner_sandbox_submission(orch_ctx, sandbox_remote, tmp_path):
    """A sandbox submission is committed into an empty remote and every ``@job``
    in it runs as Pods, built on the default Dockerfile."""
    source = (_FIXTURES / "sandbox_job" / "sandbox_jobs.py").read_text()
    with sandbox_repo_override(SandboxRepo(sandbox_remote, tmp_path / "clone")):
        view = await submit_sandbox_file(SubmitSandboxRequest(name="demo", source=source), user=None)
        second = await submit_sandbox_file(SubmitSandboxRequest(name="demo", source=source), user=None)
    assert second.git_sha != view.git_sha
    assert _remote_head(sandbox_remote) == second.git_sha

    bg_worker = BackgroundWorker(poll_interval=1.0)
    await bg_worker.start()
    try:
        await bg_worker._run_sandbox_files()
    finally:
        await bg_worker.stop()

    row = await _sandbox_row(view.id)
    assert row.status == SANDBOX_SUBMITTED, row.error
    assert row.job_ids is not None and len(row.job_ids) == 2
    module_name = module_parts(row.path)[1]

    worker_task = asyncio.create_task(
        mp_worker_main_loop(max_tasks=20, install_signal_handlers=False, max_empty_polls=10)
    )
    try:
        for job_name in (f"{module_name}.first", f"{module_name}.second"):
            completed = await wait_for_job_by_name(job_name)
            assert completed.status == JOB_COMPLETED, completed.error
            probe = next(t for t in await get_tasks_for_job(completed.id) if t.entrypoint.endswith(".probe"))
            assert probe.result == {"native_value": {"n": 1}}, (probe.result, probe.error)
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
