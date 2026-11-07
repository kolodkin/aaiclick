"""End-to-end tests for the Kubernetes runner.

Drives the full ``register-job`` → ``run-job`` → build → Pod → result path
against a real minikube cluster, a local registry, a test pypi serving the
wheel under test, and the CI ``git daemon`` (the ``kubernetes_e2e_user_repo``
fixture publishes the user repo into it). Both registration and submission go
through the ``python -m aaiclick`` CLI from the user-repo working tree, exactly
as an external user would.

Shared flows live in ``runner_flow``.

Marked ``kubernetes_e2e`` so it opts out of the default test run; the workflow
passes ``test_e2e/kubernetes/`` with ``-m kubernetes_e2e``."""

from __future__ import annotations

import asyncio
import contextlib
import json
import subprocess

import pytest
from job_wait import run_worker_until_all_done, start_worker_loop, wait_for_job_by_name
from runner_flow import (
    aaiclick_cli,
    run_child_build_flow,
    run_jvm_fanout_flow,
    run_jvm_flow,
    run_shell_command_env_flow,
    run_smoke_flow,
    submit_shell_job,
)
from sandbox_e2e import run_sandbox_submission

from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.execution.execution_worker import register_execution_worker, request_execution_worker_stop
from aaiclick.orchestration.execution.kubernetes_worker import _pod_name
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.kubernetes_config import ENV_PENDING_TIMEOUT
from aaiclick.orchestration.logging import read_task_logs
from aaiclick.orchestration.models import JOB_COMPLETED, JOB_FAILED, TASK_COMPLETED, TASK_FAILED
from aaiclick.orchestration.registered_jobs import run_job
from aaiclick.testing import get_task_runs, until


@pytest.mark.kubernetes_e2e
async def test_kubernetes_runner_smoke(orch_ctx, kubernetes_e2e_user_repo):
    """Build the fixture image, run the entry task's chain as Pods, assert it
    completes and the Objects flowed through ClickHouse across Pods."""
    await run_smoke_flow("k8s_e2e_smoke", kubernetes_e2e_user_repo)


@pytest.mark.kubernetes_e2e
async def test_kubernetes_runner_shell_child_declares_build(orch_ctx, kubernetes_e2e_user_repo):
    """A shell task's child declares its own build of the parent's repo."""
    await run_child_build_flow("k8s_e2e_child_build", kubernetes_e2e_user_repo)


@pytest.mark.kubernetes_e2e
async def test_kubernetes_runner_jvm_task(orch_ctx, kubernetes_e2e_user_repo, jvm_task_image):
    """A ``jvm`` task between two Python tasks, each in its own Pod."""
    await run_jvm_flow("kubernetes_e2e_jvm", kubernetes_e2e_user_repo, jvm_task_image)


@pytest.mark.kubernetes_e2e
async def test_kubernetes_runner_jvm_fanout(orch_ctx, kubernetes_e2e_user_repo, jvm_task_image):
    """A jvm task returning a typed ``TaskResult`` of jvm children."""
    await run_jvm_fanout_flow("kubernetes_e2e_jvm_fanout", kubernetes_e2e_user_repo, jvm_task_image)


@pytest.mark.kubernetes_e2e
async def test_kubernetes_runner_shell_command_env(orch_ctx, tmp_path):
    """A shell command as a ``kubectl run`` Pod proves the per-attempt Secret
    path end to end; afterwards the Secret must be gone (``cleanup_argv``
    deletes it with the Pod)."""
    task = await run_shell_command_env_flow("k8s_e2e_shell_command_env", tmp_path)

    secret = _pod_name(task.id, task.run_epoch)
    probe = subprocess.run(["kubectl", "get", "secret", secret], capture_output=True, text=True, check=False)
    assert probe.returncode != 0 and "NotFound" in probe.stderr, probe


@pytest.mark.kubernetes_e2e
async def test_kubernetes_runner_sandbox_submission(orch_ctx, sandbox_remote, tmp_path):
    """A sandbox submission is committed into an empty remote and every ``@job``
    in it runs on the kubernetes runner, built on the default Dockerfile."""
    await run_sandbox_submission(sandbox_remote, tmp_path / "clone", max_tasks=20)


# ---------------------------------------------------------------------------
# The concurrent worker: capacity, restart adoption, per-job gate
# ---------------------------------------------------------------------------


async def _managed_pods() -> dict[str, str]:
    """``{pod name: phase}`` of the aaiclick task Pods in the namespace."""
    proc = await asyncio.create_subprocess_exec(
        "kubectl",
        "get",
        "pods",
        "-l",
        "aaiclick/managed=true",
        "-o",
        "json",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    out, _ = await proc.communicate()
    if proc.returncode != 0:
        return {}
    # A Pod being deleted keeps its phase until the grace period ends; it is not live.
    return {
        item["metadata"]["name"]: item["status"].get("phase", "")
        for item in json.loads(out)["items"]
        if "deletionTimestamp" not in item["metadata"]
    }


async def _peak_pod_count(stop: asyncio.Event) -> int:
    peak = 0
    while not stop.is_set():
        peak = max(peak, len(await _managed_pods()))
        await asyncio.sleep(1.0)
    return peak


async def _until(predicate, timeout: float = 300.0, what: str = "condition") -> None:
    try:
        await until(predicate, timeout, interval=1.0, what=what)
    except AssertionError as e:
        raise AssertionError(f"{e}; pods={await _managed_pods()}") from None


@pytest.mark.kubernetes_e2e
async def test_concurrent_pods_one_worker(orch_ctx, kubernetes_e2e_user_repo):
    """One worker drives the fan-out's Pods at the same time: the peak number
    of live task Pods, sampled while the job runs, is above one. Printed as
    the run's one metric."""
    job_name = "k8s_e2e_concurrent"
    cwd = kubernetes_e2e_user_repo.worktree
    aaiclick_cli(
        "register-job",
        "sample_jobs.fanout_naps",
        "--name",
        job_name,
        "--build",
        "--git-remote",
        kubernetes_e2e_user_repo.remote,
        cwd=cwd,
    )
    aaiclick_cli("run-job", job_name, "--git-sha", kubernetes_e2e_user_repo.sha, cwd=cwd)
    stop = asyncio.Event()
    sampler = asyncio.create_task(_peak_pod_count(stop))

    try:
        (job,) = await run_worker_until_all_done(job_name)
    finally:
        stop.set()
    peak = await sampler

    print(f"peak concurrent task Pods: {peak}")
    assert job.status == JOB_COMPLETED, job.error
    assert peak >= 2, peak


@pytest.mark.kubernetes_e2e
async def test_worker_restart_adopts_pods(orch_ctx, kubernetes_e2e_user_repo, tmp_path):
    """Stopping the worker mid-run leaves a module Pod and a shell Pod
    running; the next worker adopts both, the jobs complete with a single
    attempt each, and the shell task's log has the lines from before and
    after the restart."""
    module_job, shell_job = "k8s_e2e_restart_module", "k8s_e2e_restart_shell"
    cwd = kubernetes_e2e_user_repo.worktree
    aaiclick_cli(
        "register-job",
        "sample_jobs.nap",
        "--name",
        module_job,
        "--build",
        "--git-remote",
        kubernetes_e2e_user_repo.remote,
        cwd=cwd,
    )
    aaiclick_cli(
        "run-job", module_job, "--git-sha", kubernetes_e2e_user_repo.sha, "--kwargs", '{"seconds": 45}', cwd=cwd
    )
    submit_shell_job(shell_job, "sh -c 'echo before; sleep 45; echo after'", tmp_path)

    worker = await register_execution_worker()
    first = start_worker_loop(max_tasks=10, execution_worker_id=worker.id)
    # Both task Pods running (the build task runs first on the host), then
    # stop the worker the way ``execution-worker stop`` does.
    try:
        await _until(_two_pods_running, timeout=600.0, what="module and shell Pods running")
    finally:
        await request_execution_worker_stop(worker.id)
        await asyncio.wait_for(first, timeout=120.0)
    assert len([p for p in (await _managed_pods()).values() if p == "Running"]) == 2, "Pods must survive the worker"

    second = start_worker_loop(max_tasks=10)
    try:
        module_done = await wait_for_job_by_name(module_job)
        shell_done = await wait_for_job_by_name(shell_job)
    finally:
        second.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await second

    assert (module_done.status, shell_done.status) == (JOB_COMPLETED, JOB_COMPLETED), (
        module_done.error,
        shell_done.error,
    )
    for job in (module_done, shell_done):
        for task in await get_tasks_for_job(job.id):
            assert task.status == TASK_COMPLETED and task.attempt == 0, (task.entrypoint, task.status, task.attempt)
            assert len(await get_task_runs(task.id)) == 1, task.entrypoint
    (shell_task,) = await get_tasks_for_job(shell_done.id)
    (shell_run,) = await get_task_runs(shell_task.id)
    # Log delivery across a restart is at-least-once: ``--since-time`` has
    # second precision, so the last line stored before the restart may repeat.
    lines = [line.text for line in await read_task_logs(shell_task.id, shell_run.run_id)]
    assert lines[0] == "before" and lines[-1] == "after" and set(lines) == {"before", "after"}, lines


async def _two_pods_running() -> bool:
    return len([p for p in (await _managed_pods()).values() if p == "Running"]) >= 2


@pytest.mark.kubernetes_e2e
async def test_unschedulable_job_does_not_block_others(orch_ctx, monkeypatch, tmp_path):
    """A job no node can fit stays Pending; another job completes meanwhile
    (per-job gate), and the stuck one fails on the not-started deadline with
    the scheduler's reason."""
    monkeypatch.setenv(ENV_PENDING_TIMEOUT, "30")
    stuck, other = "k8s_e2e_unschedulable", "k8s_e2e_fits"
    await run_job(
        stuck,
        "shell.placeholder",
        entry_type="shell",
        command=["true"],
        image="python:3.12",
        resources={"requests": {"cpu": "1000"}},
    )
    submit_shell_job(other, "true", tmp_path)

    # A failed task lands in PENDING_FAILURE_CLEANUP; the BackgroundWorker is
    # what fails the job (the success path is finalized inline).
    bg_worker = BackgroundWorker(poll_interval=1.0)
    await bg_worker.start()
    try:
        fits, failed = await run_worker_until_all_done(other, stuck)
    finally:
        await bg_worker.stop()

    assert fits.status == JOB_COMPLETED, fits.error
    assert failed.status == JOB_FAILED
    (task,) = await get_tasks_for_job(failed.id)
    assert task.status == TASK_FAILED and "not started after 30s" in (task.error or ""), task.error
    assert "Insufficient cpu" in (task.error or ""), task.error
