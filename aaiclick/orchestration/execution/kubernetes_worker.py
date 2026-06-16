"""Kubernetes runner — host-side ExecuteFn and Pod-side entrypoint.

Mirrors ``docker_worker``: ``_run_task_in_pod`` drives a ``KubernetesVehicle``
via the shared ``drive_vehicle``; ``_pod_main`` runs inside the Pod and writes
a ``TaskRunResult`` row — the cross-node equivalent of docker's bind-mounted
``result.json``.

Reaper invariant: the Pod never writes terminal task status. It writes only
its own ``task_run_results`` row; the host worker writes the terminal ``Task``
status via ``_handle_task_result``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import NamedTuple

from sqlmodel import select

from ..logging import get_logs_dir
from ..models import Job, Task, TaskRunResult
from ..orch_context import get_sql_session
from . import cli
from .claiming import check_task_cancelled
from .docker_worker import _build_container_env
from .runner import execute_task, register_returned_tasks, serialize_task_result
from .worker import POLL_INTERVAL, RunnerResult, drive_vehicle, worker_heartbeat

POD_ENTRYPOINT = ["python", "-m", "aaiclick.orchestration.execution.kubernetes_worker"]
# Pod-internal log dir; ephemeral. The host captures logs via `kubectl logs`.
POD_LOG_DIR = "/tmp/aaiclick-logs"


def _kubectl_bin() -> str:
    return os.environ.get("AAICLICK_KUBECTL_BIN", "kubectl")


def _pod_name(task_id: int, run_epoch: int) -> str:
    """DNS-safe Pod name unique per task attempt."""
    return f"aaiclick-task-{task_id}-{run_epoch}"


def _build_pod_manifest(
    *,
    name: str,
    namespace: str,
    image_tag: str,
    task_id: int,
    run_epoch: int,
    env: dict[str, str],
    service_account: str | None,
    image_pull_secret: str | None,
    resources: dict | None,
) -> dict:
    """Build the bare-Pod manifest (``restartPolicy: Never`` — aaiclick owns
    retries). Optional cluster fields are omitted when unset so the cluster
    defaults apply."""
    container: dict = {
        "name": "task",
        "image": image_tag,
        "command": [*POD_ENTRYPOINT, "--task-id", str(task_id), "--run-epoch", str(run_epoch)],
        "env": [{"name": k, "value": v} for k, v in env.items()],
    }
    if resources:
        container["resources"] = resources
    spec: dict = {"restartPolicy": "Never", "containers": [container]}
    if service_account:
        spec["serviceAccountName"] = service_account
    if image_pull_secret:
        spec["imagePullSecrets"] = [{"name": image_pull_secret}]
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": namespace},
        "spec": spec,
    }


# ---------------------------------------------------------------------------
# Host-side
# ---------------------------------------------------------------------------


class _PodSpec(NamedTuple):
    image_tag: str
    namespace: str
    service_account: str | None
    image_pull_secret: str | None
    resources: dict | None


async def _fetch_pod_spec(job_id: int) -> _PodSpec:
    async with get_sql_session() as session:
        job = (await session.execute(select(Job).where(Job.id == job_id))).scalar_one_or_none()
    if job is None or not job.image_tag:
        raise ValueError(f"Job {job_id} has no image_tag — was it submitted in kubernetes mode?")
    kc = job.kubernetes_config or {}
    return _PodSpec(
        image_tag=job.image_tag,
        namespace=kc.get("namespace") or "default",
        service_account=kc.get("service_account"),
        image_pull_secret=kc.get("image_pull_secret"),
        resources=kc.get("resources"),
    )


class _PodHandle:
    """Mutable handle: ``wait`` stashes the result row for ``collect`` (the
    ``TaskVehicle.collect`` hook is synchronous, so the async DB read happens
    in ``wait``)."""

    def __init__(self, name: str, namespace: str, log_path: str, task_id: int, run_epoch: int) -> None:
        self.name = name
        self.namespace = namespace
        self.log_path = log_path
        self.task_id = task_id
        self.run_epoch = run_epoch
        self.result_row: RunnerResult | None = None


async def _kubectl_delete(handle: _PodHandle) -> None:
    await cli.run(
        _kubectl_bin(), "delete", "pod", handle.name, "-n", handle.namespace, "--ignore-not-found", check=False
    )


async def _pod_phase(handle: _PodHandle) -> str:
    _, out, _ = await cli.run(
        _kubectl_bin(), "get", "pod", handle.name, "-n", handle.namespace,
        "-o", "jsonpath={.status.phase}", check=False,
    )
    return out.strip()


async def _pod_exit_code(handle: _PodHandle) -> int:
    _, out, _ = await cli.run(
        _kubectl_bin(), "get", "pod", handle.name, "-n", handle.namespace,
        "-o", "jsonpath={.status.containerStatuses[0].state.terminated.exitCode}", check=False,
    )
    try:
        return int(out.strip())
    except ValueError:
        return -1


async def _capture_pod_logs(handle: _PodHandle) -> None:
    """Authoritative log fetch into the host log file (run once the Pod is
    terminal). The container's task stdout reaches ``kubectl logs`` because
    ``capture_task_output`` tees to stdout."""
    _, out, _ = await cli.run(_kubectl_bin(), "logs", handle.name, "-n", handle.namespace, check=False)
    Path(handle.log_path).write_text(out)


async def _read_task_run_result_row(task_id: int, run_epoch: int) -> RunnerResult | None:
    async with get_sql_session() as session:
        row = (
            await session.execute(
                select(TaskRunResult).where(
                    TaskRunResult.task_id == task_id, TaskRunResult.run_epoch == run_epoch
                )
            )
        ).scalar_one_or_none()
    if row is None:
        return None
    return RunnerResult(row.success, row.result_ref, row.log_path, row.error)


class _KubernetesVehicle:
    """``TaskVehicle`` for the Kubernetes runner."""

    def __init__(self, spec: _PodSpec, log_base: str) -> None:
        self._spec = spec
        self._log_base = log_base

    async def launch(self, task: Task, worker_id: int) -> _PodHandle:
        env = _build_container_env()
        env["AAICLICK_LOG_DIR"] = POD_LOG_DIR
        name = _pod_name(task.id, task.run_epoch)
        manifest = _build_pod_manifest(
            name=name, namespace=self._spec.namespace, image_tag=self._spec.image_tag,
            task_id=task.id, run_epoch=task.run_epoch, env=env,
            service_account=self._spec.service_account, image_pull_secret=self._spec.image_pull_secret,
            resources=self._spec.resources,
        )
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(manifest, f)
            manifest_path = f.name
        try:
            await cli.run(_kubectl_bin(), "apply", "-f", manifest_path)
        finally:
            os.unlink(manifest_path)
        log_path = os.path.join(self._log_base, str(task.job_id), str(task.id), f"k8s-{task.run_epoch}.log")
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        return _PodHandle(name, self._spec.namespace, log_path, task.id, task.run_epoch)

    async def wait(self, handle: _PodHandle, timeout: float | None) -> tuple[int, str | None]:
        elapsed = 0.0
        error: str | None = None
        while True:
            phase = await _pod_phase(handle)
            if phase in ("Succeeded", "Failed"):
                break
            if timeout is not None and elapsed >= timeout:
                error = f"Task timed out after {timeout}s"
                break
            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
        await _capture_pod_logs(handle)
        handle.result_row = await _read_task_run_result_row(handle.task_id, handle.run_epoch)
        exit_code = await _pod_exit_code(handle) if error is None else -1
        return exit_code, error

    async def poll_cancelled(self, task: Task) -> bool:
        return await check_task_cancelled(task.id)

    async def terminate(self, handle: _PodHandle) -> None:
        await _kubectl_delete(handle)

    def collect(self, handle: _PodHandle, exit_code: int, error: str | None, was_cancelled: bool) -> RunnerResult:
        if was_cancelled:
            return RunnerResult(False, None, None, "cancelled")
        if error is not None:
            return RunnerResult(False, None, None, error)
        if handle.result_row is None:
            return RunnerResult(False, None, None, f"pod exited with code {exit_code} but wrote no result row")
        return handle.result_row

    async def cleanup(self, handle: _PodHandle) -> None:
        await _kubectl_delete(handle)


async def _run_task_in_pod(task: Task, worker_id: int) -> tuple[bool, dict | None, str | None, str | None]:
    """ExecuteFn for the Kubernetes runner."""
    spec = await _fetch_pod_spec(task.job_id)
    raw_timeout = os.environ.get("AAICLICK_TASK_TIMEOUT")
    timeout = float(raw_timeout) if raw_timeout else None
    vehicle = _KubernetesVehicle(spec, get_logs_dir())
    result = await drive_vehicle(
        task, worker_id, vehicle, timeout=timeout, poll_interval=POLL_INTERVAL, heartbeat_fn=worker_heartbeat
    )
    return result.success, result.result_ref, result.log_path, result.error


# ---------------------------------------------------------------------------
# Pod-side
# ---------------------------------------------------------------------------


async def _write_task_run_result(
    task_id: int, run_epoch: int, success: bool, result_ref: dict | None, log_path: str | None, error: str | None
) -> None:
    """Upsert the per-attempt result row the host reads back. Keyed by
    ``(task_id, run_epoch)`` so a re-run under a new epoch never collides."""
    async with get_sql_session() as session:
        existing = (
            await session.execute(
                select(TaskRunResult).where(
                    TaskRunResult.task_id == task_id, TaskRunResult.run_epoch == run_epoch
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            existing = TaskRunResult(task_id=task_id, run_epoch=run_epoch, success=success)
            session.add(existing)
        existing.success = success
        existing.result_ref = result_ref
        existing.log_path = log_path
        existing.error = error
        await session.commit()


async def _pod_main(task_id: int, run_epoch: int) -> int:
    """Entry point invoked inside the Pod. Runs the task via the shared
    ``execute_task`` path and writes a ``TaskRunResult`` row for the host."""
    from ..orch_context import orch_context

    success, result_ref, log_path, error = False, None, None, None
    exit_code = 0
    try:
        async with orch_context():
            async with get_sql_session() as session:
                task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()
            data_result, log_path = await execute_task(task)
            data_result = await register_returned_tasks(data_result, task.id, task.job_id)
            result_ref = serialize_task_result(data_result, task.job_id)
            success = True
    except BaseException as e:
        success, error, exit_code = False, f"{type(e).__name__}: {e}", 1

    await _write_task_run_result(task_id, run_epoch, success, result_ref, log_path, error)
    return exit_code


def _pod_cli() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m aaiclick.orchestration.execution.kubernetes_worker",
        description="Pod-side entrypoint for the Kubernetes runner.",
    )
    parser.add_argument("--task-id", type=int, required=True)
    parser.add_argument("--run-epoch", type=int, required=True)
    args = parser.parse_args()
    sys.exit(asyncio.run(_pod_main(args.task_id, args.run_epoch)))


if __name__ == "__main__":
    _pod_cli()
