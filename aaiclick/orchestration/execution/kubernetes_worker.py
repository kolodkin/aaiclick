"""Kubernetes runner — host-side ExecuteFn and Pod-side entrypoint.

Mirrors ``docker_worker``: ``_run_task_in_pod`` drives a ``KubernetesVehicle``
via the shared ``drive_vehicle``; ``_pod_main`` runs inside the Pod and writes
a ``RemoteTaskResult`` row — the cross-node equivalent of docker's bind-mounted
``result.json``.

Reaper invariant: the Pod never writes terminal task status. It writes only
its own ``remote_task_results`` row; the host worker writes the terminal ``Task``
status via ``_handle_task_result``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tempfile
from typing import NamedTuple

from sqlmodel import select

from ..models import RemoteTaskResult, Task
from ..orch_context import get_sql_session
from ..runner_config import ENTRY_SHELL
from . import cli
from .claiming import check_task_cancelled
from .execution_worker import (
    POLL_INTERVAL,
    JobDispatch,
    RunnerResult,
    TaskVehicle,
    drive_vehicle,
    execution_worker_heartbeat,
    parse_task_timeout,
)
from .docker_build import resolve_launch_image
from .runner import ShellSpec, execute_task, serialize_task_result
from .runner_env import build_runner_env

POD_ENTRYPOINT = ["python", "-m", "aaiclick.orchestration.execution.kubernetes_worker"]


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
    entry_type: str,
    command: list[str] | None,
    command_env: dict[str, str] | None,
) -> dict:
    """Build the bare-Pod manifest (``restartPolicy: Never`` — aaiclick owns
    retries). Optional cluster fields are omitted when unset so the cluster
    defaults apply.

    For ``shell`` tasks the container runs the task's argv directly with only
    ``command_env`` injected — the runner ``env`` (DB creds) is deliberately
    NOT read, so no aaiclick secrets reach a vanilla user image. For ``module``
    tasks the Pod runs the aaiclick shim with the full runner ``env``."""
    if entry_type == ENTRY_SHELL:
        container: dict = {
            "name": "task",
            "image": image_tag,
            "command": command,
            "env": [{"name": k, "value": v} for k, v in (command_env or {}).items()],
        }
    else:
        container = {
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
    entry_type: str
    command: list[str] | None
    command_env: dict[str, str] | None


def _pod_spec_from(task: Task, dispatch: JobDispatch) -> _PodSpec:
    if not dispatch.image_tag:
        raise ValueError(f"Job {task.job_id} has no image_tag — was it submitted in kubernetes mode?")
    kc = dispatch.kubernetes_config or {}
    return _PodSpec(
        image_tag=dispatch.image_tag,
        namespace=kc.get("namespace") or "default",
        service_account=kc.get("service_account"),
        image_pull_secret=kc.get("image_pull_secret"),
        resources=kc.get("resources"),
        entry_type=dispatch.entry_type,
        command=dispatch.command,
        command_env=dispatch.command_env,
    )


class _PodHandle:
    """Pod identity + a ``deleted`` latch so ``cleanup`` doesn't re-delete a
    Pod ``terminate`` already removed (the cancellation path)."""

    def __init__(self, name: str, namespace: str, task_id: int, job_id: int, run_epoch: int) -> None:
        self.name = name
        self.namespace = namespace
        self.task_id = task_id
        self.job_id = job_id
        self.run_epoch = run_epoch
        self.deleted = False


async def _kubectl_delete(handle: _PodHandle) -> None:
    await cli.run(
        _kubectl_bin(), "delete", "pod", handle.name, "-n", handle.namespace, "--ignore-not-found", check=False
    )


async def _pod_status(handle: _PodHandle) -> tuple[str, int]:
    """One ``kubectl get`` returning ``(phase, container exit code)``. The exit
    code is ``-1`` until the container has terminated."""
    _, out, _ = await cli.run(
        _kubectl_bin(),
        "get",
        "pod",
        handle.name,
        "-n",
        handle.namespace,
        "-o",
        "jsonpath={.status.phase} {.status.containerStatuses[0].state.terminated.exitCode}",
        check=False,
    )
    parts = out.split()
    phase = parts[0] if parts else ""
    try:
        exit_code = int(parts[1]) if len(parts) > 1 else -1
    except ValueError:
        exit_code = -1
    return phase, exit_code


def build_shell_pod_spec(task: Task, dispatch: JobDispatch, image_tag: str) -> ShellSpec:
    """Wrap a shell task's argv as a foreground ``kubectl run --attach --rm``.

    The full container spec (command, env, resources, serviceAccount,
    imagePullSecrets) rides in ``--overrides`` built from the same manifest
    as module Pods, so shell Pods keep their cluster config; ``--attach``
    propagates the container's exit code and streams its output on the
    kubectl process's stdout. ``--quiet`` keeps kubectl's own chatter out of
    the captured log."""
    pod = _pod_spec_from(task, dispatch._replace(image_tag=image_tag))
    name = _pod_name(task.id, task.run_epoch)
    manifest = _build_pod_manifest(
        name=name,
        namespace=pod.namespace,
        image_tag=image_tag,
        task_id=task.id,
        run_epoch=task.run_epoch,
        env={},
        service_account=pod.service_account,
        image_pull_secret=pod.image_pull_secret,
        resources=pod.resources,
        entry_type=ENTRY_SHELL,
        command=pod.command,
        command_env=pod.command_env,
    )
    overrides = {"apiVersion": "v1", "spec": manifest["spec"]}
    argv = [
        _kubectl_bin(),
        "run",
        name,
        "-n",
        pod.namespace,
        "--attach",
        "--rm",
        "--quiet",
        "--restart=Never",
        f"--image={image_tag}",
        f"--overrides={json.dumps(overrides)}",
    ]
    return ShellSpec(
        argv,
        None,
        cleanup_argv=[_kubectl_bin(), "delete", "pod", name, "-n", pod.namespace, "--ignore-not-found"],
    )


async def _read_task_run_result_row(task_id: int, run_epoch: int) -> RunnerResult | None:
    async with get_sql_session() as session:
        row = (
            await session.execute(
                select(RemoteTaskResult).where(
                    RemoteTaskResult.task_id == task_id, RemoteTaskResult.run_epoch == run_epoch
                )
            )
        ).scalar_one_or_none()
    if row is None:
        return None
    return RunnerResult(row.success, row.result_ref, row.error)


class _KubernetesVehicle(TaskVehicle["_PodHandle", "RunnerResult | None"]):
    """``TaskVehicle`` for the Kubernetes runner (module tasks — shell tasks
    run through the mp task child with a ``build_shell_pod_spec`` argv)."""

    def __init__(self, spec: _PodSpec) -> None:
        self._spec = spec

    async def launch(self, task: Task, execution_worker_id: int) -> _PodHandle:
        env = build_runner_env()
        name = _pod_name(task.id, task.run_epoch)
        manifest = _build_pod_manifest(
            name=name,
            namespace=self._spec.namespace,
            image_tag=self._spec.image_tag,
            task_id=task.id,
            run_epoch=task.run_epoch,
            env=env,
            service_account=self._spec.service_account,
            image_pull_secret=self._spec.image_pull_secret,
            resources=self._spec.resources,
            entry_type=self._spec.entry_type,
            command=self._spec.command,
            command_env=self._spec.command_env,
        )
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(manifest, f)
            manifest_path = f.name
        try:
            await cli.run(_kubectl_bin(), "apply", "-f", manifest_path)
        finally:
            os.unlink(manifest_path)
        return _PodHandle(name, self._spec.namespace, task.id, task.job_id, task.run_epoch)

    async def wait(self, handle: _PodHandle, timeout: float | None) -> tuple[int, str | None, RunnerResult | None]:
        elapsed = 0.0
        error: str | None = None
        exit_code = -1
        while True:
            phase, exit_code = await _pod_status(handle)
            if phase in ("Succeeded", "Failed"):
                break
            if timeout is not None and elapsed >= timeout:
                error, exit_code = f"Task timed out after {timeout}s", -1
                break
            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
        result_row = await _read_task_run_result_row(handle.task_id, handle.run_epoch)
        return exit_code, error, result_row

    async def poll_cancelled(self, task: Task) -> bool:
        return await check_task_cancelled(task.id)

    async def terminate(self, handle: _PodHandle) -> None:
        await _kubectl_delete(handle)
        handle.deleted = True

    def collect(
        self, handle: _PodHandle, exit_code: int, error: str | None, was_cancelled: bool, payload: RunnerResult | None
    ) -> RunnerResult:
        if was_cancelled:
            return RunnerResult(False, None, "cancelled")
        if error is not None:
            return RunnerResult(False, None, error)
        if payload is None:
            return RunnerResult(False, None, f"pod exited with code {exit_code} but wrote no result row")
        return payload

    async def cleanup(self, handle: _PodHandle) -> None:
        if not handle.deleted:
            await _kubectl_delete(handle)


async def _run_task_in_pod(
    task: Task, execution_worker_id: int, dispatch: JobDispatch
) -> tuple[bool, dict | None, str | None]:
    """ExecuteFn for the Kubernetes runner."""
    if dispatch.image_source is None:
        raise ValueError(f"kubernetes task {task.id} has no image_source")
    image_tag = await resolve_launch_image(dispatch.image_source, dispatch.image_tag)
    spec = _pod_spec_from(task, dispatch._replace(image_tag=image_tag))
    timeout = parse_task_timeout()
    vehicle = _KubernetesVehicle(spec)
    result = await drive_vehicle(
        task,
        execution_worker_id,
        vehicle,
        timeout=timeout,
        poll_interval=POLL_INTERVAL,
        heartbeat_fn=execution_worker_heartbeat,
    )
    return result.success, result.result_ref, result.error


# ---------------------------------------------------------------------------
# Pod-side
# ---------------------------------------------------------------------------


async def _write_task_run_result(
    task_id: int, run_epoch: int, success: bool, result_ref: dict | None, error: str | None
) -> None:
    """Upsert the per-attempt result row the host reads back. Keyed by
    ``(task_id, run_epoch)`` so a re-run under a new epoch never collides."""
    async with get_sql_session() as session:
        existing = (
            await session.execute(
                select(RemoteTaskResult).where(
                    RemoteTaskResult.task_id == task_id, RemoteTaskResult.run_epoch == run_epoch
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            existing = RemoteTaskResult(task_id=task_id, run_epoch=run_epoch, success=success)
            session.add(existing)
        existing.success = success
        existing.result_ref = result_ref
        existing.error = error
        await session.commit()


async def _pod_main(task_id: int, run_epoch: int) -> int:
    """Entry point invoked inside the Pod. Runs the task via the shared
    ``execute_task`` path and writes a ``RemoteTaskResult`` row for the host."""
    from ..orch_context import orch_context

    success, result_ref, error = False, None, None
    exit_code = 0
    # orch_context wraps both execution and the result write — the latter needs
    # an active SQL session (unlike docker's result.json file write).
    async with orch_context():
        try:
            async with get_sql_session() as session:
                task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()
            data_result = await execute_task(task)
            result_ref = serialize_task_result(data_result, task.job_id)
            success = True
        except BaseException as e:
            success, error, exit_code = False, f"{type(e).__name__}: {e}", 1

        await _write_task_run_result(task_id, run_epoch, success, result_ref, error)
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
