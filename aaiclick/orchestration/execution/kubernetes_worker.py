"""Kubernetes runner — host-side ExecuteFns driving Pods.

See docs/designs/kubernetes_runner.md.
See docs/designs/orchestration.md — "Execution layers".

Mirrors ``docker_worker``: ``_run_task_in_pod`` drives a ``KubernetesVehicle``
via the shared ``drive_vehicle``. The Pod runs the shared container entrypoint
(``remote_result``), which writes a ``RemoteTaskResult`` row the host reads
back after the Pod exits.

``_run_shell_in_pod`` is the shell/jvm counterpart on the file data plane
(``io_plane``). A Pod has no filesystem the host can reach, in either
direction, so the shell Pod carries two helper containers sharing an
``emptyDir`` with the task: init container ``io-in`` waits for ``in.json``
(``kubectl cp`` puts it there), and sidecar ``io`` outlives the task so
``kubectl exec`` can read ``out.json`` back. The host waits on the *task*
container's state, since the Pod never ends on its own.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, NamedTuple

from ..models import Task
from . import cli
from .claiming import check_task_cancelled
from .docker_build import resolve_launch_image
from .execution_worker import (
    POLL_INTERVAL,
    JobDispatch,
    RunnerResult,
    ShellVehicle,
    TaskVehicle,
    drive_shell_vehicle,
    drive_vehicle,
    execution_worker_heartbeat,
    parse_task_timeout,
)
from .io_plane import CONTAINER_IO_DIR, IN_JSON, IO_DIR_ENV, OUT_JSON, parse_out_json
from .remote_result import REMOTE_ENTRYPOINT, collect_remote_result, read_task_run_result
from .runner import stage_in_json
from .runner_env import build_runner_env

IO_IMAGE_ENV = "AAICLICK_K8S_IO_IMAGE"
DEFAULT_IO_IMAGE = "busybox:1.36"
# Written after in.json: ``kubectl cp`` extracts in place, so the marker is
# what tells ``io-in`` the file is whole.
_IN_READY = "in.ready"
# A shell Pod never ends on its own (the ``io`` sidecar sleeps), so one a
# crashed worker orphans is bounded by ``activeDeadlineSeconds``: the task
# timeout plus slack when one is set, else this.
ORPHAN_POD_DEADLINE = 7 * 24 * 3600
ORPHAN_POD_SLACK = 60
# ``kubectl cp`` needs ``pods/exec``; a cluster without it must fail the task
# with kubectl's reason rather than park the Pod in Init forever.
COPY_IN_ATTEMPTS = 30
# Exit code the out.json read uses for "no such file", distinct from any
# failure of ``kubectl exec`` itself.
_NO_OUT_JSON_RC = 3
_IO_VOLUME = "aaiclick-io"
_IO_IN_CONTAINER = "io-in"
_IO_CONTAINER = "io"
_TASK_CONTAINER = "task"


def io_image() -> str:
    """The helper image for shell Pods; overridable for air-gapped clusters."""
    return os.environ.get(IO_IMAGE_ENV, DEFAULT_IO_IMAGE)


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
    defaults apply.

    Module Pods run the aaiclick shim with the full runner ``env``; shell and
    jvm Pods are built by :func:`build_shell_pod_manifest`."""
    container: dict = {
        "name": "task",
        "image": image_tag,
        "command": [*REMOTE_ENTRYPOINT, "--task-id", str(task_id), "--run-epoch", str(run_epoch)],
        "env": [{"name": k, "value": v} for k, v in env.items()],
    }
    if resources:
        container["resources"] = resources
    return _pod_scaffold(
        name=name,
        namespace=namespace,
        service_account=service_account,
        image_pull_secret=image_pull_secret,
        containers=[container],
    )


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


def _pod_spec_from(task: Task, dispatch: JobDispatch, image_tag: str) -> _PodSpec:
    pod = dispatch.pod_config
    if pod is None:
        raise RuntimeError("kubernetes dispatch without pod_config")
    return _PodSpec(
        image_tag=image_tag,
        namespace=pod.namespace,
        service_account=pod.service_account,
        image_pull_secret=pod.image_pull_secret,
        resources=dispatch.resources,
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
        # Set by ``wait`` once the Pod leaves Pending (or ``wait`` ends), so the
        # output follower needs no status polling of its own; ``phase`` is the
        # phase seen at that moment.
        self.started = asyncio.Event()
        self.phase = ""

    def mark_started(self, phase: str) -> None:
        if not self.started.is_set():
            self.phase = phase
            self.started.set()


async def _kubectl_delete(handle: _PodHandle) -> None:
    await cli.run(
        _kubectl_bin(), "delete", "pod", handle.name, "-n", handle.namespace, "--ignore-not-found", check=False
    )


# Phase reported by ``_pod_status`` when the API says the Pod no longer exists
# (deleted on cancellation, or evicted); never a real Kubernetes phase.
POD_NOT_FOUND = "NotFound"

# ``Pending``, or empty when ``kubectl`` could not answer.
_NOT_STARTED_PHASES = ("Pending", "")


async def _pod_status(handle: _PodHandle) -> tuple[str, int]:
    """One ``kubectl get`` returning ``(phase, container exit code)``. The exit
    code is ``-1`` until the container has terminated. A missing Pod reports
    ``POD_NOT_FOUND``; any other ``kubectl`` failure reports an empty phase so
    the caller retries."""
    rc, out, err = await cli.run(
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
    if rc != 0:
        return (POD_NOT_FOUND if "NotFound" in err else ""), -1
    parts = out.split()
    phase = parts[0] if parts else ""
    try:
        exit_code = int(parts[1]) if len(parts) > 1 else -1
    except ValueError:
        exit_code = -1
    return phase, exit_code


def _build_command_env_secret(name: str, namespace: str, command_env: dict[str, str]) -> dict:
    """Manifest of the per-attempt Secret a shell Pod reads ``command_env``
    from. ``stringData`` carries the values verbatim (no env-file format
    limits), and the Secret shares the Pod's name so ``cleanup_argv`` deletes
    both in one call."""
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": namespace},
        "type": "Opaque",
        "stringData": command_env,
    }


async def _kubectl_create_from_file(manifest: dict, verb: str = "create") -> None:
    """``kubectl <verb> -f`` a manifest through a private (0600) temp file —
    never the argv — removed once kubectl returns."""
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(manifest, f)
        manifest_path = f.name
    try:
        await cli.run(_kubectl_bin(), verb, "-f", manifest_path)
    finally:
        os.unlink(manifest_path)


def _pod_scaffold(
    *, name: str, namespace: str, service_account: str | None, image_pull_secret: str | None, containers: list[dict]
) -> dict:
    spec: dict = {"restartPolicy": "Never", "containers": containers}
    if service_account:
        spec["serviceAccountName"] = service_account
    if image_pull_secret:
        spec["imagePullSecrets"] = [{"name": image_pull_secret}]
    return {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": namespace}, "spec": spec}


def build_shell_pod_manifest(
    *,
    name: str,
    namespace: str,
    image_tag: str,
    command: list[str],
    service_account: str | None,
    image_pull_secret: str | None,
    resources: dict | None,
    command_env_secret: str | None,
    active_deadline: int,
) -> dict:
    """Manifest of a shell/jvm Pod on the file data plane.

    ``task`` runs the user's argv with only ``command_env`` (from the
    per-attempt Secret, ``envFrom``) and ``AAICLICK_IO_DIR`` — never the
    runner env, so no aaiclick secrets reach a vanilla image. The argv goes
    through ``args`` so it follows the image's ENTRYPOINT — as ``docker create
    <image> <argv>`` does, and what the jvm shim ENTRYPOINT relies on. Init
    container ``io-in`` holds the Pod
    until the host has copied ``in.json`` in; sidecar ``io`` keeps the Pod
    alive so ``out.json`` can be read after ``task`` exits, bounded by
    ``activeDeadlineSeconds``. All three share one ``emptyDir``; the helpers
    request no resources."""
    mount = [{"name": _IO_VOLUME, "mountPath": CONTAINER_IO_DIR}]
    task: dict = {
        "name": _TASK_CONTAINER,
        "image": image_tag,
        "args": command,
        "env": [{"name": IO_DIR_ENV, "value": CONTAINER_IO_DIR}],
        "volumeMounts": mount,
    }
    if command_env_secret:
        task["envFrom"] = [{"secretRef": {"name": command_env_secret}}]
    if resources:
        task["resources"] = resources
    io_in = {
        "name": _IO_IN_CONTAINER,
        "image": io_image(),
        "command": ["sh", "-c", f"until [ -f {CONTAINER_IO_DIR}/{_IN_READY} ]; do sleep 0.2; done"],
        "volumeMounts": mount,
    }
    io = {"name": _IO_CONTAINER, "image": io_image(), "command": ["sleep", "infinity"], "volumeMounts": mount}
    manifest = _pod_scaffold(
        name=name,
        namespace=namespace,
        service_account=service_account,
        image_pull_secret=image_pull_secret,
        containers=[task, io],
    )
    manifest["spec"]["initContainers"] = [io_in]
    manifest["spec"]["volumes"] = [{"name": _IO_VOLUME, "emptyDir": {}}]
    manifest["spec"]["activeDeadlineSeconds"] = active_deadline
    return manifest


class _ShellPodHandle(_PodHandle):
    """A shell Pod's handle: the host-side IO dir and whether ``in.json`` has
    reached the Pod yet, plus the per-attempt Secret to delete with it."""

    def __init__(self, name: str, namespace: str, task_id: int, job_id: int, run_epoch: int, io_dir: Path) -> None:
        super().__init__(name, namespace, task_id, job_id, run_epoch)
        self.io_dir = io_dir
        self.in_copied = False
        self.copy_failures = 0
        self.copy_error = ""


def _container_state(pod: dict, container: str, *, init: bool = False) -> tuple[str, int]:
    """``(state, exit_code)`` of one container from a Pod's JSON status:
    ``"running"`` / ``"terminated"`` / ``"waiting"`` / ``""`` (not reported),
    exit code ``-1`` until terminated."""
    key = "initContainerStatuses" if init else "containerStatuses"
    for status in pod.get("status", {}).get(key, []):
        if status.get("name") != container:
            continue
        state = status.get("state", {})
        if "terminated" in state:
            return "terminated", int(state["terminated"].get("exitCode", -1))
        return next((k for k in ("running", "waiting") if k in state), ""), -1
    return "", -1


async def _pod_json(handle: _PodHandle) -> dict | None:
    """One ``kubectl get -o json``; ``None`` when the Pod is gone, ``{}`` when
    ``kubectl`` could not answer (the caller retries)."""
    rc, out, err = await cli.run(
        _kubectl_bin(), "get", "pod", handle.name, "-n", handle.namespace, "-o", "json", check=False, stream=False
    )
    if rc != 0:
        return None if "NotFound" in err else {}
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return {}


async def _kubectl_delete_shell(handle: _ShellPodHandle) -> None:
    """Delete the Pod and its per-attempt Secret (absent without ``command_env``)."""
    resources = [f"pod/{handle.name}", f"secret/{handle.name}"]
    await cli.run(_kubectl_bin(), "delete", *resources, "-n", handle.namespace, "--ignore-not-found", check=False)


class _KubernetesShellVehicle(ShellVehicle["_ShellPodHandle"]):
    """Shell and jvm tasks on the Kubernetes runner — the file data plane
    through the ``io-in`` / ``io`` helpers (see the module docstring)."""

    def __init__(self, spec: _PodSpec, timeout: float | None) -> None:
        self._spec = spec
        self._deadline = int(timeout + ORPHAN_POD_SLACK) if timeout is not None else ORPHAN_POD_DEADLINE

    async def launch(self, task: Task, execution_worker_id: int) -> _ShellPodHandle:
        name = _pod_name(task.id, task.run_epoch)
        io_dir = await stage_in_json(task)
        handle = _ShellPodHandle(name, self._spec.namespace, task.id, task.job_id, task.run_epoch, io_dir)
        try:
            (io_dir / _IN_READY).touch()
            if self._spec.command_env:
                await _kubectl_create_from_file(
                    _build_command_env_secret(name, self._spec.namespace, self._spec.command_env)
                )
            manifest = build_shell_pod_manifest(
                name=name,
                namespace=self._spec.namespace,
                image_tag=self._spec.image_tag,
                command=self._spec.command or [],
                service_account=self._spec.service_account,
                image_pull_secret=self._spec.image_pull_secret,
                resources=self._spec.resources,
                command_env_secret=name if self._spec.command_env else None,
                active_deadline=self._deadline,
            )
            await _kubectl_create_from_file(manifest, "apply")
        except BaseException:
            await self.cleanup(handle)
            raise
        return handle

    async def _copy_in(self, handle: _ShellPodHandle) -> None:
        """``kubectl cp`` in.json, then the ready marker. A failure is retried
        on the next poll, up to ``COPY_IN_ATTEMPTS``."""
        for name in (IN_JSON, _IN_READY):
            rc, _, err = await cli.run(
                _kubectl_bin(),
                "cp",
                str(handle.io_dir / name),
                f"{handle.namespace}/{handle.name}:{CONTAINER_IO_DIR}/{name}",
                "-c",
                _IO_IN_CONTAINER,
                check=False,
                stream=False,
            )
            if rc != 0:
                handle.copy_failures += 1
                handle.copy_error = err.strip()
                return
        handle.in_copied = True

    async def wait(self, handle: _ShellPodHandle, timeout: float | None) -> tuple[int, str | None, Any]:
        """Poll the Pod: copy ``in.json`` once ``io-in`` runs, then wait for
        the ``task`` container to terminate. Both legs count against
        ``timeout``; a Pod that vanishes (``terminate`` on cancellation,
        eviction) ends the wait."""
        elapsed = 0.0
        while True:
            pod = await _pod_json(handle)
            if pod is None:
                handle.mark_started(POD_NOT_FOUND)
                return -1, f"Pod {handle.name} disappeared", None
            if not handle.in_copied and _container_state(pod, _IO_IN_CONTAINER, init=True)[0] == "running":
                await self._copy_in(handle)
                if handle.copy_failures >= COPY_IN_ATTEMPTS:
                    handle.mark_started(pod.get("status", {}).get("phase", ""))
                    return -1, f"kubectl cp of in.json failed {handle.copy_failures} times: {handle.copy_error}", None
            state, exit_code = _container_state(pod, _TASK_CONTAINER)
            phase = pod.get("status", {}).get("phase", "")
            if state in ("running", "terminated"):
                handle.mark_started(phase or "Running")
            if state == "terminated":
                break
            if phase in ("Succeeded", "Failed"):
                # Helper image pull failure, activeDeadlineSeconds, eviction:
                # the task container never ran and never will.
                handle.mark_started(phase)
                return -1, f"Pod {handle.name} {phase} before the task container terminated", None
            if timeout is not None and elapsed >= timeout:
                handle.mark_started(pod.get("status", {}).get("phase", ""))
                return -1, f"Task timed out after {timeout}s", None
            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
        if exit_code != 0:
            return exit_code, None, None
        out_path = f"{CONTAINER_IO_DIR}/{OUT_JSON}"
        rc, out, err = await cli.run(
            _kubectl_bin(),
            "exec",
            handle.name,
            "-n",
            handle.namespace,
            "-c",
            _IO_CONTAINER,
            "--",
            "sh",
            "-c",
            f"if [ -f {out_path} ]; then cat {out_path}; else exit {_NO_OUT_JSON_RC}; fi",
            check=False,
            stream=False,
        )
        if rc == _NO_OUT_JSON_RC:
            return exit_code, None, None  # no out.json: no result
        if rc != 0:
            return exit_code, f"reading {OUT_JSON} failed: {err.strip() or out.strip()}", None
        try:
            return exit_code, None, parse_out_json(out)
        except ValueError as e:
            return exit_code, str(e), None

    async def terminate(self, handle: _ShellPodHandle) -> None:
        await _kubectl_delete_shell(handle)
        handle.deleted = True

    async def output_argv(self, handle: _ShellPodHandle) -> list[str] | None:
        await handle.started.wait()
        if handle.deleted or handle.phase in (*_NOT_STARTED_PHASES, POD_NOT_FOUND):
            return None
        return [_kubectl_bin(), "logs", "--follow", handle.name, "-n", handle.namespace, "-c", _TASK_CONTAINER]

    async def cleanup(self, handle: _ShellPodHandle) -> None:
        if not handle.deleted:
            await _kubectl_delete_shell(handle)
        shutil.rmtree(handle.io_dir, ignore_errors=True)


async def _run_shell_in_pod(
    task: Task, execution_worker_id: int, dispatch: JobDispatch, log_run_id: int | None
) -> tuple[bool, dict | None, str | None]:
    """ExecuteFn for shell and jvm tasks on the Kubernetes runner."""
    image_tag = await resolve_launch_image(dispatch.image_source, task_id=task.id)
    timeout = parse_task_timeout()
    vehicle = _KubernetesShellVehicle(_pod_spec_from(task, dispatch, image_tag), timeout)
    return await drive_shell_vehicle(task, execution_worker_id, vehicle, timeout=timeout, log_run_id=log_run_id)


class _KubernetesVehicle(TaskVehicle["_PodHandle", "RunnerResult | None"]):
    """``TaskVehicle`` for module tasks on the Kubernetes runner (shell and
    jvm tasks use :class:`_KubernetesShellVehicle`)."""

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
        )
        await _kubectl_create_from_file(manifest, "apply")
        return _PodHandle(name, self._spec.namespace, task.id, task.job_id, task.run_epoch)

    async def wait(self, handle: _PodHandle, timeout: float | None) -> tuple[int, str | None, RunnerResult | None]:
        """Poll the Pod until it reaches a terminal phase.

        A Pod that vanishes — deleted by ``terminate`` on cancellation, or
        evicted — never reports Succeeded/Failed, so ``POD_NOT_FOUND`` ends the
        wait too; otherwise a cancelled task with no timeout would park the
        worker here forever."""
        elapsed = 0.0
        error: str | None = None
        exit_code = -1
        while True:
            phase, exit_code = await _pod_status(handle)
            if phase not in _NOT_STARTED_PHASES:
                handle.mark_started(phase)
            if phase in ("Succeeded", "Failed"):
                break
            if phase == POD_NOT_FOUND:
                error, exit_code = f"Pod {handle.name} disappeared", -1
                break
            if timeout is not None and elapsed >= timeout:
                error, exit_code = f"Task timed out after {timeout}s", -1
                break
            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
        handle.mark_started(phase)  # release a follower still waiting on a Pod that never ran
        result_row = await read_task_run_result(handle.task_id, handle.run_epoch)
        return exit_code, error, result_row

    async def poll_cancelled(self, task: Task) -> bool:
        return await check_task_cancelled(task.id)

    async def terminate(self, handle: _PodHandle) -> None:
        await _kubectl_delete(handle)
        handle.deleted = True

    def collect(
        self, handle: _PodHandle, exit_code: int, error: str | None, was_cancelled: bool, payload: RunnerResult | None
    ) -> RunnerResult:
        return collect_remote_result(exit_code, error, was_cancelled, payload, "pod")

    async def output_argv(self, handle: _PodHandle) -> list[str] | None:
        """``kubectl logs -f`` fails on a ``Pending`` Pod (image pull,
        scheduling), so wait until ``wait`` sees it start; a Pod that never ran
        has nothing to follow, nor does one ``terminate`` already deleted. The
        log merges stdout and stderr, so every line lands as stdout."""
        await handle.started.wait()
        if handle.deleted or handle.phase in (*_NOT_STARTED_PHASES, POD_NOT_FOUND):
            return None
        return [_kubectl_bin(), "logs", "--follow", handle.name, "-n", handle.namespace]

    async def cleanup(self, handle: _PodHandle) -> None:
        if handle.deleted:
            return
        await _kubectl_delete(handle)


async def _run_task_in_pod(
    task: Task, execution_worker_id: int, dispatch: JobDispatch, log_run_id: int | None
) -> tuple[bool, dict | None, str | None]:
    """ExecuteFn for the Kubernetes runner."""
    image_tag = await resolve_launch_image(dispatch.image_source, task_id=task.id)
    spec = _pod_spec_from(task, dispatch, image_tag)
    timeout = parse_task_timeout()
    vehicle = _KubernetesVehicle(spec)
    result = await drive_vehicle(
        task,
        execution_worker_id,
        vehicle,
        timeout=timeout,
        poll_interval=POLL_INTERVAL,
        heartbeat_fn=execution_worker_heartbeat,
        log_run_id=log_run_id,
    )
    return result.success, result.result_ref, result.error
