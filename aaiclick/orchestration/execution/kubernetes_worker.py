"""Kubernetes runner — the Pod launchers and the watch-driven task coroutines.

See docs/designs/kubernetes_runner.md.
See docs/designs/orchestration.md — "Execution layers".

The concurrent worker (``k8s_worker_loop``) runs :func:`run_module_pod` /
:func:`run_shell_pod` once per claimed task; both await their Pod's stage
futures from the shared ``PodWatch`` instead of polling. A module Pod runs the
shared container entrypoint (``remote_result``), which writes a
``RemoteTaskResult`` row the host reads back once the ``task`` container exits.

Shell and jvm tasks ride the file data plane (``io_plane``). A Pod has no
filesystem the host can reach, in either direction, so the shell Pod carries
two helper containers sharing an ``emptyDir`` with the task: init container
``io-in`` waits for ``in.json`` (``kubectl cp`` puts it there), and sidecar
``io`` outlives the task so ``kubectl exec`` can read ``out.json`` back. The
*task* container's exit is what ends the run, since the Pod never ends on its
own.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NamedTuple

from ..kubernetes_config import KubernetesConfig
from ..logging import last_task_log_time
from ..models import Task
from . import cli
from .execution_worker import (
    POLL_INTERVAL,
    RunnerResult,
    finish_shell_outcome,
    parse_task_timeout,
)
from .io_plane import CONTAINER_IO_DIR, IN_JSON, IO_DIR_ENV, OUT_JSON, parse_out_json
from .pod_watch import PodWatch, pod_labels
from .remote_result import REMOTE_ENTRYPOINT, collect_remote_result, read_task_run_result
from .runner import (
    OUTPUT_FOLLOWER_DRAIN_TIMEOUT,
    follow_vehicle_output,
    register_run,
    stage_in_json,
    stop_output_follower,
)
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
    job_id: int,
    run_epoch: int,
    env_secret: str,
    service_account: str | None,
    image_pull_secret: str | None,
    resources: dict | None,
) -> dict:
    """Build the bare-Pod manifest (``restartPolicy: Never`` — aaiclick owns
    retries). Optional cluster fields are omitted when unset so the cluster
    defaults apply.

    Module Pods run the aaiclick shim with the full runner env, read from the
    per-attempt Secret ``env_secret`` so the database URLs (passwords included)
    never appear in the Pod spec. Shell and jvm Pods are built by
    :func:`build_shell_pod_manifest`."""
    container: dict = {
        "name": "task",
        "image": image_tag,
        "command": [*REMOTE_ENTRYPOINT, "--task-id", str(task_id), "--run-epoch", str(run_epoch)],
        "envFrom": [{"secretRef": {"name": env_secret}}],
    }
    if resources:
        container["resources"] = resources
    return _pod_scaffold(
        name=name,
        namespace=namespace,
        labels=pod_labels(task_id, job_id, run_epoch),
        service_account=service_account,
        image_pull_secret=image_pull_secret,
        containers=[container],
    )


# ---------------------------------------------------------------------------
# Host-side
# ---------------------------------------------------------------------------


class _PodSpec(NamedTuple):
    image_tag: str
    job_id: int
    namespace: str
    service_account: str | None
    image_pull_secret: str | None
    resources: dict | None
    entry_type: str
    command: list[str] | None
    command_env: dict[str, str] | None


class _PodHandle:
    """Pod identity + a ``deleted`` latch so ``cleanup`` doesn't re-delete a
    Pod already removed."""

    def __init__(self, name: str, namespace: str, task_id: int, job_id: int, run_epoch: int) -> None:
        self.name = name
        self.namespace = namespace
        self.task_id = task_id
        self.job_id = job_id
        self.run_epoch = run_epoch
        self.deleted = False
        # The Pod as ``kubectl apply -o json`` returned it, to seed the watch.
        self.created: dict = {}


async def _kubectl_delete(name: str, namespace: str) -> None:
    """Delete an attempt's Pod and its same-named env Secret. ``--wait=false``:
    the watch reports when the Pod is gone, and waiting through graceful
    termination inside the worker's timers would stall its heartbeats."""
    resources = [f"pod/{name}", f"secret/{name}"]
    await cli.run(
        _kubectl_bin(), "delete", *resources, "-n", namespace, "--ignore-not-found", "--wait=false", check=False
    )


async def shielded(coro):
    """Run a cleanup or DB write to completion even if the caller is cancelled
    meanwhile: a half-done ``kubectl delete`` leaks the Pod, a half-done
    transaction holds locks."""
    future = asyncio.ensure_future(coro)
    try:
        return await asyncio.shield(future)
    except asyncio.CancelledError:
        return await future


def _build_env_secret(
    name: str, namespace: str, env: dict[str, str], *, labels: dict[str, str], owner_uid: str
) -> dict:
    """Manifest of the per-attempt Secret a Pod reads its env from
    (``envFrom``). ``stringData`` carries the values verbatim (no env-file
    format limits). The Secret is owned by its Pod (``ownerReferences`` on the
    Pod's uid), so the garbage collector removes it however the Pod goes; the
    worker never needs to list Secrets."""
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels,
            "ownerReferences": [{"apiVersion": "v1", "kind": "Pod", "name": name, "uid": owner_uid}],
        },
        "type": "Opaque",
        "stringData": env,
    }


async def _kubectl_create_from_file(manifest: dict, verb: str = "create") -> dict:
    """``kubectl <verb> -f`` a manifest through a private (0600) temp file —
    never the argv — removed once kubectl returns. Returns the created object
    as kubectl reports it (``-o json``), so a Pod's uid can own its Secret."""
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(manifest, f)
        manifest_path = f.name
    try:
        _, out, _ = await cli.run(_kubectl_bin(), verb, "-f", manifest_path, "-o", "json", stream=False)
    finally:
        os.unlink(manifest_path)
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return {}


def _uid(created: dict) -> str:
    return created.get("metadata", {}).get("uid", "")


def _pod_scaffold(
    *,
    name: str,
    namespace: str,
    labels: dict[str, str],
    service_account: str | None,
    image_pull_secret: str | None,
    containers: list[dict],
) -> dict:
    spec: dict = {"restartPolicy": "Never", "containers": containers}
    if service_account:
        spec["serviceAccountName"] = service_account
    if image_pull_secret:
        spec["imagePullSecrets"] = [{"name": image_pull_secret}]
    metadata = {"name": name, "namespace": namespace, "labels": labels}
    return {"apiVersion": "v1", "kind": "Pod", "metadata": metadata, "spec": spec}


def build_shell_pod_manifest(
    *,
    name: str,
    namespace: str,
    image_tag: str,
    task_id: int,
    job_id: int,
    run_epoch: int,
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
        labels=pod_labels(task_id, job_id, run_epoch),
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


async def _read_out_json(name: str, namespace: str) -> tuple[str | None, Any]:
    """``(error, value)`` of the Pod's ``out.json`` read through the ``io``
    sidecar; ``(None, None)`` when the task wrote none."""
    out_path = f"{CONTAINER_IO_DIR}/{OUT_JSON}"
    rc, out, err = await cli.run(
        _kubectl_bin(),
        "exec",
        name,
        "-n",
        namespace,
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
        return None, None
    if rc != 0:
        return f"reading {OUT_JSON} failed: {err.strip() or out.strip()}", None
    try:
        return None, parse_out_json(out)
    except ValueError as e:
        return str(e), None


class _KubernetesShellVehicle:
    """Launcher of a shell/jvm Pod on the file data plane (``io-in`` / ``io``
    helpers, see the module docstring); :func:`run_shell_pod` drives it."""

    def __init__(self, spec: _PodSpec, timeout: float | None) -> None:
        self._spec = spec
        self._deadline = int(timeout + ORPHAN_POD_SLACK) if timeout is not None else ORPHAN_POD_DEADLINE

    async def launch(self, task: Task, execution_worker_id: int) -> _ShellPodHandle:
        name = _pod_name(task.id, task.run_epoch)
        io_dir = await stage_in_json(task)
        handle = _ShellPodHandle(name, self._spec.namespace, task.id, task.job_id, task.run_epoch, io_dir)
        try:
            (io_dir / _IN_READY).touch()
            manifest = build_shell_pod_manifest(
                name=name,
                namespace=self._spec.namespace,
                image_tag=self._spec.image_tag,
                task_id=task.id,
                job_id=task.job_id,
                run_epoch=task.run_epoch,
                command=self._spec.command or [],
                service_account=self._spec.service_account,
                image_pull_secret=self._spec.image_pull_secret,
                resources=self._spec.resources,
                command_env_secret=name if self._spec.command_env else None,
                active_deadline=self._deadline,
            )
            # Pod first: the Secret is owned by the Pod's uid, so it can never outlive it.
            pod = await _kubectl_create_from_file(manifest, "apply")
            handle.created = pod
            if self._spec.command_env:
                labels = pod_labels(task.id, task.job_id, task.run_epoch)
                secret = _build_env_secret(
                    name, self._spec.namespace, self._spec.command_env, labels=labels, owner_uid=_uid(pod)
                )
                await _kubectl_create_from_file(secret)
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

    async def cleanup(self, handle: _ShellPodHandle) -> None:
        if not handle.deleted:
            await _kubectl_delete(handle.name, handle.namespace)
        shutil.rmtree(handle.io_dir, ignore_errors=True)


class _KubernetesVehicle:
    """Launcher of a module Pod; :func:`run_module_pod` drives it."""

    def __init__(self, spec: _PodSpec) -> None:
        self._spec = spec

    async def launch(self, task: Task, execution_worker_id: int) -> _PodHandle:
        name = _pod_name(task.id, task.run_epoch)
        labels = pod_labels(task.id, task.job_id, task.run_epoch)
        try:
            manifest = _build_pod_manifest(
                name=name,
                namespace=self._spec.namespace,
                image_tag=self._spec.image_tag,
                task_id=task.id,
                job_id=task.job_id,
                run_epoch=task.run_epoch,
                env_secret=name,
                service_account=self._spec.service_account,
                image_pull_secret=self._spec.image_pull_secret,
                resources=self._spec.resources,
            )
            # Pod first: the Secret is owned by the Pod's uid, so it can never outlive it.
            pod = await _kubectl_create_from_file(manifest, "apply")
            secret = _build_env_secret(
                name, self._spec.namespace, build_runner_env(), labels=labels, owner_uid=_uid(pod)
            )
            await _kubectl_create_from_file(secret)
        except BaseException:
            await _kubectl_delete(name, self._spec.namespace)
            raise
        handle = _PodHandle(name, self._spec.namespace, task.id, task.job_id, task.run_epoch)
        handle.created = pod
        return handle


# ---------------------------------------------------------------------------
# Watch-driven coroutines (the concurrent kubernetes worker)
# ---------------------------------------------------------------------------


class QuotaExceeded(Exception):
    """``kubectl apply`` refused the Pod because of a ``ResourceQuota``; the
    claim goes back to the queue and the worker backs off."""


def _cancel_reason(exc: asyncio.CancelledError) -> str:
    """The message ``Task.cancel(msg)`` carried — a deadline reason — or
    ``"cancelled"`` for a plain cancellation (cancel_job / clear_task)."""
    return str(exc.args[0]) if exc.args and exc.args[0] else "cancelled"


async def run_module_pod(
    task: Task,
    execution_worker_id: int,
    pod_config: KubernetesConfig,
    watch: PodWatch,
    *,
    image_tag: str | None,
    resources: dict | None,
    detach: list[bool],
) -> RunnerResult:
    """Run a module task as a Pod, driven by the shared watch: create the Pod
    (``image_tag`` None ⇒ adopted, already running), await its ``done``
    future, read the ``remote_task_results`` row, delete the Pod.

    The reconciler enforces deadlines and cancellation by cancelling this
    coroutine; the cancel message is the failure reason. ``detach[0]`` set
    before the cancel means the worker is letting go of the Pod (shutdown,
    ownership lost to adoption): nothing is deleted and the cancellation
    propagates."""
    name = _pod_name(task.id, task.run_epoch)
    futures = watch.expect(name)
    try:
        if image_tag is not None:
            spec = _PodSpec(
                image_tag=image_tag,
                job_id=task.job_id,
                namespace=pod_config.namespace,
                service_account=pod_config.service_account,
                image_pull_secret=pod_config.image_pull_secret,
                resources=resources,
                entry_type="module",
                command=None,
                command_env=None,
            )
            try:
                handle = await _KubernetesVehicle(spec).launch(task, execution_worker_id)
            except cli.CommandError as e:
                if "exceeded quota" in e.stderr:
                    raise QuotaExceeded(e.stderr.strip()) from e
                raise
            watch.apply(handle.created, deleted=False)  # known to the gate and reclaim at once
        state = await futures.done
        if state.gone:
            return RunnerResult(False, None, f"Pod {name} disappeared")
        row = await read_task_run_result(task.id, task.run_epoch)
        return collect_remote_result(state.task.exit_code, None, False, row, "pod")
    except asyncio.CancelledError as e:
        if detach[0]:
            raise
        return RunnerResult(False, None, _cancel_reason(e))
    finally:
        watch.forget(name, futures)
        if not detach[0]:
            await shielded(_kubectl_delete(name, pod_config.namespace))


def _since_time(moment: datetime) -> str:
    """``--since-time`` value: RFC 3339 in UTC, second precision (kubectl
    rejects fractional offsets on some versions)."""
    return moment.astimezone(UTC).strftime("--since-time=%Y-%m-%dT%H:%M:%SZ")


async def run_shell_pod(
    task: Task,
    execution_worker_id: int,
    pod_config: KubernetesConfig,
    watch: PodWatch,
    *,
    image_tag: str | None,
    command: list[str] | None,
    command_env: dict[str, str] | None,
    resources: dict | None,
    run_id: int | None,
    detach: list[bool],
) -> RunnerResult:
    """Run a shell/jvm task as a Pod on the file data plane, driven by the
    shared watch, stage by stage: ``io-in`` running → copy ``in.json`` in;
    ``task`` running → register the run (unless ``run_id`` names an existing
    one) and follow its logs; done → read ``out.json``. ``image_tag`` None ⇒
    adopted: no launch, resume at whatever stage the Pod is in — ``in.json``
    is re-staged from the DB, the follower resumes after the last stored line.
    Cancellation and ``detach`` work as in :func:`run_module_pod`."""
    name = _pod_name(task.id, task.run_epoch)
    namespace = pod_config.namespace
    futures = watch.expect(name)
    spec = _PodSpec(
        image_tag=image_tag or "",
        job_id=task.job_id,
        namespace=namespace,
        service_account=pod_config.service_account,
        image_pull_secret=pod_config.image_pull_secret,
        resources=resources,
        entry_type=task.entry_type,
        command=command,
        command_env=command_env,
    )
    vehicle = _KubernetesShellVehicle(spec, parse_task_timeout())
    handle: _ShellPodHandle | None = None
    follower: asyncio.Task[None] | None = None
    try:
        if image_tag is not None:
            try:
                handle = await vehicle.launch(task, execution_worker_id)
            except cli.CommandError as e:
                if "exceeded quota" in e.stderr:
                    raise QuotaExceeded(e.stderr.strip()) from e
                raise
            watch.apply(handle.created, deleted=False)
        else:
            io_dir = await stage_in_json(task)
            (io_dir / _IN_READY).touch()
            handle = _ShellPodHandle(name, namespace, task.id, task.job_id, task.run_epoch, io_dir)

        state = await futures.io_in_running
        if state.io_in.state == "running":
            while not handle.in_copied and handle.copy_failures < COPY_IN_ATTEMPTS:
                await vehicle._copy_in(handle)
                if not handle.in_copied:
                    await asyncio.sleep(POLL_INTERVAL)
            if not handle.in_copied:
                return RunnerResult(
                    False, None, f"kubectl cp of in.json failed {handle.copy_failures} times: {handle.copy_error}"
                )

        await futures.task_running
        since = await last_task_log_time(task.id, run_id) if run_id is not None else None
        if run_id is None:
            run_id = await register_run(task.id)
        argv = [_kubectl_bin(), "logs", "--follow", name, "-n", namespace, "-c", _TASK_CONTAINER]
        if since is not None:
            argv.append(_since_time(since))
        follower = asyncio.create_task(follow_vehicle_output(argv, task.id, task.job_id, run_id))

        state = await futures.done
        if state.gone:
            return RunnerResult(False, None, f"Pod {name} disappeared")
        if state.task.exit_code != 0:
            return RunnerResult(False, None, f"exit {state.task.exit_code}")
        error, out_value = await _read_out_json(name, namespace)
        if error is not None:
            return RunnerResult(False, None, error)
        return await finish_shell_outcome(task, out_value)
    except asyncio.CancelledError as e:
        if detach[0]:
            raise
        return RunnerResult(False, None, _cancel_reason(e))
    finally:
        watch.forget(name, futures)
        await shielded(stop_output_follower(follower, 0.0 if detach[0] else OUTPUT_FOLLOWER_DRAIN_TIMEOUT))
        if handle is not None:
            shutil.rmtree(handle.io_dir, ignore_errors=True)
        if not detach[0]:
            await shielded(_kubectl_delete(name, namespace))
