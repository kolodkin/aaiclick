"""Tests for the Kubernetes runner — manifest builder and collect logic.

The container-side entrypoint tests live in ``test_remote_result.py`` (its
own module) because they boot a chdb ``orch_context()``; see the chdb
single-session constraint in ``docs/designs/testing.md``."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from aaiclick.data.object.refs import native_ref

from ..kubernetes_config import KubernetesConfig
from ..logging import read_task_logs
from ..models import TASK_COMPLETED, TASK_PENDING_FAILURE_CLEANUP, Task
from ..runner_config import ENTRY_JVM, ImagePrebuilt
from . import kubernetes_worker as kw
from .execution_worker import JobDispatch
from .kubernetes_worker import build_shell_pod_manifest
from .log_test_helpers import dispatch_with_fake_cli
from .runner_env import RUNNER_KUBERNETES


def test_build_pod_manifest_shape():
    m = kw._build_pod_manifest(
        name="aaiclick-task-7-2",
        namespace="ml",
        image_tag="reg/aaiclick-job:abc",
        task_id=7,
        run_epoch=2,
        env_secret="p",
        service_account="sa",
        image_pull_secret="regcred",
        resources={"limits": {"cpu": "1"}},
    )
    assert m["kind"] == "Pod"
    assert m["metadata"] == {"name": "aaiclick-task-7-2", "namespace": "ml"}
    spec = m["spec"]
    assert spec["restartPolicy"] == "Never"
    assert spec["serviceAccountName"] == "sa"
    assert spec["imagePullSecrets"] == [{"name": "regcred"}]
    c = spec["containers"][0]
    assert c["image"] == "reg/aaiclick-job:abc"
    assert c["resources"] == {"limits": {"cpu": "1"}}
    assert c["envFrom"] == [{"secretRef": {"name": "p"}}]
    assert "env" not in c
    assert c["command"][-4:] == ["--task-id", "7", "--run-epoch", "2"]


def test_build_pod_manifest_omits_optional_fields():
    m = kw._build_pod_manifest(
        name="n",
        namespace="default",
        image_tag="img",
        task_id=1,
        run_epoch=0,
        env_secret="n",
        service_account=None,
        image_pull_secret=None,
        resources=None,
    )
    spec = m["spec"]
    assert "serviceAccountName" not in spec
    assert "imagePullSecrets" not in spec
    assert "resources" not in spec["containers"][0]


def _handle(task_id=7, run_epoch=1):
    return kw._PodHandle(
        name="aaiclick-task-7-1",
        namespace="default",
        task_id=task_id,
        job_id=1,
        run_epoch=run_epoch,
    )


def _vehicle(entry_type="module"):
    v = kw._KubernetesVehicle.__new__(kw._KubernetesVehicle)
    v._spec = kw._PodSpec(
        image_tag="img",
        namespace="default",
        service_account=None,
        image_pull_secret=None,
        resources=None,
        entry_type=entry_type,
        command=None,
        command_env=None,
    )
    return v


def _collect(handle, exit_code, error, was_cancelled, payload, entry_type="module"):
    return _vehicle(entry_type).collect(handle, exit_code, error, was_cancelled, payload)


def test_collect_cancelled_overrides_row():
    payload = kw.RunnerResult(True, {"x": 1}, None)
    out = _collect(_handle(), 137, None, was_cancelled=True, payload=payload)
    assert out.success is False and out.error == "cancelled"


def test_collect_synthesizes_failure_when_row_missing():
    out = _collect(_handle(), 1, None, was_cancelled=False, payload=None)
    assert out.success is False
    assert "no result" in (out.error or "")


def test_collect_returns_row():
    payload = kw.RunnerResult(True, native_ref(5), None)
    out = _collect(_handle(), 0, None, was_cancelled=False, payload=payload)
    assert out.success is True and out.result_ref == native_ref(5)


async def test_module_pod_keeps_runner_env_out_of_the_pod_spec(monkeypatch):
    """The runner env carries database passwords; anyone who can ``get pods``
    must not read them, so they travel in a per-attempt Secret."""
    monkeypatch.setattr(kw, "build_runner_env", lambda: {"AAICLICK_SQL_URL": "postgresql://u:hunter2@db/x"})
    created: list[dict] = []
    monkeypatch.setattr(
        kw, "_kubectl_create_from_file", AsyncMock(side_effect=lambda m, verb="create": created.append(m))
    )

    await _vehicle().launch(Task(id=7, job_id=1, entrypoint="m.f", name="t", run_epoch=2), execution_worker_id=1)

    secret, pod = created
    assert secret["kind"] == "Secret" and secret["stringData"] == {"AAICLICK_SQL_URL": "postgresql://u:hunter2@db/x"}
    assert pod["kind"] == "Pod" and "hunter2" not in json.dumps(pod)
    assert pod["spec"]["containers"][0]["envFrom"] == [{"secretRef": {"name": secret["metadata"]["name"]}}]


async def test_module_pod_wait_reads_result_row(monkeypatch):
    monkeypatch.setattr(kw, "_pod_status", AsyncMock(return_value=("Succeeded", 0)))
    row = kw.RunnerResult(True, None, None)
    monkeypatch.setattr(kw, "read_task_run_result", AsyncMock(return_value=row))

    exit_code, error, payload = await _vehicle("module").wait(_handle(), None)

    assert (exit_code, error) == (0, None)
    assert payload is row


@pytest.mark.parametrize(
    "rc, stderr, expected_phase",
    [
        pytest.param(
            1, 'Error from server (NotFound): pods "aaiclick-task-7-1" not found', kw.POD_NOT_FOUND, id="gone"
        ),
        pytest.param(1, "Unable to connect to the server: dial tcp: i/o timeout", "", id="transient"),
    ],
)
async def test_pod_status_maps_kubectl_failure(monkeypatch, rc, stderr, expected_phase):
    monkeypatch.setattr(kw.cli, "run", AsyncMock(return_value=(rc, "", stderr)))

    assert await kw._pod_status(_handle()) == (expected_phase, -1)


async def test_wait_ends_when_pod_disappears(monkeypatch):
    """A Pod that vanishes never reports a terminal phase; ``wait`` must end on
    ``POD_NOT_FOUND`` instead of looping forever."""
    monkeypatch.setattr(kw, "POLL_INTERVAL", 0)
    monkeypatch.setattr(kw, "_pod_status", AsyncMock(side_effect=[("Running", -1), (kw.POD_NOT_FOUND, -1)]))
    monkeypatch.setattr(kw, "read_task_run_result", AsyncMock(return_value=None))

    exit_code, error, payload = await _vehicle("module").wait(_handle(), None)

    assert (exit_code, error, payload) == (-1, "Pod aaiclick-task-7-1 disappeared", None)


async def test_wait_retries_transient_kubectl_failure(monkeypatch):
    """An empty phase (``kubectl`` could not answer) does not end the wait."""
    monkeypatch.setattr(kw, "POLL_INTERVAL", 0)
    monkeypatch.setattr(kw, "_pod_status", AsyncMock(side_effect=[("Running", -1), ("", -1), ("Succeeded", 0)]))
    monkeypatch.setattr(kw, "read_task_run_result", AsyncMock(return_value=None))

    exit_code, error, _ = await _vehicle("module").wait(_handle(), None)

    assert (exit_code, error) == (0, None)


def test_pod_spec_requires_pod_config():
    task = Task(id=1, job_id=1, entrypoint="m.f", name="t", run_epoch=1)
    with pytest.raises(RuntimeError, match="pod_config"):
        kw._pod_spec_from(task, JobDispatch(RUNNER_KUBERNETES, None), "img:1")


def test_shell_pod_manifest_has_io_helpers_and_shared_volume(monkeypatch):
    monkeypatch.setenv("AAICLICK_K8S_IO_IMAGE", "registry.local/busybox:1")
    m = build_shell_pod_manifest(
        name="p",
        namespace="ns",
        image_tag="img",
        command=["sh", "-c", "x"],
        service_account="sa",
        image_pull_secret=None,
        resources={"limits": {"cpu": "1"}},
        command_env_secret="p",
        active_deadline=90,
    )
    spec = m["spec"]
    assert spec["restartPolicy"] == "Never" and spec["serviceAccountName"] == "sa"
    assert spec["activeDeadlineSeconds"] == 90
    assert [c["name"] for c in spec["containers"]] == ["task", "io"]
    assert [c["name"] for c in spec["initContainers"]] == ["io-in"]
    helpers = (*spec["initContainers"], spec["containers"][1])
    assert {c["image"] for c in helpers} == {"registry.local/busybox:1"}
    assert all("resources" not in c for c in helpers)
    task = spec["containers"][0]
    assert task["image"] == "img" and task["args"] == ["sh", "-c", "x"] and "command" not in task
    assert task["env"] == [{"name": "AAICLICK_IO_DIR", "value": "/aaiclick"}]
    assert task["envFrom"] == [{"secretRef": {"name": "p"}}]
    assert "/aaiclick/in.ready" in spec["initContainers"][0]["command"][-1]
    assert task["resources"] == {"limits": {"cpu": "1"}}
    every = (*spec["initContainers"], *spec["containers"])
    assert all(c["volumeMounts"] == [{"name": "aaiclick-io", "mountPath": "/aaiclick"}] for c in every)
    assert spec["volumes"] == [{"name": "aaiclick-io", "emptyDir": {}}]


def test_shell_pod_manifest_without_command_env_has_no_env_from():
    m = build_shell_pod_manifest(
        name="p",
        namespace="ns",
        image_tag="img",
        command=["true"],
        service_account=None,
        image_pull_secret=None,
        resources=None,
        command_env_secret=None,
        active_deadline=kw.ORPHAN_POD_DEADLINE,
    )
    assert "envFrom" not in m["spec"]["containers"][0]
    assert "serviceAccountName" not in m["spec"]


# One ``get`` answer serves the whole shell lifecycle: the init container is
# up (so in.json gets copied in) and the task container has already exited 0.
_FAKE_KUBECTL_SHELL = """#!/bin/sh
d="$(dirname "$0")"
case "$1" in
  get) if [ -n "$FAKE_CP_FAIL" ]; then phase=Pending; task='{"waiting": {}}'; else phase=Running; task='{"terminated": {"exitCode": 0}}'; fi
       if [ -n "$FAKE_POD_PHASE" ]; then phase="$FAKE_POD_PHASE"; task='{"waiting": {}}'; fi
       printf '{"status": {"phase": "%s", "initContainerStatuses": [{"name": "io-in", "state": {"running": {}}}], "containerStatuses": [{"name": "task", "state": %s}, {"name": "io", "state": {"running": {}}}]}}' "$phase" "$task" ;;
  cp) if [ -n "$FAKE_CP_FAIL" ]; then echo "pods/exec forbidden" >&2; exit 1; fi
      cp "$2" "$d/copied-$(basename "$2")"; echo "$@" >> "$d/cp-argv" ;;
  exec) echo "$@" > "$d/exec-argv"
        case "$FAKE_OUT_MODE" in
          missing) exit 3 ;;
          broken) echo "unable to upgrade connection" >&2; exit 1 ;;
          *) printf '%s' "$FAKE_OUT_JSON" ;;
        esac ;;
  logs) echo "pod says hi" ;;
  apply) cp "$3" "$d/applied.json"; echo "$1" >> "$d/calls" ;;
  create|delete) echo "$1" >> "$d/calls" ;;
esac
"""


async def test_shell_pod_round_trips_in_and_out_json(orch_ctx, monkeypatch, tmp_path):
    """The host copies the resolved kwargs into the waiting init container,
    reads ``out.json`` back through the ``io`` sidecar, stores the result and
    deletes the Pod."""
    monkeypatch.setenv("FAKE_OUT_JSON", '{"_aaiclick_type": "native", "value": {"k": "v"}}')
    spec = JobDispatch(
        RUNNER_KUBERNETES,
        KubernetesConfig("jobs", None, None),
        entry_type="shell",
        command=["sh", "-c", "x"],
        command_env={"K": "v"},
        image_source=ImagePrebuilt(image_tag="img:1"),
    )
    stored = await dispatch_with_fake_cli(monkeypatch, tmp_path, "AAICLICK_KUBECTL_BIN", _FAKE_KUBECTL_SHELL, spec)

    assert stored.result == native_ref({"k": "v"}), stored.error
    assert (tmp_path / "copied-in.json").read_text() == "{}"
    # in.json lands first, then the marker io-in waits for — kubectl cp
    # extracts in place, so the marker is what proves the file is whole.
    cps = (tmp_path / "cp-argv").read_text().splitlines()
    assert [c.split()[2].rsplit(":", 1)[1] for c in cps] == ["/aaiclick/in.json", "/aaiclick/in.ready"]
    assert all(c.split()[-2:] == ["-c", "io-in"] for c in cps)
    exec_argv = (tmp_path / "exec-argv").read_text()
    assert "-c io -- sh -c" in exec_argv and "/aaiclick/out.json" in exec_argv
    assert (tmp_path / "calls").read_text().split() == ["create", "apply", "delete"]


def _shell_spec() -> JobDispatch:
    return JobDispatch(
        RUNNER_KUBERNETES,
        KubernetesConfig("jobs", None, None),
        entry_type="shell",
        command=["sh", "-c", "x"],
        image_source=ImagePrebuilt(image_tag="img:1"),
    )


@pytest.mark.parametrize(
    "mode, status, error",
    [
        # Only "no such file" is "no result"; any other read failure fails the task.
        pytest.param("missing", TASK_COMPLETED, None, id="missing-out-json"),
        pytest.param("broken", TASK_PENDING_FAILURE_CLEANUP, "unable to upgrade connection", id="exec-failure"),
    ],
)
async def test_shell_pod_out_json_read_failures(orch_ctx, monkeypatch, tmp_path, mode, status, error):
    monkeypatch.setenv("FAKE_OUT_MODE", mode)
    stored = await dispatch_with_fake_cli(
        monkeypatch, tmp_path, "AAICLICK_KUBECTL_BIN", _FAKE_KUBECTL_SHELL, _shell_spec()
    )
    assert (stored.status, stored.result) == (status, None)
    if error:
        assert error in (stored.error or "")


async def test_shell_pod_terminal_phase_without_task_exit_fails(orch_ctx, monkeypatch, tmp_path):
    """A Pod that reaches Failed while the task container never ran (helper
    image pull failure, activeDeadlineSeconds, eviction) ends the wait with
    an error instead of parking the worker."""
    monkeypatch.setenv("FAKE_POD_PHASE", "Failed")
    stored = await dispatch_with_fake_cli(
        monkeypatch, tmp_path, "AAICLICK_KUBECTL_BIN", _FAKE_KUBECTL_SHELL, _shell_spec()
    )
    assert stored.status == TASK_PENDING_FAILURE_CLEANUP
    assert "Failed before the task container terminated" in (stored.error or "")


async def test_shell_pod_copy_in_failure_fails_the_task(orch_ctx, monkeypatch, tmp_path):
    """``kubectl cp`` needs ``pods/exec``; a cluster without it must fail the
    task with kubectl's reason, not park the Pod in Init forever."""
    monkeypatch.setenv("FAKE_CP_FAIL", "1")
    monkeypatch.setattr(kw, "COPY_IN_ATTEMPTS", 2)
    monkeypatch.setattr(kw, "POLL_INTERVAL", 0.01)
    stored = await dispatch_with_fake_cli(
        monkeypatch, tmp_path, "AAICLICK_KUBECTL_BIN", _FAKE_KUBECTL_SHELL, _shell_spec()
    )
    assert stored.status == TASK_PENDING_FAILURE_CLEANUP
    assert "pods/exec forbidden" in (stored.error or "")


async def test_jvm_pod_output_reaches_task_logs(orch_ctx, monkeypatch, tmp_path):
    """The jvm shim writes no logs itself: the host registers the attempt and
    follows ``kubectl logs -c task`` into task_logs (Kubernetes merges the
    streams). The class name is the shim's one argument."""
    monkeypatch.setenv("FAKE_OUT_JSON", "null")
    spec = JobDispatch(
        RUNNER_KUBERNETES,
        KubernetesConfig("default", None, None),
        entry_type=ENTRY_JVM,
        command=["com.example.Pipeline"],
        image_source=ImagePrebuilt(image_tag="img:1"),
    )
    stored = await dispatch_with_fake_cli(monkeypatch, tmp_path, "AAICLICK_KUBECTL_BIN", _FAKE_KUBECTL_SHELL, spec)

    assert len(stored.run_ids) == 1
    lines = await read_task_logs(stored.id, stored.run_ids[0])
    assert [(line.stream, line.text) for line in lines] == [("stdout", "pod says hi")]
    applied = json.loads((tmp_path / "applied.json").read_text())
    task = applied["spec"]["containers"][0]
    assert task["args"] == ["com.example.Pipeline"] and "command" not in task
