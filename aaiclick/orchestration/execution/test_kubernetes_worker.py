"""Tests for the Kubernetes runner — manifest builder and collect logic.

The container-side entrypoint tests live in ``test_remote_result.py`` (its
own module) because they boot a chdb ``orch_context()``; see the chdb
single-session constraint in ``docs/designs/testing.md``."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from aaiclick.data.object.refs import native_ref

from ..kubernetes_config import KubernetesConfig
from ..logging import read_task_logs
from ..models import Task
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
        env={"AAICLICK_SQL_URL": "u"},
        service_account="sa",
        image_pull_secret="regcred",
        resources={"limits": {"cpu": "1"}},
        entry_type="module",
        command=None,
        command_env_secret=None,
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
    assert {"name": "AAICLICK_SQL_URL", "value": "u"} in c["env"]
    assert c["command"][-4:] == ["--task-id", "7", "--run-epoch", "2"]


def test_build_pod_manifest_omits_optional_fields():
    m = kw._build_pod_manifest(
        name="n",
        namespace="default",
        image_tag="img",
        task_id=1,
        run_epoch=0,
        env={},
        service_account=None,
        image_pull_secret=None,
        resources=None,
        entry_type="module",
        command=None,
        command_env_secret=None,
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


def test_module_pod_uses_shim_and_runner_env():
    m = kw._build_pod_manifest(
        name="p",
        namespace="default",
        image_tag="aaiclick-job:abc",
        task_id=7,
        run_epoch=2,
        env={"AAICLICK_SQL_URL": "u"},
        service_account=None,
        image_pull_secret=None,
        resources=None,
        entry_type="module",
        command=None,
        command_env_secret=None,
    )
    c = m["spec"]["containers"][0]
    assert "--task-id" in c["command"] and "7" in c["command"]
    assert {e["name"] for e in c["env"]} == {"AAICLICK_SQL_URL"}


def test_jvm_pod_sets_args_only_with_runner_env():
    m = kw._build_pod_manifest(
        name="p",
        namespace="default",
        image_tag="ghcr.io/example/pipeline:1.0",
        task_id=7,
        run_epoch=2,
        env={"AAICLICK_SQL_URL": "u"},
        service_account=None,
        image_pull_secret=None,
        resources=None,
        entry_type="jvm",
        command=None,
        command_env_secret=None,
    )
    c = m["spec"]["containers"][0]
    # The image's own ENTRYPOINT is the aaiclick-task-api shim — only args are set.
    assert "command" not in c
    assert c["args"] == ["--task-id", "7", "--run-epoch", "2"]
    assert {e["name"] for e in c["env"]} == {"AAICLICK_SQL_URL"}


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


_FAKE_KUBECTL = """#!/bin/sh
case "$1" in
  get) printf 'Succeeded 0' ;;
  logs) echo "jvm says hi"; echo "jvm warns" ;;
esac
"""


async def test_jvm_pod_output_reaches_task_logs(orch_ctx, monkeypatch, tmp_path):
    """The jvm shim writes no logs itself: the host registers the attempt and
    follows ``kubectl logs`` into task_logs (Kubernetes merges the streams)."""
    spec = JobDispatch(
        RUNNER_KUBERNETES,
        KubernetesConfig("default", None, None),
        entry_type=ENTRY_JVM,
        image_source=ImagePrebuilt(image_tag="img:1"),
    )
    stored = await dispatch_with_fake_cli(monkeypatch, tmp_path, "AAICLICK_KUBECTL_BIN", _FAKE_KUBECTL, spec)

    assert len(stored.run_ids) == 1
    lines = await read_task_logs(stored.id, stored.run_ids[0])
    assert [(line.stream, line.text) for line in lines] == [("stdout", "jvm says hi"), ("stdout", "jvm warns")]


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
    )
    spec = m["spec"]
    assert spec["restartPolicy"] == "Never" and spec["serviceAccountName"] == "sa"
    assert [c["name"] for c in spec["containers"]] == ["task", "io"]
    assert [c["name"] for c in spec["initContainers"]] == ["io-in"]
    helpers = (*spec["initContainers"], spec["containers"][1])
    assert {c["image"] for c in helpers} == {"registry.local/busybox:1"}
    assert all("resources" not in c for c in helpers)
    task = spec["containers"][0]
    assert task["image"] == "img" and task["command"] == ["sh", "-c", "x"]
    assert task["env"] == [{"name": "AAICLICK_IO_DIR", "value": "/aaiclick"}]
    assert task["envFrom"] == [{"secretRef": {"name": "p"}}]
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
    )
    assert "envFrom" not in m["spec"]["containers"][0]
    assert "serviceAccountName" not in m["spec"]


# One ``get`` answer serves the whole shell lifecycle: the init container is
# up (so in.json gets copied in) and the task container has already exited 0.
_FAKE_KUBECTL_SHELL = """#!/bin/sh
d="$(dirname "$0")"
case "$1" in
  get) printf '%s' '{"status": {"phase": "Running", "initContainerStatuses": [{"name": "io-in", "state": {"running": {}}}], "containerStatuses": [{"name": "task", "state": {"terminated": {"exitCode": 0}}}, {"name": "io", "state": {"running": {}}}]}}' ;;
  cp) cp "$2" "$d/in-copy.json"; echo "$@" > "$d/cp-argv" ;;
  exec) echo "$@" > "$d/exec-argv"; printf '%s' "$FAKE_OUT_JSON" ;;
  logs) echo "pod says hi" ;;
  apply|create|delete) echo "$1" >> "$d/calls" ;;
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
    assert (tmp_path / "in-copy.json").read_text() == "{}"
    cp_argv = (tmp_path / "cp-argv").read_text().split()
    assert cp_argv[2].endswith(":/aaiclick/in.json") and cp_argv[-2:] == ["-c", "io-in"]
    exec_argv = (tmp_path / "exec-argv").read_text().split()
    assert exec_argv[-5:] == ["-c", "io", "--", "cat", "/aaiclick/out.json"]
    assert (tmp_path / "calls").read_text().split() == ["create", "apply", "delete"]
