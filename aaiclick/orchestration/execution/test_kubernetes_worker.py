"""Tests for the Kubernetes runner — manifest builder and collect logic.

The container-side entrypoint tests live in ``test_remote_result.py`` (its
own module) because they boot a chdb ``orch_context()``; see the chdb
single-session constraint in ``docs/designs/testing.md``."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from ..kubernetes_config import KubernetesConfig
from ..logging import read_task_logs
from ..models import Task
from ..runner_config import ENTRY_JVM, ImagePrebuilt
from . import kubernetes_worker as kw
from .execution_worker import JobDispatch
from .kubernetes_worker import build_shell_pod_spec
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
    payload = kw.RunnerResult(True, {"native_value": 5}, None)
    out = _collect(_handle(), 0, None, was_cancelled=False, payload=payload)
    assert out.success is True and out.result_ref == {"native_value": 5}


def test_shell_pod_runs_argv_only_command_env():
    m = kw._build_pod_manifest(
        name="p",
        namespace="default",
        image_tag="python:3.12",
        task_id=1,
        run_epoch=0,
        env={"AAICLICK_SQL_URL": "secret"},
        service_account=None,
        image_pull_secret=None,
        resources=None,
        entry_type="shell",
        command=["python", "main.py"],
        command_env_secret="aaiclick-task-1-0",
    )
    c = m["spec"]["containers"][0]
    assert c["command"] == ["python", "main.py"]
    assert "env" not in c  # runner env (AAICLICK_SQL_URL) excluded for shell
    assert c["envFrom"] == [{"secretRef": {"name": "aaiclick-task-1-0"}}]


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


def _shell_task_and_dispatch(command_env):
    task = Task(
        id=9,
        job_id=1,
        name="t",
        entrypoint="",
        entry_type="shell",
        command=["echo", "hi"],
        command_env=command_env,
        run_epoch=1,
    )
    dispatch = JobDispatch(
        RUNNER_KUBERNETES,
        KubernetesConfig("jobs", "sa", None, None),
        "shell",
        ["echo", "hi"],
        command_env,
    )
    return task, dispatch


def _capture_kubectl_create(monkeypatch) -> list[dict]:
    """Stub ``cli.run`` to record the manifest ``kubectl create -f`` is given
    (read at call time — the file is removed once kubectl returns)."""
    manifests: list[dict] = []

    async def fake_run(*cmd, **kwargs):
        assert cmd[:3] == ("kubectl", "create", "-f")
        assert Path(cmd[3]).stat().st_mode & 0o777 == 0o600
        manifests.append(json.loads(Path(cmd[3]).read_text()))
        return 0, "", ""

    monkeypatch.setattr(kw.cli, "run", fake_run)
    return manifests


async def test_build_shell_pod_spec_wraps_argv(monkeypatch):
    manifests = _capture_kubectl_create(monkeypatch)
    task, dispatch = _shell_task_and_dispatch({"K": "v", "PATH": "/evil"})

    spec = await build_shell_pod_spec(task, dispatch, "img:tag")

    assert spec.argv[:3] == ["kubectl", "run", "aaiclick-task-9-1"]
    assert {"--attach", "--rm", "--quiet", "--restart=Never"} <= set(spec.argv)
    assert "--image=img:tag" in spec.argv
    overrides_arg = next(a for a in spec.argv if a.startswith("--overrides="))
    overrides = json.loads(overrides_arg.removeprefix("--overrides="))
    container = overrides["spec"]["containers"][0]
    assert container["command"] == ["echo", "hi"]
    assert overrides["spec"]["serviceAccountName"] == "sa"
    assert ["-n", "jobs"] == spec.argv[spec.argv.index("-n") : spec.argv.index("-n") + 2]
    assert spec.env is None
    assert spec.env_file is None
    # command_env values live in a per-attempt Secret, never on the argv (ps).
    assert "/evil" not in " ".join(spec.argv)
    assert container["envFrom"] == [{"secretRef": {"name": "aaiclick-task-9-1"}}]
    assert manifests == [
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "aaiclick-task-9-1", "namespace": "jobs"},
            "type": "Opaque",
            "stringData": {"K": "v", "PATH": "/evil"},
        }
    ]
    assert spec.cleanup_argv == [
        "kubectl",
        "delete",
        "pod/aaiclick-task-9-1",
        "secret/aaiclick-task-9-1",
        "-n",
        "jobs",
        "--ignore-not-found",
    ]


async def test_build_shell_pod_spec_without_command_env_creates_no_secret(monkeypatch):
    manifests = _capture_kubectl_create(monkeypatch)
    task, dispatch = _shell_task_and_dispatch(None)

    spec = await build_shell_pod_spec(task, dispatch, "img:tag")

    assert manifests == []
    overrides = json.loads(next(a for a in spec.argv if a.startswith("--overrides=")).removeprefix("--overrides="))
    assert "envFrom" not in overrides["spec"]["containers"][0]
    assert spec.cleanup_argv == ["kubectl", "delete", "pod/aaiclick-task-9-1", "-n", "jobs", "--ignore-not-found"]


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
        KubernetesConfig("default", None, None, None),
        entry_type=ENTRY_JVM,
        image_source=ImagePrebuilt(image_tag="img:1"),
    )
    stored = await dispatch_with_fake_cli(monkeypatch, tmp_path, "AAICLICK_KUBECTL_BIN", _FAKE_KUBECTL, spec)

    assert len(stored.run_ids) == 1
    lines = await read_task_logs(stored.id, stored.run_ids[0])
    assert [(line.stream, line.text) for line in lines] == [("stdout", "jvm says hi"), ("stdout", "jvm warns")]
