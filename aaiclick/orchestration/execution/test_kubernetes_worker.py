"""Tests for the Kubernetes runner — manifest builder and collect logic.

The Pod-side ``_pod_main`` test lives in ``test_kubernetes_pod_main.py`` (its
own module) because it boots a chdb ``orch_context()``; see the chdb
single-session constraint in ``docs/designs/testing.md``."""

from __future__ import annotations

from unittest.mock import AsyncMock

from . import kubernetes_worker as kw
from .log_test_helpers import flush_recorder


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
        command_env=None,
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
        command_env=None,
    )
    spec = m["spec"]
    assert "serviceAccountName" not in spec
    assert "imagePullSecrets" not in spec
    assert "resources" not in spec["containers"][0]


def _handle(task_id=7, run_epoch=1, run_id=None):
    return kw._PodHandle(
        name="aaiclick-task-7-1",
        namespace="default",
        task_id=task_id,
        job_id=1,
        run_epoch=run_epoch,
        run_id=run_id,
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
        command_env={"K": "v"},
    )
    c = m["spec"]["containers"][0]
    assert c["command"] == ["python", "main.py"]
    names = {e["name"] for e in c["env"]}
    assert names == {"K"}  # runner env (AAICLICK_SQL_URL) excluded for shell


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
        command_env=None,
    )
    c = m["spec"]["containers"][0]
    assert "--task-id" in c["command"] and "7" in c["command"]
    assert {e["name"] for e in c["env"]} == {"AAICLICK_SQL_URL"}


def test_collect_shell_success_from_exit_code():
    out = _collect(_handle(), 0, None, was_cancelled=False, payload=None, entry_type="shell")
    assert out.success is True and out.error is None


def test_collect_shell_failure_from_exit_code():
    out = _collect(_handle(), 3, None, was_cancelled=False, payload=None, entry_type="shell")
    assert out.success is False and out.error == "exit 3"


async def test_shell_pod_logs_flushed_to_ch(monkeypatch):
    """Shell pod output is fetched via `kubectl logs` and flushed to CH under
    the run_id registered at launch; module pods skip the host-side fetch
    (they stream to CH from inside the pod)."""
    flushed, fake_flush = flush_recorder()
    monkeypatch.setattr(kw, "flush_shell_logs", fake_flush)
    monkeypatch.setattr(kw, "_pod_status", AsyncMock(return_value=("Succeeded", 0)))
    monkeypatch.setattr(kw, "_pod_logs_text", AsyncMock(return_value="pod out\n"))

    exit_code, error, payload = await _vehicle("shell").wait(_handle(run_id=88), None)

    assert (exit_code, error, payload) == (0, None, None)
    assert flushed == {"task_id": 7, "job_id": 1, "run_id": 88, "text": "pod out\n"}


async def test_module_pod_wait_skips_host_log_fetch(monkeypatch):
    logs_fetch = AsyncMock(return_value="ignored")
    monkeypatch.setattr(kw, "_pod_logs_text", logs_fetch)
    monkeypatch.setattr(kw, "_pod_status", AsyncMock(return_value=("Succeeded", 0)))
    row = kw.RunnerResult(True, None, None)
    monkeypatch.setattr(kw, "_read_task_run_result_row", AsyncMock(return_value=row))

    exit_code, error, payload = await _vehicle("module").wait(_handle(), None)

    assert (exit_code, error) == (0, None)
    assert payload is row
    logs_fetch.assert_not_awaited()
