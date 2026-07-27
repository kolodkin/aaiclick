"""Tests for the Kubernetes runner — manifest builder and collect logic.

The Pod-side ``_pod_main`` test lives in ``test_kubernetes_pod_main.py`` (its
own module) because it boots a chdb ``orch_context()``; see the chdb
single-session constraint in ``docs/designs/testing.md``."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

from ..models import Task
from . import kubernetes_worker as kw
from .execution_worker import JobDispatch
from .kubernetes_worker import build_shell_pod_spec


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


def test_build_shell_pod_spec_wraps_argv():
    task = Task(
        id=9,
        job_id=1,
        name="t",
        entrypoint="",
        entry_type="shell",
        command=["echo", "hi"],
        command_env={"K": "v"},
        run_epoch=1,
    )
    dispatch = JobDispatch(
        "kubernetes",
        "img:tag",
        {"namespace": "jobs", "service_account": "sa", "image_pull_secret": None, "resources": None},
        "shell",
        ["echo", "hi"],
        {"K": "v"},
    )
    spec = build_shell_pod_spec(task, dispatch, "img:tag")
    assert spec.argv[:3] == ["kubectl", "run", "aaiclick-task-9-1"]
    assert {"--attach", "--rm", "--quiet", "--restart=Never"} <= set(spec.argv)
    assert "--image=img:tag" in spec.argv
    overrides = json.loads(next(a for a in spec.argv if a.startswith("--overrides=")).removeprefix("--overrides="))
    assert overrides["spec"]["containers"][0]["command"] == ["echo", "hi"]
    assert overrides["spec"]["containers"][0]["env"] == [{"name": "K", "value": "v"}]
    assert overrides["spec"]["serviceAccountName"] == "sa"
    assert ["-n", "jobs"] == spec.argv[spec.argv.index("-n") : spec.argv.index("-n") + 2]
    assert spec.cleanup_argv == ["kubectl", "delete", "pod", "aaiclick-task-9-1", "-n", "jobs", "--ignore-not-found"]


async def test_module_pod_wait_reads_result_row(monkeypatch):
    monkeypatch.setattr(kw, "_pod_status", AsyncMock(return_value=("Succeeded", 0)))
    row = kw.RunnerResult(True, None, None)
    monkeypatch.setattr(kw, "_read_task_run_result_row", AsyncMock(return_value=row))

    exit_code, error, payload = await _vehicle("module").wait(_handle(), None)

    assert (exit_code, error) == (0, None)
    assert payload is row
