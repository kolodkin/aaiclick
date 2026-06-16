"""Tests for the Kubernetes runner — manifest builder and collect logic.

The Pod-side ``_pod_main`` test lives in ``test_kubernetes_pod_main.py`` (its
own module) because it boots a chdb ``orch_context()``; see the chdb
single-session constraint in ``docs/testing.md``."""

from __future__ import annotations

from . import kubernetes_worker as kw


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
    )
    spec = m["spec"]
    assert "serviceAccountName" not in spec
    assert "imagePullSecrets" not in spec
    assert "resources" not in spec["containers"][0]


def _handle(task_id=7, run_epoch=1):
    return kw._PodHandle(
        name="aaiclick-task-7-1",
        namespace="default",
        log_path="/logs/k8s-1.log",
        task_id=task_id,
        run_epoch=run_epoch,
    )


def test_collect_cancelled_overrides_row():
    h = _handle()
    h.result_row = kw.RunnerResult(True, {"x": 1}, None, None)
    out = kw._KubernetesVehicle.__new__(kw._KubernetesVehicle).collect(h, 137, None, was_cancelled=True)
    assert out.success is False and out.error == "cancelled"


def test_collect_synthesizes_failure_when_row_missing():
    h = _handle()
    h.result_row = None
    out = kw._KubernetesVehicle.__new__(kw._KubernetesVehicle).collect(h, 1, None, was_cancelled=False)
    assert out.success is False
    assert "no result" in (out.error or "")


def test_collect_returns_row():
    h = _handle()
    h.result_row = kw.RunnerResult(True, {"native_value": 5}, "/logs/k8s-1.log", None)
    out = kw._KubernetesVehicle.__new__(kw._KubernetesVehicle).collect(h, 0, None, was_cancelled=False)
    assert out.success is True and out.result_ref == {"native_value": 5}
