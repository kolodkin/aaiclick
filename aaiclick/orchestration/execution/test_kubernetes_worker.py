"""Tests for the Kubernetes runner — manifest builder and collect logic.

The container-side entrypoint tests live in ``test_remote_result.py`` (its
own module) because they boot a chdb ``orch_context()``; see the chdb
single-session constraint in ``docs/designs/testing.md``."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from aaiclick.data.object.refs import native_ref

from ...testing import get_task_runs
from ..factories import create_job, create_task
from ..jobs.queries import get_tasks_for_job
from ..kubernetes_config import KubernetesConfig
from ..logging import read_task_logs
from ..models import Task
from ..runner_config import ENTRY_JVM
from . import cli
from . import kubernetes_worker as kw
from .kubernetes_worker import build_shell_pod_manifest
from .pod_watch import PodWatch, pod_labels


def test_build_pod_manifest_shape():
    m = kw._build_pod_manifest(
        name="aaiclick-task-7-2",
        namespace="ml",
        image_tag="reg/aaiclick-job:abc",
        task_id=7,
        job_id=3,
        run_epoch=2,
        env_secret="p",
        service_account="sa",
        image_pull_secret="regcred",
        resources={"limits": {"cpu": "1"}},
    )
    assert m["kind"] == "Pod"
    assert m["metadata"] == {"name": "aaiclick-task-7-2", "namespace": "ml", "labels": LABELS}
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
        job_id=1,
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


def _vehicle():
    return kw._KubernetesVehicle(kw._PodSpec("img", 1, "default", None, None, None, "module", None, None))


LABELS = {"aaiclick/managed": "true", "aaiclick/job-id": "3", "aaiclick/task-id": "7", "aaiclick/run-epoch": "2"}


def _fake_create(created: list[dict]):
    """Stand-in for ``_kubectl_create_from_file``: records manifests and
    answers like ``kubectl -o json``, with a uid for Pods."""

    async def create(manifest: dict, verb: str = "create") -> dict:
        created.append(manifest)
        return {**manifest, "metadata": {**manifest["metadata"], "uid": f"uid-{manifest['kind'].lower()}"}}

    return create


def test_env_secret_owned_by_pod():
    secret = kw._build_env_secret("aaiclick-task-7-2", "ns", {"A": "1"}, labels=LABELS, owner_uid="u1")
    assert secret["metadata"]["labels"] == LABELS
    assert secret["metadata"]["ownerReferences"] == [
        {"apiVersion": "v1", "kind": "Pod", "name": "aaiclick-task-7-2", "uid": "u1"}
    ]


async def test_module_launch_creates_pod_before_secret(monkeypatch):
    monkeypatch.setattr(kw, "build_runner_env", lambda: {"X": "1"})
    created: list[dict] = []
    monkeypatch.setattr(kw, "_kubectl_create_from_file", AsyncMock(side_effect=_fake_create(created)))

    await _vehicle().launch(Task(id=7, job_id=3, entrypoint="m.f", name="t", run_epoch=2), execution_worker_id=1)

    assert [m["kind"] for m in created] == ["Pod", "Secret"]
    assert created[1]["metadata"]["ownerReferences"][0]["uid"] == "uid-pod"
    assert created[0]["metadata"]["labels"] == LABELS == created[1]["metadata"]["labels"]


@pytest.mark.parametrize("command_env,kinds", [({"K": "v"}, ["Pod", "Secret"]), (None, ["Pod"])])
async def test_shell_launch_creates_pod_before_secret(monkeypatch, tmp_path, command_env, kinds):
    created: list[dict] = []
    monkeypatch.setattr(kw, "_kubectl_create_from_file", AsyncMock(side_effect=_fake_create(created)))
    monkeypatch.setattr(kw, "stage_in_json", AsyncMock(return_value=tmp_path))
    spec = kw._PodSpec("img", 3, "ns", None, None, None, "shell", ["true"], command_env)
    vehicle = kw._KubernetesShellVehicle(spec, timeout=None)

    await vehicle.launch(Task(id=7, job_id=3, entrypoint="m.f", name="t", run_epoch=2), execution_worker_id=1)

    assert [m["kind"] for m in created] == kinds
    if command_env:
        assert created[1]["metadata"]["ownerReferences"][0]["uid"] == "uid-pod"


async def test_module_pod_keeps_runner_env_out_of_the_pod_spec(monkeypatch):
    """The runner env carries database passwords; anyone who can ``get pods``
    must not read them, so they travel in a per-attempt Secret."""
    monkeypatch.setattr(kw, "build_runner_env", lambda: {"AAICLICK_SQL_URL": "postgresql://u:hunter2@db/x"})
    created: list[dict] = []
    monkeypatch.setattr(kw, "_kubectl_create_from_file", AsyncMock(side_effect=_fake_create(created)))

    await _vehicle().launch(Task(id=7, job_id=1, entrypoint="m.f", name="t", run_epoch=2), execution_worker_id=1)

    pod, secret = created
    assert secret["kind"] == "Secret" and secret["stringData"] == {"AAICLICK_SQL_URL": "postgresql://u:hunter2@db/x"}
    assert pod["kind"] == "Pod" and "hunter2" not in json.dumps(pod)
    assert pod["spec"]["containers"][0]["envFrom"] == [{"secretRef": {"name": secret["metadata"]["name"]}}]


def test_shell_pod_manifest_has_io_helpers_and_shared_volume(monkeypatch):
    monkeypatch.setenv("AAICLICK_K8S_IO_IMAGE", "registry.local/busybox:1")
    m = build_shell_pod_manifest(
        name="p",
        namespace="ns",
        image_tag="img",
        task_id=7,
        job_id=3,
        run_epoch=2,
        command=["sh", "-c", "x"],
        service_account="sa",
        image_pull_secret=None,
        resources={"limits": {"cpu": "1"}},
        command_env_secret="p",
        active_deadline=90,
    )
    spec = m["spec"]
    assert m["metadata"]["labels"] == LABELS
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
        task_id=1,
        job_id=1,
        run_epoch=0,
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
  apply) cp "$3" "$d/applied.json"; echo "$1" >> "$d/calls"; printf '{"metadata": {"uid": "u-fake"}}' ;;
  create) echo "$1" >> "$d/calls"; printf '{"metadata": {"uid": "u-fake"}}' ;;
  delete) echo "$1" >> "$d/calls" ;;
esac
"""


# ---------------------------------------------------------------------------
# run_module_pod — the watch-driven module coroutine
# ---------------------------------------------------------------------------


def _pod_json(name: str, task_state: dict, *, node: str | None = "n1") -> dict:
    spec: dict = {"containers": [{"name": "task"}]}
    if node:
        spec["nodeName"] = node
    return {
        "metadata": {"name": name, "labels": pod_labels(7, 3, 2)},
        "spec": spec,
        "status": {"phase": "Running", "containerStatuses": [{"name": "task", "state": task_state}]},
    }


_POD_CONFIG = KubernetesConfig("ns", None, None)
_MODULE_TASK = Task(id=7, job_id=3, entrypoint="m.f", name="t", run_epoch=2)
_POD = "aaiclick-task-7-2"


@pytest.fixture
def module_pod(monkeypatch):
    """A fake cluster for ``run_module_pod``: records creates and deletes,
    serves a result row, and exposes the watch to drive Pod events."""
    created: list[dict] = []
    deleted: list[str] = []
    monkeypatch.setattr(kw, "build_runner_env", lambda: {"X": "1"})
    monkeypatch.setattr(kw, "_kubectl_create_from_file", AsyncMock(side_effect=_fake_create(created)))
    monkeypatch.setattr(kw, "_kubectl_delete", AsyncMock(side_effect=lambda name, ns: deleted.append(name)))
    monkeypatch.setattr(kw, "read_task_run_result", AsyncMock(return_value=kw.RunnerResult(True, native_ref(5), None)))
    watch = PodWatch("ns", "kubectl")

    def start(*, image_tag: str | None = "img:1", detach: list[bool] | None = None) -> asyncio.Task:
        coro = kw.run_module_pod(
            _MODULE_TASK, 1, _POD_CONFIG, watch, image_tag=image_tag, resources=None, detach=detach or [False]
        )
        return asyncio.create_task(coro)

    return SimpleNamespace(watch=watch, created=created, deleted=deleted, start=start)


async def test_run_module_pod_success_reads_result_row(module_pod):
    run = module_pod.start()
    await asyncio.sleep(0)
    assert [m["kind"] for m in module_pod.created] == ["Pod", "Secret"]
    module_pod.watch.apply(_pod_json(_POD, {"terminated": {"exitCode": 0}}), deleted=False)

    result = await run

    assert result == kw.RunnerResult(True, native_ref(5), None)
    assert module_pod.deleted == [_POD]


async def test_run_module_pod_adopted_skips_launch(module_pod):
    module_pod.watch.apply(_pod_json(_POD, {"running": {}}), deleted=False)
    run = module_pod.start(image_tag=None)
    await asyncio.sleep(0)
    assert module_pod.created == []
    module_pod.watch.apply(_pod_json(_POD, {"terminated": {"exitCode": 0}}), deleted=False)

    assert (await run).success
    assert module_pod.deleted == [_POD]


async def test_cancel_with_deadline_reason_reports_it_and_deletes_pod(module_pod):
    run = module_pod.start()
    await asyncio.sleep(0)

    run.cancel("Pod aaiclick-task-7-2 not started after 900s: ImagePullBackOff: back-off")
    result = await run

    assert result.success is False and "ImagePullBackOff" in (result.error or "")
    assert module_pod.deleted == [_POD]


async def test_cancel_without_reason_reports_cancelled(module_pod):
    run = module_pod.start()
    await asyncio.sleep(0)
    run.cancel()
    assert (await run) == kw.RunnerResult(False, None, "cancelled")


async def test_cancel_detached_keeps_pod(module_pod):
    detach = [False]
    run = module_pod.start(detach=detach)
    await asyncio.sleep(0)

    detach[0] = True
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run

    assert module_pod.deleted == []


async def test_quota_rejection_raises(module_pod, monkeypatch):
    err = cli.CommandError(("kubectl",), 1, 'pods "x" is forbidden: exceeded quota: compute, requested: cpu=1')
    monkeypatch.setattr(kw, "_kubectl_create_from_file", AsyncMock(side_effect=err))

    with pytest.raises(kw.QuotaExceeded):
        await module_pod.start()


async def test_pod_gone_reports_disappeared(module_pod):
    run = module_pod.start()
    await asyncio.sleep(0)
    module_pod.watch.apply(_pod_json(_POD, {"running": {}}), deleted=False)
    module_pod.watch.apply(_pod_json(_POD, {"running": {}}), deleted=True)

    result = await run

    assert result.success is False and "disappeared" in (result.error or "")


# ---------------------------------------------------------------------------
# run_shell_pod — the watch-driven shell/jvm coroutine with mid-stage adoption
# ---------------------------------------------------------------------------


def _shell_pod_json(name: str, io_in: dict, task_state: dict, *, node: str | None = "n1") -> dict:
    pod = _pod_json(name, task_state, node=node)
    pod["status"]["initContainerStatuses"] = [{"name": "io-in", "state": io_in}]
    return pod


_SHELL_TASK = Task(id=7, job_id=3, entrypoint="m.f", name="t", run_epoch=2, entry_type="shell", kwargs={})
_OUT_JSON = '{"_aaiclick_type": "native", "value": {"k": "v"}}'


@pytest.fixture
def shell_pod(monkeypatch, tmp_path):
    """A fake cluster for ``run_shell_pod``: ``kubectl cp`` / ``exec`` are
    recorded, the log follower and run registration are stubbed."""
    created: list[dict] = []
    deleted: list[str] = []
    cli_calls: list[list[str]] = []
    follows: list[list[str]] = []
    io_dir = tmp_path / "io"
    io_dir.mkdir()
    (io_dir / "in.json").write_text("{}")

    knobs = SimpleNamespace(exec_mode="ok", cp_fails=False)

    async def fake_cli(*argv, check=True, stream=True, cwd=None, env=None):
        cli_calls.append(list(argv))
        if argv[1] == "exec":
            if knobs.exec_mode == "missing":
                return 3, "", ""
            if knobs.exec_mode == "broken":
                return 1, "", "error: unable to upgrade connection"
            return 0, _OUT_JSON, ""
        if argv[1] == "cp" and knobs.cp_fails:
            return 1, "", "pods/exec forbidden"
        return 0, "", ""

    async def fake_follow(argv, task_id, job_id, run_id):
        follows.append(argv)

    monkeypatch.setattr(kw, "_kubectl_create_from_file", AsyncMock(side_effect=_fake_create(created)))
    monkeypatch.setattr(kw, "_kubectl_delete", AsyncMock(side_effect=lambda name, ns: deleted.append(name)))
    monkeypatch.setattr(kw, "stage_in_json", AsyncMock(return_value=io_dir))
    monkeypatch.setattr(kw.cli, "run", fake_cli)
    monkeypatch.setattr(kw, "follow_vehicle_output", fake_follow)
    monkeypatch.setattr(kw, "register_run", AsyncMock(return_value=123))
    monkeypatch.setattr(kw, "last_task_log_time", AsyncMock(return_value=None))
    watch = PodWatch("ns", "kubectl")

    def start(*, image_tag: str | None = "img:1", run_id: int | None = None, detach: list[bool] | None = None):
        coro = kw.run_shell_pod(
            _SHELL_TASK,
            1,
            _POD_CONFIG,
            watch,
            image_tag=image_tag,
            command=["sh", "-c", "x"],
            command_env={"K": "v"},
            resources=None,
            run_id=run_id,
            detach=detach or [False],
        )
        return asyncio.create_task(coro)

    return SimpleNamespace(
        watch=watch,
        created=created,
        deleted=deleted,
        cli=cli_calls,
        follows=follows,
        io_dir=io_dir,
        start=start,
        knobs=knobs,
    )


def _cp_targets(calls: list[list[str]]) -> list[str]:
    return [c[3].rsplit(":", 1)[1] for c in calls if c[1] == "cp"]


async def test_run_shell_pod_full_lifecycle(shell_pod):
    run = shell_pod.start()
    await asyncio.sleep(0)
    assert [m["kind"] for m in shell_pod.created] == ["Pod", "Secret"]

    shell_pod.watch.apply(_shell_pod_json(_POD, {"running": {}}, {"waiting": {}}), deleted=False)
    await asyncio.sleep(0.05)
    assert _cp_targets(shell_pod.cli) == ["/aaiclick/in.json", "/aaiclick/in.ready"]
    assert kw.register_run.await_count == 0 and shell_pod.follows == []

    shell_pod.watch.apply(_shell_pod_json(_POD, {"terminated": {"exitCode": 0}}, {"running": {}}), deleted=False)
    await asyncio.sleep(0.05)
    kw.register_run.assert_awaited_once_with(7)
    assert shell_pod.follows and shell_pod.follows[0][1:3] == ["logs", "--follow"]
    assert not any(a.startswith("--since-time") for a in shell_pod.follows[0])

    shell_pod.watch.apply(
        _shell_pod_json(_POD, {"terminated": {"exitCode": 0}}, {"terminated": {"exitCode": 0}}), deleted=False
    )
    result = await run

    assert result == kw.RunnerResult(True, native_ref({"k": "v"}), None)
    assert [c[1] for c in shell_pod.cli if c[1] == "exec"] == ["exec"]
    assert shell_pod.deleted == [_POD] and not shell_pod.io_dir.exists()


async def test_run_shell_pod_adopted_at_io_in_restages_and_copies(shell_pod):
    shell_pod.watch.apply(_shell_pod_json(_POD, {"running": {}}, {"waiting": {}}), deleted=False)
    run = shell_pod.start(image_tag=None)
    await asyncio.sleep(0.05)

    assert shell_pod.created == []
    kw.stage_in_json.assert_awaited_once()
    assert _cp_targets(shell_pod.cli) == ["/aaiclick/in.json", "/aaiclick/in.ready"]
    shell_pod.watch.apply(
        _shell_pod_json(_POD, {"terminated": {"exitCode": 0}}, {"terminated": {"exitCode": 0}}), deleted=False
    )
    assert (await run).success


async def test_run_shell_pod_adopted_at_task_running_reuses_run_and_follows_since(shell_pod, monkeypatch):
    since = datetime(2026, 11, 5, 12, 0, 1, tzinfo=UTC)
    monkeypatch.setattr(kw, "last_task_log_time", AsyncMock(return_value=since))
    shell_pod.watch.apply(_shell_pod_json(_POD, {"terminated": {"exitCode": 0}}, {"running": {}}), deleted=False)
    run = shell_pod.start(image_tag=None, run_id=555)
    await asyncio.sleep(0.05)

    assert _cp_targets(shell_pod.cli) == []
    kw.register_run.assert_not_awaited()
    kw.last_task_log_time.assert_awaited_once_with(7, 555)
    assert "--since-time=2026-11-05T12:00:01Z" in shell_pod.follows[0]
    shell_pod.watch.apply(
        _shell_pod_json(_POD, {"terminated": {"exitCode": 0}}, {"terminated": {"exitCode": 0}}), deleted=False
    )
    assert (await run).success


async def test_run_shell_pod_adopted_at_done_reads_out_json(shell_pod):
    shell_pod.watch.apply(
        _shell_pod_json(_POD, {"terminated": {"exitCode": 0}}, {"terminated": {"exitCode": 0}}), deleted=False
    )

    result = await shell_pod.start(image_tag=None, run_id=555)

    assert result.success and [c[1] for c in shell_pod.cli] == ["exec"]


async def test_run_shell_pod_detached_keeps_pod_removes_tmpdir(shell_pod):
    detach = [False]
    run = shell_pod.start(detach=detach)
    await asyncio.sleep(0)
    detach[0] = True
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run

    assert shell_pod.deleted == [] and not shell_pod.io_dir.exists()


async def test_run_shell_pod_nonzero_exit_fails(shell_pod):
    run = shell_pod.start()
    await asyncio.sleep(0)
    shell_pod.watch.apply(
        _shell_pod_json(_POD, {"terminated": {"exitCode": 0}}, {"terminated": {"exitCode": 3}}), deleted=False
    )
    assert (await run) == kw.RunnerResult(False, None, "exit 3")


async def test_run_shell_pod_done_without_pod_terminal_phase(shell_pod):
    """The ``io`` sidecar keeps the Pod ``Running`` forever; the task
    container's exit is what ends the run."""
    run = shell_pod.start()
    await asyncio.sleep(0)
    pod = _shell_pod_json(_POD, {"terminated": {"exitCode": 0}}, {"terminated": {"exitCode": 0}})
    pod["status"]["containerStatuses"].append({"name": "io", "state": {"running": {}}})
    assert pod["status"]["phase"] == "Running"
    shell_pod.watch.apply(pod, deleted=False)
    assert (await run).success


@pytest.mark.parametrize(
    "mode, expected",
    [
        # Only "no such file" is "no result"; any other read failure fails the task.
        pytest.param("missing", kw.RunnerResult(True, None, None), id="missing-out-json"),
        pytest.param(
            "broken",
            kw.RunnerResult(False, None, "reading out.json failed: error: unable to upgrade connection"),
            id="exec-failure",
        ),
    ],
)
async def test_run_shell_pod_out_json_read_failures(shell_pod, mode, expected):
    shell_pod.knobs.exec_mode = mode
    done = _shell_pod_json(_POD, {"terminated": {"exitCode": 0}}, {"terminated": {"exitCode": 0}})
    shell_pod.watch.apply(done, deleted=False)

    assert await shell_pod.start(image_tag=None, run_id=555) == expected


async def test_run_shell_pod_copy_in_failure_fails_the_task(shell_pod, monkeypatch):
    """``kubectl cp`` needs ``pods/exec``; a cluster without it fails the task
    with kubectl's reason rather than parking the Pod in Init forever."""
    monkeypatch.setattr(kw, "COPY_IN_ATTEMPTS", 2)
    monkeypatch.setattr(kw, "POLL_INTERVAL", 0.01)
    shell_pod.knobs.cp_fails = True
    run = shell_pod.start()
    await asyncio.sleep(0)
    shell_pod.watch.apply(_shell_pod_json(_POD, {"running": {}}, {"waiting": {}}), deleted=False)

    result = await run

    assert result == kw.RunnerResult(False, None, "kubectl cp of in.json failed 2 times: pods/exec forbidden")
    assert shell_pod.deleted == [_POD]


async def test_run_shell_pod_jvm_logs_reach_task_logs(orch_ctx, monkeypatch, tmp_path):
    """End to end against the fake ``kubectl`` script: the jvm shim writes no
    logs itself, so the host registers the attempt once the task container
    runs and follows ``kubectl logs -c task`` into task_logs. The class name
    is the shim's one argument."""
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(_FAKE_KUBECTL_SHELL)
    kubectl.chmod(0o755)
    monkeypatch.setenv("AAICLICK_KUBECTL_BIN", str(kubectl))
    monkeypatch.setenv("FAKE_OUT_JSON", "null")
    job = await create_job("jvm_logs", create_task("com.example.Pipeline", entry_type=ENTRY_JVM, image="img:1"))
    (task,) = await get_tasks_for_job(job.id)
    watch = PodWatch("default", str(kubectl))
    name = kw._pod_name(task.id, task.run_epoch)

    def pod(io_in: dict, task_state: dict) -> dict:
        return {
            "metadata": {"name": name, "labels": pod_labels(task.id, task.job_id, task.run_epoch)},
            "spec": {"nodeName": "n1", "containers": [{"name": "task"}]},
            "status": {
                "phase": "Running",
                "initContainerStatuses": [{"name": "io-in", "state": io_in}],
                "containerStatuses": [{"name": "task", "state": task_state}],
            },
        }

    run = asyncio.create_task(
        kw.run_shell_pod(
            task,
            1,
            KubernetesConfig("default", None, None),
            watch,
            image_tag="img:1",
            command=["com.example.Pipeline"],
            command_env=None,
            resources=None,
            run_id=None,
            detach=[False],
        )
    )
    await asyncio.sleep(0.2)
    watch.apply(pod({"running": {}}, {"waiting": {}}), deleted=False)
    await asyncio.sleep(0.2)
    watch.apply(pod({"terminated": {"exitCode": 0}}, {"running": {}}), deleted=False)
    await asyncio.sleep(0.2)
    watch.apply(pod({"terminated": {"exitCode": 0}}, {"terminated": {"exitCode": 0}}), deleted=False)

    result = await run

    assert result == kw.RunnerResult(True, None, None), result
    (task_run,) = await get_task_runs(task.id)
    lines = await read_task_logs(task.id, task_run.run_id)
    assert [(line.stream, line.text) for line in lines] == [("stdout", "pod says hi")]
    applied = json.loads((tmp_path / "applied.json").read_text())
    container = applied["spec"]["containers"][0]
    assert container["args"] == ["com.example.Pipeline"] and "command" not in container
