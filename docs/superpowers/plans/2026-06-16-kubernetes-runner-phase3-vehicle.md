# Kubernetes Runner — Phase 3: Vehicle & Pod Entrypoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Build the execution path for the Kubernetes runner — `kubernetes_worker.py` with a `KubernetesVehicle` (driven by the shared `drive_vehicle`), the Pod-side `_pod_main` entrypoint that writes a `TaskRunResult` row, host-side `kubectl logs` capture, and the `dispatch_execute` branch that routes `RUNNER_KUBERNETES` tasks to it.

**Architecture:** Mirrors `docker_worker.py`. The host builds a bare-Pod manifest (JSON), `kubectl apply`s it, watches the Pod phase, streams its logs to a host-side file, then reads the result back from `task_run_results` (the cross-node replacement for Docker's bind-mounted `result.json`). The Pod runs the task via the shared `runner.execute_task` path and writes one `TaskRunResult` row. `drive_vehicle` is reused verbatim; the result row is read in `wait()` and stashed on a mutable handle (the `mp_worker` pattern) because the `TaskVehicle.collect` hook is synchronous.

**Tech Stack:** Python 3.10, asyncio, `kubectl` via `execution/cli.py`, SQLModel, pytest.

**Scope note:** Phase 3 = execution path only. The submission path (`create_kubernetes_job`, the `run_job` branch) and CLI flags are Phase 4; the minikube e2e is Phase 5. `kubectl`-interacting methods (`launch`/`wait`/`terminate`/`cleanup`) are exercised end-to-end by Phase 5 — Phase 3 unit-tests the deterministic parts (manifest shape, `collect`, `_pod_main`, dispatch routing) by mocking `cli.run` and the DB.

**Log design (corrected):** `capture_task_output` already tees task stdout to the real stdout, so a Pod's task output reaches `kubectl logs` with no pod-side change. The host streams `kubectl logs` into `{log_base}/{job_id}/{task_id}/k8s-{run_epoch}.log` and reports that path (host names it deterministically from claim info — no `run_id` coordination needed).

**Run tests with:**
```bash
uv run --no-project python -m pytest <paths> -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto
```

---

## File Structure

- Create: `aaiclick/orchestration/execution/kubernetes_worker.py` — host vehicle + Pod entrypoint.
- Create: `aaiclick/orchestration/execution/test_kubernetes_worker.py` — unit tests.
- Modify: `aaiclick/orchestration/execution/docker_worker.py` — `dispatch_execute` routes `RUNNER_KUBERNETES`.

---

## Task 1: Pod manifest builder

A pure function — easy to unit-test, like `docker_worker._build_docker_run_cmd`.

**Files:**
- Create: `aaiclick/orchestration/execution/kubernetes_worker.py` (initial)
- Test: `aaiclick/orchestration/execution/test_kubernetes_worker.py`

- [ ] **Step 1: Write the failing test**

Create `aaiclick/orchestration/execution/test_kubernetes_worker.py`:

```python
"""Tests for the Kubernetes runner — manifest, dispatch, collect, pod entrypoint."""

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
        name="n", namespace="default", image_tag="img", task_id=1, run_epoch=0,
        env={}, service_account=None, image_pull_secret=None, resources=None,
    )
    spec = m["spec"]
    assert "serviceAccountName" not in spec
    assert "imagePullSecrets" not in spec
    assert "resources" not in spec["containers"][0]
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/execution/test_kubernetes_worker.py -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: FAIL — `ModuleNotFoundError`/`ImportError` (module not created).

- [ ] **Step 3: Create the module with imports + manifest builder**

Create `aaiclick/orchestration/execution/kubernetes_worker.py`:

```python
"""Kubernetes runner — host-side ExecuteFn and Pod-side entrypoint.

Mirrors ``docker_worker``: ``_run_task_in_pod`` drives a ``KubernetesVehicle``
via the shared ``drive_vehicle``; ``_pod_main`` runs inside the Pod and writes
a ``TaskRunResult`` row — the cross-node equivalent of docker's bind-mounted
``result.json``.

Reaper invariant: the Pod never writes terminal task status. It writes only
its own ``task_run_results`` row; the host worker writes the terminal ``Task``
status via ``_handle_task_result``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import NamedTuple

from sqlmodel import select

from ..logging import get_logs_dir
from ..models import Job, Task, TaskRunResult
from ..orch_context import get_sql_session
from . import cli
from .claiming import check_task_cancelled
from .docker_worker import _build_container_env
from .runner import execute_task, register_returned_tasks, serialize_task_result
from .worker import POLL_INTERVAL, RunnerResult, drive_vehicle, worker_heartbeat

POD_ENTRYPOINT = ["python", "-m", "aaiclick.orchestration.execution.kubernetes_worker"]
# Pod-internal log dir; ephemeral. Host captures logs via `kubectl logs`.
POD_LOG_DIR = "/tmp/aaiclick-logs"


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
    defaults apply."""
    container: dict = {
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
```

- [ ] **Step 4: Run the tests**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/execution/test_kubernetes_worker.py -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/execution/kubernetes_worker.py aaiclick/orchestration/execution/test_kubernetes_worker.py
git commit -m "Add kubernetes_worker pod manifest builder"
```

---

## Task 2: Pod-side `_pod_main` (writes `TaskRunResult`)

Mirrors `docker_worker._container_main`, but writes a DB row instead of a file.

**Files:**
- Modify: `aaiclick/orchestration/execution/kubernetes_worker.py`
- Test: `aaiclick/orchestration/execution/test_kubernetes_worker.py`

- [ ] **Step 1: Write the failing test**

Append to `test_kubernetes_worker.py`:

```python
import pytest

from ..models import Task, TaskRunResult
from ..orch_context import get_sql_session


@pytest.mark.usefixtures("fast_poll")
async def test_pod_main_writes_task_run_result(orch_ctx_no_ch, monkeypatch):
    from ..factories import create_job

    job = await create_job("k8s_pod_main", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    async with get_sql_session() as s:
        task = (await s.execute(select(Task).where(Task.job_id == job.id))).scalar_one()

    rc = await kw._pod_main(task.id, run_epoch=task.run_epoch)
    assert rc == 0

    async with get_sql_session() as s:
        row = (
            await s.execute(
                select(TaskRunResult).where(
                    TaskRunResult.task_id == task.id, TaskRunResult.run_epoch == task.run_epoch
                )
            )
        ).scalar_one()
    assert row.success is True
    assert row.error is None
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/execution/test_kubernetes_worker.py::test_pod_main_writes_task_run_result -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: FAIL — `AttributeError: module ... has no attribute '_pod_main'`.

- [ ] **Step 3: Implement `_pod_main` + CLI**

Append to `kubernetes_worker.py`:

```python
# ---------------------------------------------------------------------------
# Pod-side
# ---------------------------------------------------------------------------


async def _write_task_run_result(
    task_id: int, run_epoch: int, success: bool, result_ref: dict | None, log_path: str | None, error: str | None
) -> None:
    """Upsert the per-attempt result row the host reads back. Keyed by
    ``(task_id, run_epoch)`` so a re-run under a new epoch never collides."""
    async with get_sql_session() as session:
        existing = (
            await session.execute(
                select(TaskRunResult).where(
                    TaskRunResult.task_id == task_id, TaskRunResult.run_epoch == run_epoch
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            existing = TaskRunResult(task_id=task_id, run_epoch=run_epoch, success=success)
            session.add(existing)
        existing.success = success
        existing.result_ref = result_ref
        existing.log_path = log_path
        existing.error = error
        await session.commit()


async def _pod_main(task_id: int, run_epoch: int) -> int:
    """Entry point invoked inside the Pod. Runs the task via the shared
    ``execute_task`` path and writes a ``TaskRunResult`` row for the host."""
    from ..orch_context import orch_context

    success, result_ref, log_path, error = False, None, None, None
    exit_code = 0
    try:
        async with orch_context():
            async with get_sql_session() as session:
                task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()
            data_result, log_path = await execute_task(task)
            data_result = await register_returned_tasks(data_result, task.id, task.job_id)
            result_ref = serialize_task_result(data_result, task.job_id)
            success = True
    except BaseException as e:
        success, error, exit_code = False, f"{type(e).__name__}: {e}", 1

    await _write_task_run_result(task_id, run_epoch, success, result_ref, log_path, error)
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
```

- [ ] **Step 4: Run the test**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/execution/test_kubernetes_worker.py::test_pod_main_writes_task_run_result -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/execution/kubernetes_worker.py aaiclick/orchestration/execution/test_kubernetes_worker.py
git commit -m "Add kubernetes_worker pod entrypoint writing TaskRunResult"
```

---

## Task 3: The `KubernetesVehicle` + `_run_task_in_pod`

`launch`/`wait`/`terminate`/`cleanup` shell out to `kubectl`; `collect` reads the
stashed result row. `wait` polls the Pod phase, streams logs to the host file,
and reads the `TaskRunResult` row (stashing it on the handle for `collect`).

**Files:**
- Modify: `aaiclick/orchestration/execution/kubernetes_worker.py`
- Test: `aaiclick/orchestration/execution/test_kubernetes_worker.py`

- [ ] **Step 1: Write the failing tests (collect behavior)**

Append to `test_kubernetes_worker.py`:

```python
def _handle(task_id=7, run_epoch=1):
    return kw._PodHandle(
        name="aaiclick-task-7-1", namespace="default",
        log_path="/logs/k8s-1.log", task_id=task_id, run_epoch=run_epoch,
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
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/execution/test_kubernetes_worker.py -k collect -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: FAIL — `_PodHandle`/`_KubernetesVehicle` not defined.

- [ ] **Step 3: Implement the host-side vehicle**

Insert into `kubernetes_worker.py` (after the manifest builder, before the Pod-side section):

```python
# ---------------------------------------------------------------------------
# Host-side
# ---------------------------------------------------------------------------


class _PodSpec(NamedTuple):
    image_tag: str
    namespace: str
    service_account: str | None
    image_pull_secret: str | None
    resources: dict | None


async def _fetch_pod_spec(job_id: int) -> _PodSpec:
    async with get_sql_session() as session:
        job = (await session.execute(select(Job).where(Job.id == job_id))).scalar_one_or_none()
    if job is None or not job.image_tag:
        raise ValueError(f"Job {job_id} has no image_tag — was it submitted in kubernetes mode?")
    kc = job.kubernetes_config or {}
    return _PodSpec(
        image_tag=job.image_tag,
        namespace=kc.get("namespace") or "default",
        service_account=kc.get("service_account"),
        image_pull_secret=kc.get("image_pull_secret"),
        resources=kc.get("resources"),
    )


class _PodHandle:
    """Mutable handle: ``wait`` stashes the result row + exit info for ``collect``."""

    def __init__(self, name: str, namespace: str, log_path: str, task_id: int, run_epoch: int) -> None:
        self.name = name
        self.namespace = namespace
        self.log_path = log_path
        self.task_id = task_id
        self.run_epoch = run_epoch
        self.result_row: RunnerResult | None = None


async def _kubectl_delete(handle: _PodHandle) -> None:
    await cli.run(
        _kubectl_bin(), "delete", "pod", handle.name, "-n", handle.namespace, "--ignore-not-found", check=False
    )


async def _pod_phase(handle: _PodHandle) -> str:
    rc, out, _ = await cli.run(
        _kubectl_bin(), "get", "pod", handle.name, "-n", handle.namespace,
        "-o", "jsonpath={.status.phase}", check=False,
    )
    return out.strip()


async def _pod_exit_code(handle: _PodHandle) -> int:
    rc, out, _ = await cli.run(
        _kubectl_bin(), "get", "pod", handle.name, "-n", handle.namespace,
        "-o", "jsonpath={.status.containerStatuses[0].state.terminated.exitCode}", check=False,
    )
    try:
        return int(out.strip())
    except ValueError:
        return -1


async def _capture_pod_logs(handle: _PodHandle) -> None:
    """Authoritative log fetch into the host log file (run once the Pod is
    terminal). The container's task stdout reaches ``kubectl logs`` because
    ``capture_task_output`` tees to stdout."""
    rc, out, _ = await cli.run(_kubectl_bin(), "logs", handle.name, "-n", handle.namespace, check=False)
    Path(handle.log_path).write_text(out)


async def _read_task_run_result_row(task_id: int, run_epoch: int) -> RunnerResult | None:
    async with get_sql_session() as session:
        row = (
            await session.execute(
                select(TaskRunResult).where(
                    TaskRunResult.task_id == task_id, TaskRunResult.run_epoch == run_epoch
                )
            )
        ).scalar_one_or_none()
    if row is None:
        return None
    return RunnerResult(row.success, row.result_ref, row.log_path, row.error)


class _KubernetesVehicle:
    """``TaskVehicle`` for the Kubernetes runner."""

    def __init__(self, spec: _PodSpec, log_base: str) -> None:
        self._spec = spec
        self._log_base = log_base

    async def launch(self, task: Task, worker_id: int) -> _PodHandle:
        env = _build_container_env()
        env["AAICLICK_LOG_DIR"] = POD_LOG_DIR
        name = _pod_name(task.id, task.run_epoch)
        manifest = _build_pod_manifest(
            name=name, namespace=self._spec.namespace, image_tag=self._spec.image_tag,
            task_id=task.id, run_epoch=task.run_epoch, env=env,
            service_account=self._spec.service_account, image_pull_secret=self._spec.image_pull_secret,
            resources=self._spec.resources,
        )
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(manifest, f)
            manifest_path = f.name
        try:
            await cli.run(_kubectl_bin(), "apply", "-f", manifest_path)
        finally:
            os.unlink(manifest_path)
        log_path = os.path.join(self._log_base, str(task.job_id), str(task.id), f"k8s-{task.run_epoch}.log")
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        return _PodHandle(name, self._spec.namespace, log_path, task.id, task.run_epoch)

    async def wait(self, handle: _PodHandle, timeout: float | None) -> tuple[int, str | None]:
        elapsed = 0.0
        error: str | None = None
        while True:
            phase = await _pod_phase(handle)
            if phase in ("Succeeded", "Failed"):
                break
            if timeout is not None and elapsed >= timeout:
                error = f"Task timed out after {timeout}s"
                break
            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
        await _capture_pod_logs(handle)
        handle.result_row = await _read_task_run_result_row(handle.task_id, handle.run_epoch)
        exit_code = await _pod_exit_code(handle) if error is None else -1
        return exit_code, error

    async def poll_cancelled(self, task: Task) -> bool:
        return await check_task_cancelled(task.id)

    async def terminate(self, handle: _PodHandle) -> None:
        await _kubectl_delete(handle)

    def collect(self, handle: _PodHandle, exit_code: int, error: str | None, was_cancelled: bool) -> RunnerResult:
        if was_cancelled:
            return RunnerResult(False, None, None, "cancelled")
        if error is not None:
            return RunnerResult(False, None, None, error)
        if handle.result_row is None:
            return RunnerResult(False, None, None, f"pod exited with code {exit_code} but wrote no result row")
        return handle.result_row

    async def cleanup(self, handle: _PodHandle) -> None:
        await _kubectl_delete(handle)


async def _run_task_in_pod(task: Task, worker_id: int) -> tuple[bool, dict | None, str | None, str | None]:
    """ExecuteFn for the Kubernetes runner."""
    spec = await _fetch_pod_spec(task.job_id)
    raw_timeout = os.environ.get("AAICLICK_TASK_TIMEOUT")
    timeout = float(raw_timeout) if raw_timeout else None
    vehicle = _KubernetesVehicle(spec, get_logs_dir())
    result = await drive_vehicle(
        task, worker_id, vehicle, timeout=timeout, poll_interval=POLL_INTERVAL, heartbeat_fn=worker_heartbeat
    )
    return result.success, result.result_ref, result.log_path, result.error
```

- [ ] **Step 4: Run the collect tests**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/execution/test_kubernetes_worker.py -k collect -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/execution/kubernetes_worker.py aaiclick/orchestration/execution/test_kubernetes_worker.py
git commit -m "Add KubernetesVehicle (kubectl pod lifecycle + log capture + collect)"
```

---

## Task 4: Route `RUNNER_KUBERNETES` in `dispatch_execute`

**Files:**
- Modify: `aaiclick/orchestration/execution/docker_worker.py` (`dispatch_execute`)
- Test: `aaiclick/orchestration/execution/test_docker_worker.py`

- [ ] **Step 1: Write the failing test**

Append to `test_docker_worker.py`:

```python
async def test_dispatch_execute_routes_kubernetes_to_pod_runner(monkeypatch):
    from ..models import RUNNER_KUBERNETES
    from . import kubernetes_worker

    user_task = _task()
    monkeypatch.setattr(docker_worker, "_resolve_runner", AsyncMock(return_value=RUNNER_KUBERNETES))
    in_pod = AsyncMock(return_value=(True, None, None, None))
    monkeypatch.setattr(kubernetes_worker, "_run_task_in_pod", in_pod)

    await docker_worker.dispatch_execute(user_task, worker_id=3)
    in_pod.assert_awaited_once_with(user_task, 3)
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/execution/test_docker_worker.py::test_dispatch_execute_routes_kubernetes_to_pod_runner -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: FAIL — kubernetes tasks currently fall through to the mp child runner.

- [ ] **Step 3: Add the branch in `dispatch_execute`**

In `docker_worker.py`, update `dispatch_execute` (add the import of the runner-mode constant at the top with the others: `from ..models import RUNNER_DOCKER, RUNNER_KUBERNETES, RUNNER_SUBPROCESS, Job, RunnerMode, Task`):

```python
async def dispatch_execute(task: Task, worker_id: int) -> tuple[bool, dict | None, str | None, str | None]:
    """ExecuteFn that picks the runner per task."""
    runner = await _resolve_runner(task)
    if runner == RUNNER_DOCKER:
        return await _run_task_in_container(task, worker_id)
    if runner == RUNNER_KUBERNETES:
        # Imported here to avoid a circular import at module load time —
        # kubernetes_worker imports helpers from docker_worker.
        from .kubernetes_worker import _run_task_in_pod

        return await _run_task_in_pod(task, worker_id)
    from .mp_worker import _run_task_in_child

    return await _run_task_in_child(task, worker_id)
```

- [ ] **Step 4: Run the test (and the whole docker_worker file)**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/execution/test_docker_worker.py -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: PASS (all, including the new routing test).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/execution/docker_worker.py aaiclick/orchestration/execution/test_docker_worker.py
git commit -m "Route kubernetes-mode tasks to the pod runner in dispatch_execute"
```

---

## Task 5: Verification

- [ ] **Step 1: Run the execution suite + lint**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/execution -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: PASS (prior count + the new kubernetes_worker tests).

Run: `uv run --no-project ruff check aaiclick/orchestration/execution/kubernetes_worker.py aaiclick/orchestration/execution/docker_worker.py && uv run --no-project ruff format --check aaiclick/orchestration/execution`
Expected: clean.

- [ ] **Step 2: Push + mark Phase 3 partial in the tracker**

```bash
git push -u origin claude/kubernetes-runner-support-rxv9hm
```

In `docs/kubernetes_runner_implementation_plan.md`, note the execution path is done (vehicle + pod entrypoint + dispatch) and the submission path (`create_kubernetes_job`, `run_job` branch) + CLI moves to Phase 4. Commit.

---

## Self-Review

- **Spec coverage:** vehicle (Task 3), Pod entrypoint writing `TaskRunResult` (Task 2), manifest/bare-Pod `restartPolicy: Never` (Task 1), dispatch routing (Task 4). Log capture is in `wait()` via `kubectl logs` into a host file; the pod-side tee is unnecessary (`capture_task_output` already tees to stdout) — noted in the plan header.
- **Type consistency:** `_PodHandle` carries `task_id`/`run_epoch`/`result_row`; `wait` sets `result_row: RunnerResult | None`, `collect` reads it. `_PodSpec` fields match `_fetch_pod_spec` and `_build_pod_manifest` kwargs.
- **Reaper invariant:** `_pod_main` writes only `task_run_results`; the host `_handle_task_result` writes terminal `Task` status. Documented in the module docstring.
- **Deferred to Phase 5:** `launch`/`wait`/`terminate`/`cleanup` `kubectl` calls are validated end-to-end by the minikube e2e; Phase 3 unit-tests the deterministic seams.
- **Placeholder scan:** none — full code in every step.
