# Kubernetes Runner — Phase 5: minikube E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** End-to-end test that drives `register-job → run-job → build → Pod → result` against a real minikube cluster, mirroring `test_e2e/docker/`, plus the reusable + nightly CI workflows.

**Architecture:** Reuse the Docker e2e shape. Extract the shared `sample_job` fixture + git-daemon publish helper so both suites use it. The reusable workflow stands up the same services (ClickHouse, Postgres, registry, pypiserver, git daemon) and adds minikube via the validated Phase 0.5 recipe: `--insecure-registry=host.minikube.internal:5000`, runner daemon `insecure-registries` + `/etc/hosts` mapping so `docker push` and the cluster pull hit the same registry, and **wait for CoreDNS** before submitting Pods. Validate on-branch via a temporary `on: push` trigger (like the Phase 0.5 spike); convert to a `schedule` nightly caller once green (cron only fires on `main`).

**Tech Stack:** pytest, minikube (docker driver), kubectl, GitHub Actions.

**Run tests (CI only):** `uv run --no-project pytest test_e2e/kubernetes/ -m kubernetes_e2e -n 0 -v ...`

---

## File Structure

- Move: `test_e2e/docker/fixtures/sample_job/` → `test_e2e/fixtures/sample_job/` (shared).
- Modify: `aaiclick/testing.py` — add `publish_user_repo(tmp_path_factory, fixture_dir)`.
- Modify: `test_e2e/docker/conftest.py` — use the shared helper + shared fixture path.
- Create: `test_e2e/kubernetes/conftest.py`, `test_e2e/kubernetes/test_runner_e2e.py`.
- Create: `.github/workflows/_kubernetes-e2e-reusable.yaml`, `.github/workflows/test-k8s-nightly.yaml`.

---

## Task 1: Extract the shared user-repo fixture

- [ ] **Step 1: Add `publish_user_repo` to `aaiclick/testing.py`**

Add `import subprocess` and `from pathlib import Path` to the imports, then:

```python
def publish_user_repo(tmp_path_factory: pytest.TempPathFactory, fixture_dir: Path) -> tuple[str, str, Path]:
    """Publish ``fixture_dir`` as a bare git repo into the CI git daemon.

    Returns ``(remote_url, commit_sha, worktree)``. Skips when the daemon env
    (``AAICLICK_E2E_GIT_DAEMON_BASE`` / ``_PORT``) is unset — these e2es are
    workflow-driven. Shared by the docker and kubernetes runner suites."""
    base = os.environ.get("AAICLICK_E2E_GIT_DAEMON_BASE")
    port = os.environ.get("AAICLICK_E2E_GIT_DAEMON_PORT")
    if not base or not port:
        pytest.skip("git daemon not configured; this runner e2e is workflow-driven")

    worktree = tmp_path_factory.mktemp("user_repo")
    shutil.copytree(fixture_dir, worktree, dirs_exist_ok=True)

    def git(cwd: Path, *args: str) -> str:
        result = subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True)
        return result.stdout.strip()

    git(worktree, "init", "-q", "-b", "main")
    git(worktree, "add", "-A")
    git(
        worktree, "-c", "user.email=e2e@example.com", "-c", "user.name=e2e",
        "-c", "commit.gpgsign=false", "commit", "-qm", "fixture",
    )
    sha = git(worktree, "rev-parse", "HEAD")

    bare = Path(base) / "sample_job.git"
    git(worktree, "clone", "-q", "--bare", str(worktree), str(bare))
    git(bare, "config", "uploadpack.allowAnySHA1InWant", "true")
    return f"git://127.0.0.1:{port}/sample_job.git", sha, worktree
```

- [ ] **Step 2: Move the fixture dir**

```bash
mkdir -p test_e2e/fixtures
git mv test_e2e/docker/fixtures/sample_job test_e2e/fixtures/sample_job
rmdir test_e2e/docker/fixtures 2>/dev/null || true
```

- [ ] **Step 3: Repoint `test_e2e/docker/conftest.py`**

Replace the inline `docker_e2e_user_repo` body with a call to the shared helper:

```python
from pathlib import Path

import pytest

from aaiclick.testing import (  # noqa: F401
    ch_worker_setup,
    orch_ctx,
    orch_ctx_no_ch,
    orch_module_ctx,
    orch_module_ctx_no_ch,
    publish_user_repo,
    sql_worker_setup,
)

_SAMPLE_JOB = Path(__file__).parent.parent / "fixtures" / "sample_job"


@pytest.fixture(scope="session")
def docker_e2e_user_repo(tmp_path_factory):
    return publish_user_repo(tmp_path_factory, _SAMPLE_JOB)
```

Keep the existing `pytest_configure` (docker_e2e marker) and `pytest_collection_modifyitems` (docker daemon skip) unchanged. Drop now-unused imports (`os`, `subprocess`, `shutil`) if the linter flags them.

- [ ] **Step 4: Import-check both conftests + ruff**

Run: `uv run --no-project python -c "import aaiclick.testing; print(hasattr(aaiclick.testing,'publish_user_repo'))"`
Run: `uv run --no-project ruff check aaiclick/testing.py test_e2e/docker/conftest.py`
Expected: `True`; clean.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/testing.py test_e2e/
git commit -m "Extract shared publish_user_repo helper + move sample_job fixture"
```

---

## Task 2: Kubernetes e2e suite

- [ ] **Step 1: `test_e2e/kubernetes/conftest.py`**

```python
"""Pytest config for the Kubernetes-runner e2e suite.

Workflow-driven (``test_e2e/kubernetes/`` is outside the default testpaths).
Skips unless a cluster is reachable (`kubectl cluster-info`)."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from aaiclick.testing import (  # noqa: F401
    ch_worker_setup,
    orch_ctx,
    orch_ctx_no_ch,
    orch_module_ctx,
    orch_module_ctx_no_ch,
    publish_user_repo,
    sql_worker_setup,
)

_SAMPLE_JOB = Path(__file__).parent.parent / "fixtures" / "sample_job"


@pytest.fixture(scope="session")
def kubernetes_e2e_user_repo(tmp_path_factory):
    return publish_user_repo(tmp_path_factory, _SAMPLE_JOB)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "kubernetes_e2e: end-to-end kubernetes runner tests requiring a real cluster"
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if not items:
        return
    ok = False
    if shutil.which("kubectl"):
        try:
            ok = subprocess.run(["kubectl", "cluster-info"], capture_output=True, timeout=15).returncode == 0
        except (subprocess.TimeoutExpired, OSError):
            ok = False
    if ok:
        return
    skipper = pytest.mark.skip(reason="kubernetes cluster not reachable")
    for item in items:
        if "kubernetes_e2e" in item.keywords:
            item.add_marker(skipper)
```

- [ ] **Step 2: `test_e2e/kubernetes/test_runner_e2e.py`**

Mirror `test_e2e/docker/test_runner_e2e.py` but register with `--runner kubernetes` (no `_wait_for_job`/CLI helper changes — copy them). Full file:

```python
"""End-to-end smoke test for the Kubernetes runner (minikube)."""

from __future__ import annotations

import asyncio
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pytest
from sqlmodel import col, select

from aaiclick.datetime_utils import utc_now
from aaiclick.orchestration.execution.mp_worker import mp_worker_main_loop
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import JOB_COMPLETED, JOB_FAILED, TASK_COMPLETED, Job
from aaiclick.orchestration.orch_context import get_sql_session


def _aaiclick(*args: str, cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "aaiclick", *args], check=True, capture_output=True, text=True, cwd=cwd
    )


async def _wait_for_job(job_name: str, timeout: float = 600.0) -> Job:
    deadline = utc_now() + timedelta(seconds=timeout)
    while utc_now() < deadline:
        async with get_sql_session() as session:
            result = await session.execute(
                select(Job).where(Job.name == job_name).order_by(col(Job.id).desc()).limit(1)
            )
            job = result.scalar_one_or_none()
        if job is not None and job.status in (JOB_COMPLETED, JOB_FAILED):
            return job
        await asyncio.sleep(1.0)
    raise TimeoutError(f"Job {job_name!r} did not complete within {timeout}s")


@pytest.mark.kubernetes_e2e
async def test_kubernetes_runner_smoke(orch_ctx, kubernetes_e2e_user_repo):
    remote, sha, worktree = kubernetes_e2e_user_repo
    job_name = "k8s_e2e_smoke"

    _aaiclick("register-job", "sample_jobs.entry_task", "--name", job_name,
              "--runner", "kubernetes", "--git-remote", remote, cwd=worktree)
    _aaiclick("run-job", job_name, "--git-sha", sha, cwd=worktree)

    worker_task = asyncio.create_task(
        mp_worker_main_loop(max_tasks=10, install_signal_handlers=False, max_empty_polls=10)
    )
    try:
        completed = await _wait_for_job(job_name)
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

    assert completed.status == JOB_COMPLETED, completed.error
    assert completed.image_tag and completed.image_tag.endswith(f":{sha}")

    tasks = await get_tasks_for_job(completed.id)
    non_terminal = [t for t in tasks if t.status != TASK_COMPLETED]
    assert not non_terminal, [(t.entrypoint, t.status, t.error) for t in non_terminal]
    summed = next(t for t in tasks if t.entrypoint == "sample_jobs.compute_sum")
    assert summed.result == {"native_value": {"total": 120}}, summed.result
```

- [ ] **Step 3: Commit**

```bash
git add test_e2e/kubernetes/
git commit -m "Add kubernetes runner e2e suite"
```

---

## Task 3: Reusable workflow + on-branch validation

- [ ] **Step 1: `.github/workflows/_kubernetes-e2e-reusable.yaml`**

Mirror `_docker-e2e-reusable.yaml` (same `services:` block, wheel build/upload, pypiserver, git daemon, migrate) with these k8s-specific changes:
- Map `host.minikube.internal` → `127.0.0.1` in `/etc/hosts`.
- Write `/etc/docker/daemon.json` with `{"insecure-registries":["host.minikube.internal:5000"]}` and restart docker (so host `docker push host.minikube.internal:5000/...` works over HTTP).
- Install minikube + kubectl; `minikube start --driver=docker --insecure-registry="host.minikube.internal:5000" --insecure-registry="10.0.0.0/8" --insecure-registry="172.16.0.0/12" --insecure-registry="192.168.0.0/16"`.
- `kubectl -n kube-system rollout status deploy/coredns --timeout=180s` before the test.
- Test-step env: `AAICLICK_SQL_URL`/`AAICLICK_CH_URL` at `host.minikube.internal`, `AAICLICK_REGISTRY=host.minikube.internal:5000`, `AAICLICK_PIP_INDEX_URL=http://host.minikube.internal:8080/simple/`, `AAICLICK_PIP_TRUSTED_HOST=host.minikube.internal`, `AAICLICK_DOCKER_BUILD_ADD_HOST=host.minikube.internal:host-gateway`.
- Run `uv run --no-project pytest test_e2e/kubernetes/ -m kubernetes_e2e -n 0 -v -o asyncio_mode=auto ... --junitxml=tmp/pytest-report.xml`.

- [ ] **Step 2: Temporary validation caller `.github/workflows/test-k8s-nightly.yaml`**

Start with a push trigger to validate on-branch (convert to schedule once green):

```yaml
name: Kubernetes Runner E2E (nightly)
on:
  push:
    branches: [claude/kubernetes-runner-support-rxv9hm]
    paths: [.github/workflows/_kubernetes-e2e-reusable.yaml, .github/workflows/test-k8s-nightly.yaml,
            test_e2e/kubernetes/**, aaiclick/orchestration/execution/kubernetes_worker.py]
  workflow_dispatch:
jobs:
  e2e:
    uses: ./.github/workflows/_kubernetes-e2e-reusable.yaml
    with:
      wheel_source: source
```

- [ ] **Step 3: Push, watch the run, iterate to green**

Push; watch the `e2e` run. Triage failures from job logs (image pull, DNS, DB reach). Re-push fixes until green.

- [ ] **Step 4: Convert to nightly**

Replace the `push:` trigger with `schedule: - cron: "0 7 * * *"` (keep `workflow_dispatch`). Note in the plan that the cron only fires once merged to `main`.

- [ ] **Step 5: Mark Phase 5 ✅** in `docs/kubernetes_runner_implementation_plan.md`.

---

## Self-Review

- **Spec coverage:** shared fixture (Task 1), k8s e2e suite (Task 2), reusable + nightly workflow with the Phase 0.5 recipe (Task 3).
- **Reuse:** `publish_user_repo` + the `sample_job` fixture are shared with the docker suite; no duplicated fixture project.
- **Validation:** on-branch push trigger proves the reusable before merge; cron activates post-merge (documented).
- **Networking:** `host.minikube.internal` resolves on the runner (`/etc/hosts`) and in pods (minikube CoreDNS); insecure registry configured on both the runner daemon and the cluster; CoreDNS readiness gated — all per the validated spike.
