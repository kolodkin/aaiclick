# Kubernetes Runner — Phase 4: Submission Path & CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make a Kubernetes job *submittable* end-to-end — a `create_kubernetes_job` factory, the `run_job` branch that resolves git/image + cluster config, registered-job `kubernetes_config` persistence, and `register-job` CLI flags.

**Architecture:** Mirrors the Docker submission path. `create_docker_job` and `create_kubernetes_job` are thin wrappers over a shared `_create_built_job` (DRY — they differ only in `runner_mode` and the `kubernetes_config` snapshot). `run_job` gains a `RUNNER_KUBERNETES` branch that reuses `resolve_docker_config` for git/image and `resolve_kubernetes_config` for cluster fields. Registered jobs persist cluster defaults in the `kubernetes_config` JSON column; `register-job --runner kubernetes` populates it from `--namespace` / `--k8s-service-account` / `--k8s-image-pull-secret`.

**Tech Stack:** Python 3.10, SQLModel, pytest, uv.

**Scope:** submission + register CLI. Per-run namespace override on `run-job` and resource flags are deferred (note in `docs/future.md` if needed); `run-job --git-sha/--git-remote` already flow through `resolve_docker_config` for k8s. Phase 5 is the minikube e2e.

**Run tests:** `uv run --no-project python -m pytest <paths> -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`

---

## File Structure

- Modify: `aaiclick/orchestration/factories.py` — extract `_create_built_job`; add `create_kubernetes_job`.
- Modify: `aaiclick/orchestration/registered_jobs.py` — `run_job` k8s branch; `upsert_registered_job` gains `kubernetes_config`.
- Modify: `aaiclick/view_models.py` — `RegisterJobRequest.kubernetes_config`.
- Modify: `aaiclick/internal_api/registered_jobs.py` — thread `kubernetes_config`.
- Modify: `aaiclick/__main__.py` — register-job cluster flags.
- Test: `aaiclick/orchestration/test_kubernetes_submission.py`.

---

## Task 1: Shared built-job factory + `create_kubernetes_job`

**Files:** `aaiclick/orchestration/factories.py`; test `aaiclick/orchestration/test_kubernetes_submission.py`

- [ ] **Step 1: Write the failing test**

Create `aaiclick/orchestration/test_kubernetes_submission.py`:

```python
"""Tests for the Kubernetes submission path (factory + register)."""

from __future__ import annotations

import pytest
from sqlmodel import select

from aaiclick.orchestration.docker_config import DockerJobConfig
from aaiclick.orchestration.factories import create_kubernetes_job
from aaiclick.orchestration.models import RUNNER_KUBERNETES, Job, Task
from aaiclick.orchestration.orch_context import get_sql_session

_DOCKER_CFG = DockerJobConfig(
    git_remote="git://x/repo.git",
    git_sha="a" * 40,
    git_branch="main",
    dockerfile=None,
    image_tag="reg/aaiclick-job:" + "a" * 40,
)


@pytest.mark.usefixtures("fast_poll")
async def test_create_kubernetes_job_writes_job_and_build_task(orch_ctx_no_ch):
    job = await create_kubernetes_job(
        name="k8s_submit",
        entrypoint="sample_jobs.entry",
        docker_config=_DOCKER_CFG,
        kubernetes_config={"namespace": "ml"},
    )
    assert job.runner_mode == RUNNER_KUBERNETES
    assert job.image_tag == _DOCKER_CFG.image_tag
    assert job.kubernetes_config == {"namespace": "ml"}

    async with get_sql_session() as session:
        tasks = (await session.execute(select(Task).where(Task.job_id == job.id))).scalars().all()
    entrypoints = {t.entrypoint for t in tasks}
    assert "aaiclick.orchestration.execution.docker_build.build_image" in entrypoints
    assert "sample_jobs.entry" in entrypoints
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/test_kubernetes_submission.py -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: FAIL — `ImportError: cannot import name 'create_kubernetes_job'`.

- [ ] **Step 3: Refactor `create_docker_job` into a shared helper + add `create_kubernetes_job`**

In `factories.py`, add `RUNNER_KUBERNETES` to the `from .models import (...)` block (alongside `RUNNER_DOCKER`). Replace the existing `create_docker_job` with a shared private helper and two wrappers:

```python
async def _create_built_job(
    *,
    name: str,
    entrypoint: str,
    runner_mode: RunnerMode,
    docker_config: DockerJobConfig,
    kubernetes_config: dict | None = None,
    kwargs: dict | None = None,
    run_type: RunType = RUN_MANUAL,
    registered_job_id: int | None = None,
    preservation_mode: PreservationMode | None = None,
    registered: RegisteredJob | None = None,
) -> Job:
    """Create an image-built Job (docker or kubernetes) + the auto-injected
    build task, in one transaction. The build task is identical for both
    runners; they differ only in ``runner_mode`` and the k8s config snapshot."""
    mode = resolve_job_config(preservation_mode, registered)

    job_id = get_snowflake_id()
    job = Job(
        id=job_id,
        name=name,
        status=JOB_PENDING,
        run_type=run_type,
        registered_job_id=registered_job_id,
        preservation_mode=mode,
        runner_mode=runner_mode,
        git_remote=docker_config.git_remote,
        git_sha=docker_config.git_sha,
        git_branch=docker_config.git_branch,
        dockerfile=docker_config.dockerfile,
        image_tag=docker_config.image_tag,
        kubernetes_config=kubernetes_config,
        created_at=utc_now(),
    )

    build_task = create_task(BUILD_TASK_ENTRYPOINT, {"job_id": job_id}, name="docker_build", max_retries=2)
    build_task.job_id = job_id

    entry_task = create_task(entrypoint, kwargs or {}, name=name)
    entry_task.job_id = job_id
    entry_task.depends_on(build_task)

    async with get_sql_session() as session:
        session.add(job)
        session.add(build_task)
        session.add(entry_task)
        await session.commit()

    registry = get_task_registry()
    if registry is not None:
        registry.pop(build_task.id, None)
        registry.pop(entry_task.id, None)

    return job


async def create_docker_job(
    *,
    name: str,
    entrypoint: str,
    kwargs: dict | None = None,
    run_type: RunType = RUN_MANUAL,
    registered_job_id: int | None = None,
    preservation_mode: PreservationMode | None = None,
    registered: RegisteredJob | None = None,
    docker_config: DockerJobConfig,
) -> Job:
    """Create a Docker-mode Job along with the auto-injected build task."""
    return await _create_built_job(
        name=name,
        entrypoint=entrypoint,
        runner_mode=RUNNER_DOCKER,
        docker_config=docker_config,
        kwargs=kwargs,
        run_type=run_type,
        registered_job_id=registered_job_id,
        preservation_mode=preservation_mode,
        registered=registered,
    )


async def create_kubernetes_job(
    *,
    name: str,
    entrypoint: str,
    kwargs: dict | None = None,
    run_type: RunType = RUN_MANUAL,
    registered_job_id: int | None = None,
    preservation_mode: PreservationMode | None = None,
    registered: RegisteredJob | None = None,
    docker_config: DockerJobConfig,
    kubernetes_config: dict | None = None,
) -> Job:
    """Create a Kubernetes-mode Job along with the auto-injected build task.

    Reuses the Docker build pipeline (``docker_config``) for the image; the
    cluster config snapshot is stored on ``Job.kubernetes_config``."""
    return await _create_built_job(
        name=name,
        entrypoint=entrypoint,
        runner_mode=RUNNER_KUBERNETES,
        docker_config=docker_config,
        kubernetes_config=kubernetes_config,
        kwargs=kwargs,
        run_type=run_type,
        registered_job_id=registered_job_id,
        preservation_mode=preservation_mode,
        registered=registered,
    )
```

Add `RunnerMode` to the `from .models import (...)` block if not already imported.

- [ ] **Step 4: Run the test**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/test_kubernetes_submission.py -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/factories.py aaiclick/orchestration/test_kubernetes_submission.py
git commit -m "Add create_kubernetes_job via shared _create_built_job factory"
```

---

## Task 2: `run_job` Kubernetes branch + registered `kubernetes_config`

**Files:** `aaiclick/orchestration/registered_jobs.py`; test in `test_kubernetes_submission.py`

- [ ] **Step 1: Add the failing register round-trip test**

Append to `test_kubernetes_submission.py`:

```python
from aaiclick.orchestration.registered_jobs import get_registered_job, upsert_registered_job


@pytest.mark.usefixtures("fast_poll")
async def test_upsert_registered_job_persists_kubernetes_config(orch_ctx_no_ch):
    await upsert_registered_job(
        name="k8s_reg",
        entrypoint="sample_jobs.entry",
        runner_mode=RUNNER_KUBERNETES,
        kubernetes_config={"namespace": "ml", "service_account": "sa"},
    )
    reg = await get_registered_job("k8s_reg")
    assert reg is not None
    assert reg.runner_mode == RUNNER_KUBERNETES
    assert reg.kubernetes_config == {"namespace": "ml", "service_account": "sa"}
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/test_kubernetes_submission.py::test_upsert_registered_job_persists_kubernetes_config -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: FAIL — `upsert_registered_job() got an unexpected keyword argument 'kubernetes_config'`.

- [ ] **Step 3: Add `kubernetes_config` to `upsert_registered_job`**

In `registered_jobs.py`, add the parameter `kubernetes_config: dict[str, Any] | None = None` to `upsert_registered_job`'s signature (after `git_remote`). In the **existing-update** branch add `existing.kubernetes_config = kubernetes_config`, and in the **new-create** `RegisteredJob(...)` constructor add `kubernetes_config=kubernetes_config`.

- [ ] **Step 4: Add the `RUNNER_KUBERNETES` branch to `run_job`**

In `registered_jobs.py`, import the helpers at the top (with the existing imports):

```python
from .factories import create_docker_job, create_job, create_kubernetes_job, create_task
from .kubernetes_config import resolve_kubernetes_config
from .models import (
    RUNNER_DOCKER,
    RUNNER_KUBERNETES,
    RUNNER_SUBPROCESS,
    ...
)
```

In `run_job`, after the `RUNNER_DOCKER` block, add:

```python
    if runner_mode == RUNNER_KUBERNETES:
        if is_local():
            raise ValueError(
                "Kubernetes runner requires distributed mode (Postgres + ClickHouse); "
                "got chdb + SQLite. Set AAICLICK_SQL_URL and AAICLICK_CH_URL to "
                "remote services before submitting kubernetes-runner jobs."
            )
        docker_config = await resolve_docker_config(
            registered, git_remote=git_remote, git_sha=git_sha, git_branch=git_branch, dockerfile=dockerfile
        )
        kube = resolve_kubernetes_config(registered)
        return await create_kubernetes_job(
            name=name,
            entrypoint=entrypoint,
            kwargs=merged_kwargs,
            run_type=run_type,
            registered_job_id=registered.id if registered is not None else None,
            preservation_mode=preservation_mode,
            registered=registered,
            docker_config=docker_config,
            kubernetes_config=kube._asdict(),
        )
```

- [ ] **Step 5: Run the tests**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/test_kubernetes_submission.py -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/registered_jobs.py aaiclick/orchestration/test_kubernetes_submission.py
git commit -m "Wire run_job kubernetes branch + registered kubernetes_config"
```

---

## Task 3: Thread `kubernetes_config` through register API

**Files:** `aaiclick/view_models.py`, `aaiclick/internal_api/registered_jobs.py`

- [ ] **Step 1: Add the field to `RegisterJobRequest`**

In `view_models.py`, in `RegisterJobRequest` (after `git_remote`):

```python
    kubernetes_config: dict[str, Any] | None = None
```

(`Any` is already imported in `view_models.py`.)

- [ ] **Step 2: Pass it through `internal_api.register_job`**

In `aaiclick/internal_api/registered_jobs.py`, in the `_register_job_impl(...)` call inside `register_job`, add:

```python
            kubernetes_config=request.kubernetes_config,
```

- [ ] **Step 3: Verify the register API suite still passes**

Run: `uv run --no-project python -m pytest aaiclick/internal_api -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto -k "register"`
Expected: PASS (no regressions; existing register tests unaffected by the optional field).

- [ ] **Step 4: Commit**

```bash
git add aaiclick/view_models.py aaiclick/internal_api/registered_jobs.py
git commit -m "Thread kubernetes_config through the register-job API"
```

---

## Task 4: `register-job` CLI cluster flags

**Files:** `aaiclick/__main__.py`

- [ ] **Step 1: Add the argparse flags**

In `__main__.py`, on the `register_job_parser` (near the existing `--dockerfile` / `--git-remote` flags), add:

```python
    register_job_parser.add_argument(
        "--namespace", default=None, help="Kubernetes namespace (kubernetes runner only)"
    )
    register_job_parser.add_argument(
        "--k8s-service-account", default=None, help="Kubernetes service account (kubernetes runner only)"
    )
    register_job_parser.add_argument(
        "--k8s-image-pull-secret", default=None, help="Kubernetes imagePullSecret name (kubernetes runner only)"
    )
```

- [ ] **Step 2: Build `kubernetes_config` in `_run_register_job`**

In `_run_register_job`, before building `RegisterJobRequest`:

```python
    kubernetes_config = {
        k: v
        for k, v in {
            "namespace": args.namespace,
            "service_account": args.k8s_service_account,
            "image_pull_secret": args.k8s_image_pull_secret,
        }.items()
        if v is not None
    } or None
```

and pass `kubernetes_config=kubernetes_config` into `RegisterJobRequest(...)`.

- [ ] **Step 3: Smoke-check the CLI parses**

Run: `uv run --no-project python -m aaiclick register-job --help 2>&1 | grep -E "namespace|image-pull-secret"`
Expected: the three new flags appear.

- [ ] **Step 4: Commit**

```bash
git add aaiclick/__main__.py
git commit -m "Add register-job kubernetes cluster flags"
```

---

## Task 5: Verification

- [ ] **Step 1: Full suites + lint**

Run: `uv run --no-project python -m pytest aaiclick/orchestration aaiclick/internal_api -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: PASS (prior + new submission tests).

Run: `uv run --no-project ruff check aaiclick/orchestration aaiclick/internal_api aaiclick/__main__.py aaiclick/view_models.py && uv run --no-project ruff format --check aaiclick`
Expected: clean.

- [ ] **Step 2: Push + mark Phase 4 done**

```bash
git push -u origin claude/kubernetes-runner-support-rxv9hm
```

Mark Phase 4 ✅ in `docs/kubernetes_runner_implementation_plan.md` (reference `factories.create_kubernetes_job`, the `run_job` branch). Commit.

---

## Self-Review

- **Spec coverage:** factory (Task 1), run_job branch + registered config (Task 2), register API (Task 3), CLI (Task 4). The submission path is complete; per-run namespace override + resource flags deferred (noted).
- **DRY:** `create_docker_job` / `create_kubernetes_job` share `_create_built_job` — no near-duplicate. Image build/git resolution reuse `resolve_docker_config`.
- **Type consistency:** `create_kubernetes_job(..., docker_config, kubernetes_config)`; `resolve_kubernetes_config(registered)._asdict()` → the `kubernetes_config` JSON; `upsert_registered_job(..., kubernetes_config=...)`.
- **Placeholder scan:** none.
- **is_local guard:** kubernetes branch refuses local mode, mirroring docker.
