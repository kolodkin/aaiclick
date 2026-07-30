# Per-Task Image Requirement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the container image from a job-level setting to a per-task requirement (`tasks.image_source`), coordinate registry-mode builds with ordinary graph dependency edges on an injected build task, and delete the `build_tasks` claim/lease/poll machinery.

**Architecture:** Spec is `docs/designs/task_image.md` — read it first. Tasks carry a nullable `image_source` JSON (`ImageBuild`/`ImagePrebuilt` union; NULL ⇒ host subprocess). Commit points stamp inheritance and, in registry mode, inject one ordinary build task per distinct image with `build >> dependent` edges; the existing scheduler dependency filter does all gating. No registry ⇒ inline build at docker launch. Each plan task keeps the tree green: additive tasks first, then the switch-over, then retirement of the old machinery.

**Tech Stack:** Python 3.12, SQLModel/SQLAlchemy async, Pydantic discriminated unions, pytest (+pytest-asyncio auto mode), Alembic via the `generate-migration` skill.

## Global Constraints

- ALL imports at top of file, three groups (stdlib / external / package). No inline imports, no `TYPE_CHECKING`.
- No `Any` shortcuts; `Literal` over enums; NamedTuples over plain tuples in APIs.
- Tests: flat module-level functions, no `@pytest.mark.asyncio` (auto mode), files alongside module under test. Don't test plain assignment/defaults.
- Never hand-write Alembic migrations — use the `generate-migration` skill (GitHub Actions).
- Test schemas come from `SQLModel.metadata.create_all` (see `aaiclick/testing.py`), so model changes are immediately test-visible without a migration.
- No history comments (`# Removed: ...`). No `__all__` in `__init__.py`.
- Commit after every task; push at the end of each task.
- Docker/kubernetes jobs require distributed mode; unit tests here mock the docker CLI layer (`build_image_to_tag`) — never invoke real docker.

---

### Task 1: `Task.image_source` column + image-source JSON helpers

**Files:**
- Modify: `aaiclick/orchestration/runner_config.py`
- Modify: `aaiclick/orchestration/models.py` (Task model)
- Test: `aaiclick/orchestration/test_runner_config.py` (create if missing; check `ls aaiclick/orchestration/test_runner_config.py` first)

**Interfaces:**
- Produces: `parse_image_source(data: dict) -> ImageSourceT`, `dump_image_source(source: ImageSourceT) -> dict` in `runner_config.py`; `Task.image_source: dict[str, Any] | None` JSON column.

- [ ] **Step 1: Write the failing test**

Append to (or create) `aaiclick/orchestration/test_runner_config.py`:

```python
import pytest
from pydantic import ValidationError

from .runner_config import ImageBuild, ImagePrebuilt, dump_image_source, parse_image_source


def test_image_source_round_trip_build():
    source = ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40, dockerfile="Dockerfile.gpu")
    parsed = parse_image_source(dump_image_source(source))
    assert isinstance(parsed, ImageBuild)
    assert parsed.git_sha == "a" * 40


def test_image_source_round_trip_prebuilt():
    parsed = parse_image_source(dump_image_source(ImagePrebuilt(image_tag="ghcr.io/x/y:1")))
    assert isinstance(parsed, ImagePrebuilt)
    assert parsed.image_tag == "ghcr.io/x/y:1"


def test_parse_image_source_rejects_unknown_type():
    with pytest.raises(ValidationError):
        parse_image_source({"type": "carrier-pigeon"})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/test_runner_config.py -v`
Expected: FAIL with `ImportError: cannot import name 'parse_image_source'`

- [ ] **Step 3: Implement**

In `runner_config.py`, next to `parse_runner_config`/`dump_runner_config` (the `_IMAGE_ADAPTER` already exists):

```python
def parse_image_source(data: dict) -> ImageSourceT:
    """Validate a JSON dict into the matching image-source model."""
    return _IMAGE_ADAPTER.validate_python(data)


def dump_image_source(source: ImageSourceT) -> dict:
    """Serialize an image-source model to a JSON-safe dict for the DB column."""
    return _IMAGE_ADAPTER.dump_python(source, mode="json")
```

In `models.py`, add to `Task` (after `command_env`):

```python
image_source: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON, nullable=True))
```

- [ ] **Step 4: Run tests**

Run: `pytest aaiclick/orchestration/test_runner_config.py aaiclick/orchestration/test_dependencies.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/runner_config.py aaiclick/orchestration/models.py aaiclick/orchestration/test_runner_config.py
git commit -m "Add tasks.image_source column and image-source JSON helpers"
```

---

### Task 2: Build-task body module (`image_build_task.py`)

**Files:**
- Create: `aaiclick/orchestration/execution/image_build_task.py`
- Test: `aaiclick/orchestration/execution/test_image_build_task.py`

**Interfaces:**
- Consumes: `build_image_to_tag(source: ImageBuild, image_tag: str)` from `docker_build.py`; `compute_image_tag(git_sha: str) -> str` from `docker_config.py`.
- Produces: `IMAGE_BUILD_ENTRYPOINT: str` (module constant, value `"aaiclick.orchestration.execution.image_build_task.run_image_build"`), `is_image_build_task(entrypoint: str) -> bool`, `async run_image_build(*, image_key: str, git_remote: str, git_sha: str, git_branch: str | None = None, dockerfile: str | None = None) -> None`, `build_task_name(git_sha: str) -> str` (returns `f"build-image:{git_sha[:8]}"`).

- [ ] **Step 1: Write the failing test**

Create `aaiclick/orchestration/execution/test_image_build_task.py`:

```python
"""Tests for the registry-mode image-build task body."""

import pytest

from ..runner_config import ImageBuild
from . import image_build_task
from .image_build_task import IMAGE_BUILD_ENTRYPOINT, build_task_name, is_image_build_task, run_image_build


def test_is_image_build_task_matches_only_the_constant():
    assert is_image_build_task(IMAGE_BUILD_ENTRYPOINT)
    assert not is_image_build_task("myapp.pipelines.etl_job")


def test_build_task_name_uses_short_sha():
    assert build_task_name("abcdef1234" + "0" * 30) == "build-image:abcdef12"


async def test_run_image_build_delegates_to_build_image_to_tag(monkeypatch):
    calls: list[tuple[ImageBuild, str]] = []

    async def fake_build(source: ImageBuild, image_tag: str) -> None:
        calls.append((source, image_tag))

    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    monkeypatch.setattr(image_build_task, "build_image_to_tag", fake_build)
    await run_image_build(image_key="k1", git_remote="https://example.com/r.git", git_sha="a" * 40)
    source, tag = calls[0]
    assert source.git_sha == "a" * 40
    assert tag == "registry.example:5000/aaiclick-job:" + "a" * 40


async def test_run_image_build_requires_registry(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    with pytest.raises(RuntimeError, match="AAICLICK_REGISTRY"):
        await run_image_build(image_key="k1", git_remote="https://example.com/r.git", git_sha="a" * 40)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/execution/test_image_build_task.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'aaiclick.orchestration.execution.image_build_task'`

- [ ] **Step 3: Implement**

Create `aaiclick/orchestration/execution/image_build_task.py`:

```python
"""Registry-mode image-build task body.

An ordinary module task injected at commit points (see
``orchestration.image_injection``) for every distinct build-source image in a
docker/kubernetes job. It runs on the dispatching worker host
(``image_source=NULL`` ⇒ subprocess vehicle) because it needs the docker CLI
and daemon socket. The body is pull-first via ``build_image_to_tag``: pull
from the registry (someone already pushed this SHA → done), else clone +
build + push. Cross-job dedup is the registry itself — a lost race
double-builds, which is wasteful but correct.
"""

from __future__ import annotations

import os

from ..docker_config import compute_image_tag
from ..runner_config import ImageBuild
from .docker_build import build_image_to_tag

IMAGE_BUILD_ENTRYPOINT = "aaiclick.orchestration.execution.image_build_task.run_image_build"


def is_image_build_task(entrypoint: str) -> bool:
    """True for the injected image-build task; drives UI styling and the
    per-job injection dedup lookup."""
    return entrypoint == IMAGE_BUILD_ENTRYPOINT


def build_task_name(git_sha: str) -> str:
    """Display name for an injected build task."""
    return f"build-image:{git_sha[:8]}"


async def run_image_build(
    *,
    image_key: str,
    git_remote: str,
    git_sha: str,
    git_branch: str | None = None,
    dockerfile: str | None = None,
) -> None:
    """Ensure the image for these build coordinates is pushed to the registry.

    ``image_key`` is carried in kwargs for the injection dedup lookup; the
    body itself only needs the build coordinates. Build tasks are only
    injected when submission saw ``AAICLICK_REGISTRY``; a worker without it
    would build an image no other host could use, so fail loudly on the
    env-layer mismatch instead.
    """
    if not os.environ.get("AAICLICK_REGISTRY"):
        raise RuntimeError(
            "image build task requires AAICLICK_REGISTRY on the worker; "
            "submission-side and worker-side env must agree (see docs/designs/task_image.md)"
        )
    source = ImageBuild(git_remote=git_remote, git_sha=git_sha, git_branch=git_branch, dockerfile=dockerfile)
    await build_image_to_tag(source, compute_image_tag(git_sha))
```

- [ ] **Step 4: Run tests**

Run: `pytest aaiclick/orchestration/execution/test_image_build_task.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/execution/image_build_task.py aaiclick/orchestration/execution/test_image_build_task.py
git commit -m "Add registry-mode image-build task body"
```

---

### Task 3: Commit-time stamping/validation/injection module (`image_injection.py`)

**Files:**
- Create: `aaiclick/orchestration/image_injection.py`
- Test: `aaiclick/orchestration/test_image_injection.py`

**Interfaces:**
- Consumes: `image_key(source: ImageBuild) -> str` from `docker_config.py`; `IMAGE_BUILD_ENTRYPOINT`, `build_task_name` from `execution/image_build_task.py`; `parse_image_source`/`dump_image_source` from `runner_config.py`; models `Task`, `Job`, `Dependency`.
- Produces (all in `image_injection.py`):
  - `stamp_inherited_image(tasks: list[Task], parent_image_source: dict | None) -> None`
  - `validate_image_sources(tasks: list[Task], runner_mode: RunnerMode) -> None` (raises `ValueError`)
  - `async inject_build_tasks(session: AsyncSession, tasks: list[Task], job: Job) -> list[Task]` — returns newly created build tasks (caller adds them to the session/commit batch)
  - `BUILD_TASK_MAX_RETRIES = 2`

Import-cycle note: this module must NOT import `factories` or `orch_context` (both will import it, directly or transitively). It builds `Task` rows directly via `models.Task` + `get_snowflake_id`.

- [ ] **Step 1: Write the failing test**

Create `aaiclick/orchestration/test_image_injection.py`. Fixture note: reuse the same DB fixture style as `aaiclick/orchestration/test_dependencies.py` — open that file first and copy its fixture usage exactly (it provides an orch context with SQLite). The tests below assume a fixture named as in that file; adjust the fixture argument name to match, nothing else.

```python
"""Tests for commit-time image stamping, validation, and build-task injection."""

import pytest
from sqlmodel import select

from .execution.image_build_task import IMAGE_BUILD_ENTRYPOINT
from .factories import create_job, create_task
from .image_injection import inject_build_tasks, stamp_inherited_image, validate_image_sources
from .models import RUNNER_DOCKER, RUNNER_SUBPROCESS, Dependency, Job, Task
from .orch_context import get_sql_session
from .runner_config import ImageBuild, dump_image_source

BUILD_A = dump_image_source(ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40))
BUILD_B = dump_image_source(ImageBuild(git_remote="https://example.com/r.git", git_sha="b" * 40))


def test_stamp_inherited_image_fills_only_undeclared():
    declared = create_task("m.f1")
    declared.image_source = BUILD_B
    inherited = create_task("m.f2")
    stamp_inherited_image([declared, inherited], BUILD_A)
    assert declared.image_source == BUILD_B
    assert inherited.image_source == BUILD_A


def test_stamp_inherited_image_none_parent_is_noop():
    t = create_task("m.f")
    stamp_inherited_image([t], None)
    assert t.image_source is None


def test_validate_rejects_image_on_subprocess_job():
    t = create_task("m.f")
    t.image_source = BUILD_A
    with pytest.raises(ValueError, match="subprocess"):
        validate_image_sources([t], RUNNER_SUBPROCESS)


def test_validate_rejects_kubernetes_build_without_registry(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    t = create_task("m.f")
    t.image_source = BUILD_A
    with pytest.raises(ValueError, match="AAICLICK_REGISTRY"):
        validate_image_sources([t], "kubernetes")


async def test_inject_creates_one_build_task_per_image(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    job = await create_job("j", "m.entry")
    async with get_sql_session() as session:
        row = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        row.runner_mode = RUNNER_DOCKER
        t1, t2, t3 = create_task("m.f1"), create_task("m.f2"), create_task("m.f3")
        t1.image_source, t2.image_source, t3.image_source = BUILD_A, BUILD_A, BUILD_B
        for t in (t1, t2, t3):
            t.job_id = job.id
        injected = await inject_build_tasks(session, [t1, t2, t3], row)
        assert len(injected) == 2
        assert all(b.entrypoint == IMAGE_BUILD_ENTRYPOINT and b.image_source is None for b in injected)
        assert all(b.max_retries == 2 for b in injected)
        # every dependent got an edge to its image's build task
        edges = {(d.previous_id, d.next_id) for t in (t1, t2, t3) for d in t.previous_dependencies}
        by_sha = {b.kwargs["git_sha"]: b.id for b in injected}
        assert (by_sha["a" * 40], t1.id) in edges
        assert (by_sha["a" * 40], t2.id) in edges
        assert (by_sha["b" * 40], t3.id) in edges


async def test_inject_dedups_against_existing_build_task_in_job(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    job = await create_job("j", "m.entry")
    async with get_sql_session() as session:
        row = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        row.runner_mode = RUNNER_DOCKER
        first = create_task("m.f1")
        first.image_source = BUILD_A
        first.job_id = job.id
        injected1 = await inject_build_tasks(session, [first], row)
        for obj in (*injected1, first):
            session.add(obj)
        await session.commit()
    async with get_sql_session() as session:
        row = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        second = create_task("m.f2")
        second.image_source = BUILD_A
        second.job_id = job.id
        injected2 = await inject_build_tasks(session, [second], row)
        assert injected2 == []  # existing build task reused
        assert second.previous_dependencies[0].previous_id == injected1[0].id


async def test_inject_noop_without_registry(orch_ctx_no_ch, monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    job = await create_job("j", "m.entry")
    async with get_sql_session() as session:
        row = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        row.runner_mode = RUNNER_DOCKER
        t = create_task("m.f")
        t.image_source = BUILD_A
        t.job_id = job.id
        assert await inject_build_tasks(session, [t], row) == []
        assert t.previous_dependencies == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/test_image_injection.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'aaiclick.orchestration.image_injection'`

- [ ] **Step 3: Implement**

Create `aaiclick/orchestration/image_injection.py`:

```python
"""Commit-time image stamping, validation, and build-task injection.

Called from every commit point (``orch_context.commit_tasks`` and
``factories.create_built_job``) so a committed task's ``image_source`` is
final by the time its row lands — dispatch never resolves inheritance. In
registry mode it injects one ordinary build task per distinct build image
and wires ``build >> dependent`` edges; the scheduler's existing dependency
filter is the whole coordination story (spec: docs/designs/task_image.md).

Deliberately imports neither ``factories`` nor ``orch_context`` (both reach
this module), building ``Task`` rows directly instead.
"""

from __future__ import annotations

import os

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..datetime_utils import utc_now
from ..snowflake import get_snowflake_id
from .docker_config import image_key
from .execution.image_build_task import IMAGE_BUILD_ENTRYPOINT, build_task_name
from .models import (
    DEPENDENCY_TASK,
    RUNNER_DOCKER,
    RUNNER_KUBERNETES,
    TASK_PENDING,
    Dependency,
    Job,
    RunnerMode,
    Task,
)
from .runner_config import ImageBuild, parse_image_source

BUILD_TASK_MAX_RETRIES = 2


def stamp_inherited_image(tasks: list[Task], parent_image_source: dict | None) -> None:
    """Fill ``image_source`` on tasks that didn't declare their own.

    ``parent_image_source`` is the committing task's own stamped value (or
    None outside task execution / for a NULL-image parent, in which case
    undeclared children stay NULL ⇒ host subprocess)."""
    if parent_image_source is None:
        return
    for task in tasks:
        if task.image_source is None:
            task.image_source = parent_image_source


def validate_image_sources(tasks: list[Task], runner_mode: RunnerMode) -> None:
    """Enforce the spec's commit-point rules; raises ``ValueError``."""
    for task in tasks:
        if task.image_source is None:
            continue
        if runner_mode not in (RUNNER_DOCKER, RUNNER_KUBERNETES):
            raise ValueError(
                f"task {task.name!r} declares an image_source but the job's runner_mode "
                f"is {runner_mode!r}; images are only valid on docker/kubernetes jobs"
            )
        source = parse_image_source(task.image_source)
        if (
            isinstance(source, ImageBuild)
            and runner_mode == RUNNER_KUBERNETES
            and not os.environ.get("AAICLICK_REGISTRY")
        ):
            raise ValueError(
                "kubernetes build image sources require AAICLICK_REGISTRY — "
                "the cluster cannot pull from a worker's local docker daemon"
            )


def _make_build_task(source: ImageBuild, key: str, job_id: int) -> Task:
    return Task(
        id=get_snowflake_id(),
        job_id=job_id,
        entrypoint=IMAGE_BUILD_ENTRYPOINT,
        name=build_task_name(source.git_sha),
        kwargs={
            "image_key": key,
            "git_remote": source.git_remote,
            "git_sha": source.git_sha,
            "git_branch": source.git_branch,
            "dockerfile": source.dockerfile,
        },
        status=TASK_PENDING,
        created_at=utc_now(),
        max_retries=BUILD_TASK_MAX_RETRIES,
    )


async def inject_build_tasks(session: AsyncSession, tasks: list[Task], job: Job) -> list[Task]:
    """Ensure a build task exists per distinct build image and wire
    ``build >> dependent`` edges onto ``tasks``.

    Registry mode + docker/kubernetes jobs only. Returns newly created build
    tasks — the caller commits them alongside ``tasks``. Two concurrent
    commits in one job can race past the lookup and double-inject; both
    builds are pull-first so the loser is a cheap no-op (accepted, spec
    "Races")."""
    if not os.environ.get("AAICLICK_REGISTRY"):
        return []
    if job.runner_mode not in (RUNNER_DOCKER, RUNNER_KUBERNETES):
        return []

    dependents_by_key: dict[str, list[Task]] = {}
    source_by_key: dict[str, ImageBuild] = {}
    for task in tasks:
        if task.image_source is None or task.entrypoint == IMAGE_BUILD_ENTRYPOINT:
            continue
        source = parse_image_source(task.image_source)
        if not isinstance(source, ImageBuild):
            continue
        key = image_key(source)
        dependents_by_key.setdefault(key, []).append(task)
        source_by_key[key] = source
    if not dependents_by_key:
        return []

    existing = (
        (
            await session.execute(
                select(Task).where(Task.job_id == job.id, Task.entrypoint == IMAGE_BUILD_ENTRYPOINT)
            )
        )
        .scalars()
        .all()
    )
    build_by_key: dict[str, Task] = {t.kwargs["image_key"]: t for t in existing}

    injected: list[Task] = []
    for key, dependents in dependents_by_key.items():
        build = build_by_key.get(key)
        if build is None:
            build = _make_build_task(source_by_key[key], key, job.id)
            build_by_key[key] = build
            injected.append(build)
        for task in dependents:
            task.previous_dependencies.append(
                Dependency(
                    previous_id=build.id,
                    previous_type=DEPENDENCY_TASK,
                    next_id=task.id,
                    next_type=DEPENDENCY_TASK,
                )
            )
    return injected
```

Check `DEPENDENCY_TASK` exists in `models.py` (it is used there as `DEPENDENCY_TASK`/`DEPENDENCY_GROUP`); if the constant lives elsewhere, import from where `models.py` gets it.

- [ ] **Step 4: Run tests**

Run: `pytest aaiclick/orchestration/test_image_injection.py -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/image_injection.py aaiclick/orchestration/test_image_injection.py
git commit -m "Add commit-time image stamping, validation, and build-task injection"
```

---

### Task 4: Wire commit points (`commit_tasks` + `create_built_job` + entry stamping)

**Files:**
- Modify: `aaiclick/orchestration/orch_context.py` (`commit_tasks`)
- Modify: `aaiclick/orchestration/factories.py` (`create_built_job`)
- Modify: `aaiclick/orchestration/execution/runner_env.py` (`ALWAYS_PASSED_ENV_VARS`)
- Test: `aaiclick/orchestration/test_image_injection.py` (extend), `aaiclick/orchestration/test_orchestration_factories.py` (extend)

**Interfaces:**
- Consumes: Task 3's functions; `get_current_task_info()` from `execution/execution_worker_context.py` (raises `RuntimeError` outside task execution).
- Produces: `commit_tasks(items, job_id)` — same signature, now stamps inheritance from the current task, validates, and injects build tasks. `create_built_job` — same signature for now (still takes `runner: DockerRunner | KubernetesRunner` **with** image; Task 6 changes the signature), but additionally stamps `entry_task.image_source` and injects the build task + edge. This is the dual-write stage: `Job.runner` still carries the image so the old dispatch path keeps working until Task 5.

- [ ] **Step 1: Write the failing tests**

Append to `aaiclick/orchestration/test_image_injection.py`:

```python
async def test_commit_tasks_stamps_and_injects_for_docker_job(orch_ctx_no_ch, monkeypatch):
    """commit_tasks on a docker job: undeclared tasks inherit the committing
    task's image, and a build task + edges appear in the same commit."""
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    job = await create_job("j", "m.entry")
    async with get_sql_session() as session:
        row = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        row.runner_mode = RUNNER_DOCKER
        entry = (await session.execute(select(Task).where(Task.job_id == job.id))).scalar_one()
        entry.image_source = BUILD_A
        await session.commit()
        entry_id = entry.id

    set_current_task_info(task_id=entry_id, job_id=job.id)
    child = create_task("m.child")
    await commit_tasks(child, job.id)

    async with get_sql_session() as session:
        rows = (await session.execute(select(Task).where(Task.job_id == job.id))).scalars().all()
    by_entry = {t.entrypoint: t for t in rows}
    assert by_entry["m.child"].image_source == BUILD_A
    build = by_entry[IMAGE_BUILD_ENTRYPOINT]
    async with get_sql_session() as session:
        deps = (await session.execute(select(Dependency).where(Dependency.next_id == by_entry["m.child"].id))).scalars().all()
    assert build.id in {d.previous_id for d in deps}


async def test_commit_tasks_subprocess_job_rejects_image(orch_ctx_no_ch):
    job = await create_job("j", "m.entry")
    t = create_task("m.child")
    t.image_source = BUILD_A
    with pytest.raises(ValueError, match="subprocess"):
        await commit_tasks(t, job.id)
```

Add imports to the test file top: `from .execution.execution_worker_context import set_current_task_info`, `from .orch_context import commit_tasks` — and note `set_current_task_info` sets a ContextVar that leaks across tests in the same async context; reset by value: capture `token = image_injection`-independent — simplest is to call `set_current_task_info` only in tests that need it and rely on per-test event loop isolation (pytest-asyncio creates a fresh loop/context per test; verify with a quick run).

Append to `aaiclick/orchestration/test_orchestration_factories.py` (match existing fixture names in that file):

```python
async def test_create_built_job_stamps_entry_and_injects_build_task(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    runner = DockerRunner(image=ImageBuild(git_remote="https://example.com/r.git", git_sha="c" * 40))
    job = await create_built_job(name="j", entrypoint="m.entry", runner=runner)
    async with get_sql_session() as session:
        rows = (await session.execute(select(Task).where(Task.job_id == job.id))).scalars().all()
    by_entry = {t.entrypoint: t for t in rows}
    assert by_entry["m.entry"].image_source == dump_image_source(runner.image)
    assert IMAGE_BUILD_ENTRYPOINT in by_entry


async def test_create_built_job_prebuilt_injects_nothing(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    runner = DockerRunner(image=ImagePrebuilt(image_tag="ghcr.io/x/y:1"))
    job = await create_built_job(name="j", entrypoint="m.entry", runner=runner)
    async with get_sql_session() as session:
        rows = (await session.execute(select(Task).where(Task.job_id == job.id))).scalars().all()
    assert [t.entrypoint for t in rows] == ["m.entry"]
    assert rows[0].image_source == dump_image_source(runner.image)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest aaiclick/orchestration/test_image_injection.py aaiclick/orchestration/test_orchestration_factories.py -v`
Expected: new tests FAIL (no stamping/injection yet); existing tests PASS.

- [ ] **Step 3: Implement**

In `orch_context.py` — add imports (top of file): `from sqlmodel import select`, `from .execution.execution_worker_context import get_current_task_info`, `from .image_injection import inject_build_tasks, stamp_inherited_image, validate_image_sources`, and `Job` from `.models`. Then rework `commit_tasks`:

```python
async def _current_parent_image_source() -> dict | None:
    """The committing task's own image_source, or None outside task execution."""
    try:
        info = get_current_task_info()
    except RuntimeError:
        return None
    async with get_sql_session() as session:
        return (
            await session.execute(select(Task.image_source).where(Task.id == info.task_id))
        ).scalar_one_or_none()


async def commit_tasks(items: TasksType, job_id: int) -> TasksType:
    # (docstring: keep existing, add a paragraph on image stamping/injection)
    items_list = items if isinstance(items, list) else [items]
    all_items = _collect_from_registry(items_list)
    tasks_only = [item for item in all_items if isinstance(item, Task)]

    parent_image = await _current_parent_image_source()
    stamp_inherited_image(tasks_only, parent_image)

    async with get_sql_session() as session:
        job = (await session.execute(select(Job).where(Job.id == job_id))).scalar_one_or_none()
        injected: list[Task] = []
        if job is not None:
            validate_image_sources(tasks_only, job.runner_mode)
            for item in tasks_only:
                item.job_id = job_id
            injected = await inject_build_tasks(session, tasks_only, job)
        for item in [*injected, *all_items]:
            item.job_id = job_id
            if isinstance(item, Group) and item.id is None:
                item.id = get_snowflake_id()
            session.add(item)
        await session.commit()

    registry = get_task_registry()
    if registry is not None:
        for item in all_items:
            registry.pop(item.id, None)

    if isinstance(items, list):
        return items_list
    return items_list[0]
```

In `runner_env.py`: add `"AAICLICK_REGISTRY"` to `ALWAYS_PASSED_ENV_VARS` and extend the constant's docstring — dynamic `commit_tasks` runs *inside* containers, and `inject_build_tasks` checks the registry var at commit time; without forwarding it, a dynamic child declaring a new build image would silently skip injection and later fail pulling an unpushed tag.

Cycle check: `orch_context` already imports from `.execution.db_handler`; `image_injection` imports `docker_config` + `execution.image_build_task`, neither of which imports `orch_context`. If an import cycle appears at test time, follow CLAUDE.md's restructuring rules (move shared code; no inline imports).

In `factories.py` — `create_built_job`: after `entry_task.job_id = job.id`, stamp and inject inside the session block:

```python
    entry_task.image_source = dump_image_source(runner.image)

    async with get_sql_session() as session:
        injected = await inject_build_tasks(session, [entry_task], job)
        for obj in [job, *injected, entry_task]:
            session.add(obj)
        await session.commit()
```

(`inject_build_tasks` needs the `Job` row's `runner_mode`; the in-memory `job` from `new_job_row` already has it — pass it directly, no re-fetch.) Add imports: `from .image_injection import inject_build_tasks`, `from .runner_config import dump_image_source`.

- [ ] **Step 4: Run tests**

Run: `pytest aaiclick/orchestration/ -v -x`
Expected: PASS (full orchestration suite — the dual-write keeps old paths green)

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/orch_context.py aaiclick/orchestration/factories.py aaiclick/orchestration/test_image_injection.py aaiclick/orchestration/test_orchestration_factories.py
git commit -m "Stamp image inheritance and inject build tasks at commit points"
```

---

### Task 5: Dispatch switch — per-task vehicle, inline no-registry build

**Files:**
- Modify: `aaiclick/orchestration/execution/dispatch.py`
- Modify: `aaiclick/orchestration/execution/docker_worker.py` (drop `resolve_image_tag` import/use)
- Modify: `aaiclick/orchestration/execution/kubernetes_worker.py` (drop `resolve_image_tag` import/use)
- Delete: `aaiclick/orchestration/execution/image_builder.py`, `aaiclick/orchestration/execution/test_image_builder.py`
- Test: `aaiclick/orchestration/execution/test_dispatch.py` (rewrite affected tests)

**Interfaces:**
- Consumes: `Task.image_source`, `parse_image_source`, `compute_image_tag`, `build_image_to_tag`.
- Produces: in `dispatch.py` — `async resolve_launch_image(image_source: ImageSourceT, image_tag: str) -> str` (inline-builds for `ImageBuild` without registry, else returns the tag); `_resolve_dispatch` now returns a subprocess `JobDispatch` whenever `task.image_source is None`, regardless of job `runner_mode`.

- [ ] **Step 1: Read `test_dispatch.py` and rewrite its expectations**

Read the whole file first. Update tests that assert "every task inherits the job's runner_mode" to the new rule. Add:

```python
async def test_null_image_source_dispatches_subprocess_even_on_docker_job(orch_ctx_no_ch):
    """A NULL-image task in a docker job runs on the host — the rule that
    host-pins injected build tasks."""
    # create docker-mode job + task with image_source=None (see existing
    # helpers in this file for job/task setup), then:
    dispatch = await _resolve_dispatch(task)
    assert dispatch.runner_mode == RUNNER_SUBPROCESS


async def test_prebuilt_image_source_dispatches_with_tag_verbatim(orch_ctx_no_ch):
    # docker job; task.image_source = dump_image_source(ImagePrebuilt(image_tag="ghcr.io/x/y:1"))
    dispatch = await _resolve_dispatch(task)
    assert dispatch.runner_mode == RUNNER_DOCKER
    assert dispatch.image_tag == "ghcr.io/x/y:1"


async def test_build_image_source_computes_registry_tag(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    # docker job; task.image_source = BUILD_A (sha "a"*40)
    dispatch = await _resolve_dispatch(task)
    assert dispatch.image_tag == "registry.example:5000/aaiclick-job:" + "a" * 40


async def test_resolve_launch_image_builds_inline_without_registry(monkeypatch):
    calls = []

    async def fake_build(source, image_tag):
        calls.append(image_tag)

    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    monkeypatch.setattr(dispatch_module, "build_image_to_tag", fake_build)
    source = ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40)
    tag = await resolve_launch_image(source, "aaiclick-job:" + "a" * 40)
    assert calls == ["aaiclick-job:" + "a" * 40]
    assert tag == "aaiclick-job:" + "a" * 40


async def test_resolve_launch_image_skips_build_with_registry(monkeypatch):
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    # same fake_build; assert calls == [] — the dependency edge guaranteed the push
```

(Use the file's existing job/task creation helpers; the comments above mark where. `dispatch_module` = `from . import dispatch as dispatch_module`.)

- [ ] **Step 2: Run to verify new tests fail**

Run: `pytest aaiclick/orchestration/execution/test_dispatch.py -v`
Expected: new tests FAIL; note which old tests fail (they encode the old inheritance rule — they will be updated, not deleted, unless they test `resolve_image_tag` polling itself).

- [ ] **Step 3: Implement**

`dispatch.py` — replace `_resolve_dispatch` and add `resolve_launch_image`; drop the `image_builder` import:

```python
async def _resolve_dispatch(task: Task) -> JobDispatch:
    """Pick the runner for a task from its own image_source.

    NULL image_source ⇒ host subprocess, regardless of the job's
    runner_mode — the rule that host-pins injected build tasks (spec:
    docs/designs/task_image.md)."""
    if task.image_source is None:
        return JobDispatch(RUNNER_SUBPROCESS, None, None, task.entry_type, task.command, task.command_env, None)
    source = parse_image_source(task.image_source)
    async with get_sql_session() as session:
        job = (await session.execute(select(Job).where(Job.id == task.job_id))).scalar_one_or_none()
    if job is None:
        return JobDispatch(RUNNER_SUBPROCESS, None, None, task.entry_type, task.command, task.command_env, None)
    runner = parse_runner_config(job.runner) if job.runner else None
    image_tag = source.image_tag if isinstance(source, ImagePrebuilt) else compute_image_tag(source.git_sha)
    return JobDispatch(
        job.runner_mode, image_tag, _kube_dict(runner), task.entry_type, task.command, task.command_env, source
    )


async def resolve_launch_image(image_source: ImageSourceT, image_tag: str) -> str:
    """Resolve the image a container actually launches with.

    Registry mode: the dependency edge guarantees the tag is pushed — return
    it (the launch path pulls). No registry + build source: build inline on
    this host (``build_image_to_tag`` short-circuits on a local-cache hit),
    holding the slot for a cold build (accepted, spec "No registry")."""
    if isinstance(image_source, ImageBuild) and not os.environ.get("AAICLICK_REGISTRY"):
        await build_image_to_tag(image_source, image_tag)
    return image_tag
```

Imports for `dispatch.py`: add `import os`, `from ..docker_config import compute_image_tag`, `from ..runner_config import ImageBuild, ImagePrebuilt, ImageSourceT, parse_image_source, ...`, `from .docker_build import build_image_to_tag`; remove `from .image_builder import resolve_image_tag` and `from ..docker_config import effective_image_tag`.

`build_shell_spec` in the same file: replace both `resolve_image_tag(...)` calls with `resolve_launch_image(dispatch.image_source, dispatch.image_tag)` (add an assert/raise if `dispatch.image_source is None` — shell tasks reaching the docker branch always have one now). The `execution_worker_id` parameter of `build_shell_spec` becomes unused — remove it and update its callers (grep `build_shell_spec(`).

`docker_worker.py` `_run_task_in_container` and `kubernetes_worker.py` `_run_task_in_pod` (line ~317): replace `await resolve_image_tag(task, dispatch.image_source, dispatch.image_tag, execution_worker_id)` with `await resolve_launch_image(dispatch.image_source, dispatch.image_tag)` — import from `.dispatch`. Watch for an import cycle (`dispatch` imports `docker_worker`): if one appears, move `resolve_launch_image` into `docker_build.py` instead (it only depends on `runner_config` + `docker_build` internals — this is the cycle-free home; prefer it if in doubt).

Delete `image_builder.py` and `test_image_builder.py`. Grep for remaining imports: `grep -rn "image_builder" aaiclick/` must return nothing.

- [ ] **Step 4: Run tests**

Run: `pytest aaiclick/orchestration/ -v`
Expected: PASS. Failures in tests that submitted docker jobs and relied on dispatch-time builds must be fixed by updating the test to the new flow (build task in graph), not by re-adding the old path.

- [ ] **Step 5: Commit**

```bash
git add -A aaiclick/orchestration/execution/
git commit -m "Dispatch per-task image_source; inline build without registry; drop ensure_image machinery"
```

---

### Task 6: Retire job-level image — models, factories, registered jobs

**Files:**
- Modify: `aaiclick/orchestration/runner_config.py` (drop `image` from `DockerRunner`/`KubernetesRunner`)
- Modify: `aaiclick/orchestration/models.py` (drop `BuildTask`, `BUILD_*` statuses, `Task.build_task_id`; `RegisteredJob`: add `image: str | None`, drop `runner`)
- Modify: `aaiclick/orchestration/docker_config.py` (`resolve_runner_config` → image + cluster split; `_registered_image` reads `registered.image`; delete `effective_image_tag`)
- Modify: `aaiclick/orchestration/factories.py` (`create_built_job` final signature)
- Modify: `aaiclick/orchestration/registered_jobs.py` (`register_job`/`upsert_registered_job` store `image` column; `run_job` threads `image_source`)
- Delete: `aaiclick/orchestration/test_build_task_model.py`
- Test: `aaiclick/orchestration/test_docker_config.py`, `test_orchestration_factories.py`, `test_kubernetes_submission.py`, `test_models_kubernetes.py` (update)

**Interfaces:**
- Produces (final shapes):
  - `class DockerRunner(BaseModel): type: Literal["docker"] = "docker"` (no image)
  - `class KubernetesRunner(BaseModel): type: Literal["kubernetes"] = "kubernetes"; namespace/service_account/image_pull_secret/resources` (no image)
  - `docker_config.resolve_image_source(registered, *, image, git_remote, git_sha, git_branch, dockerfile) -> ImageSourceT` (rename of `_resolve_image_source`, now public; unchanged body except `_registered_image` reads `registered.image`)
  - `resolve_runner_config(registered, *, runner_mode, kubernetes_config) -> RunnerConfigT` (cluster config only, no image params)
  - `create_built_job(*, name, entrypoint, runner: DockerRunner | KubernetesRunner, image_source: ImageSourceT, entry_type=..., command=..., command_env=..., kwargs=..., run_type=..., registered_job_id=..., preservation_mode=..., registered=...) -> Job` — stamps `entry_task.image_source = dump_image_source(image_source)`
  - `RegisteredJob.image: str | None` column (prebuilt default; replaces the `runner` JSON marker)
  - `create_task(..., image: str | None = None, git_remote: str | None = None, git_sha: str | None = None, git_branch: str | None = None, dockerfile: str | None = None)` — declares the task's own image (sets `task.image_source`); `image` and `git_*` mutually exclusive; a build declaration requires both `git_remote` and `git_sha` explicitly (`create_task` is sync — no git auto-detect; auto-detect exists only in the async `run_job` submission path, and inside a container there is no working tree to detect from anyway)

- [ ] **Step 0: create_task image-sugar test + implementation**

Append to `test_orchestration_factories.py`:

```python
def test_create_task_image_kwarg_sets_prebuilt_source():
    t = create_task("m.f", image="ghcr.io/x/y:1")
    assert t.image_source == {"type": "prebuilt", "image_tag": "ghcr.io/x/y:1"}


def test_create_task_git_kwargs_set_build_source():
    t = create_task("m.f", git_remote="https://example.com/r.git", git_sha="a" * 40, dockerfile="Dockerfile.gpu")
    assert t.image_source["type"] == "build"
    assert t.image_source["git_sha"] == "a" * 40


def test_create_task_image_and_git_mutually_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        create_task("m.f", image="ghcr.io/x/y:1", git_sha="a" * 40)


def test_create_task_build_requires_remote_and_sha():
    with pytest.raises(ValueError, match="git_remote and git_sha"):
        create_task("m.f", git_sha="a" * 40)
```

Run (expect FAIL), then implement in `factories.py` — add the five keyword params to `create_task` and, before constructing the `Task`:

```python
    image_source: dict | None = None
    git_fields = (git_remote, git_sha, git_branch, dockerfile)
    if image is not None and any(v is not None for v in git_fields):
        raise ValueError("image (prebuilt) and git_* (build) are mutually exclusive")
    if image is not None:
        image_source = dump_image_source(ImagePrebuilt(image_tag=image))
    elif any(v is not None for v in git_fields):
        if git_remote is None or git_sha is None:
            raise ValueError(
                "a per-task build image requires both git_remote and git_sha "
                "(create_task has no git auto-detect; that only exists at run_job submission)"
            )
        image_source = dump_image_source(
            ImageBuild(git_remote=git_remote, git_sha=git_sha, git_branch=git_branch, dockerfile=dockerfile)
        )
```

then pass `image_source=image_source` into the `Task(...)` constructor. Imports: `ImageBuild`, `ImagePrebuilt`, `dump_image_source` from `.runner_config`. Document the params in the docstring with literal examples per the docstring convention. Run the tests (expect PASS).

- [ ] **Step 1: Update tests first**

- `test_docker_config.py`: change `resolve_runner_config` calls to the split API — image expectations move to `resolve_image_source` return values; runner expectations drop `.image`.
- `test_orchestration_factories.py`: `create_built_job(..., runner=DockerRunner(), image_source=ImageBuild(...))`.
- `test_kubernetes_submission.py` / `test_models_kubernetes.py`: same split; `KubernetesRunner` keeps cluster fields only.
- Delete `test_build_task_model.py`.
- New test in `test_docker_config.py`:

```python
async def test_registered_prebuilt_image_default(monkeypatch):
    registered = RegisteredJob(id=1, name="j", entrypoint="m.e", image="ghcr.io/x/y:1")
    source = await resolve_image_source(registered, image=None, git_remote=None, git_sha=None, git_branch=None, dockerfile=None)
    assert isinstance(source, ImagePrebuilt)
    assert source.image_tag == "ghcr.io/x/y:1"
```

- [ ] **Step 2: Run to verify failures**

Run: `pytest aaiclick/orchestration/test_docker_config.py aaiclick/orchestration/test_orchestration_factories.py -v`
Expected: FAIL (old signatures)

- [ ] **Step 3: Implement**

- `runner_config.py`: remove `image: ImageSource` fields from `DockerRunner`/`KubernetesRunner`.
- `models.py`: delete `class BuildTask`, the `BUILD_PENDING/BUILD_BUILDING/BUILD_READY/BUILD_FAILED` constants and `BuildStatus` Literal, and `Task.build_task_id`. `RegisteredJob`: delete `runner` field, add `image: str | None = Field(default=None)`.
- `docker_config.py`: rename `_resolve_image_source` → `resolve_image_source` (public); `_registered_image(registered)` becomes `registered.image if registered is not None else None` (inline it); `resolve_runner_config` loses `image/git_*/dockerfile` params and the source resolution — returns `KubernetesRunner(namespace=..., ...)` or `DockerRunner()`. Delete `effective_image_tag` (grep `effective_image_tag` — dispatch no longer uses it after Task 5; fix any survivor).
- `factories.py`: `create_built_job` gains `image_source: ImageSourceT` keyword; stamps `entry_task.image_source = dump_image_source(image_source)`; `runner` param is now cluster-only config for `Job.runner`.
- `registered_jobs.py`: `register_job`/`upsert_registered_job` — replace the `runner = dump_runner_config(DockerRunner(image=...))` lines with storing `image=image` on the row (drop the `runner=` kwarg threading and `_build_registered_job`'s `runner` param). `run_job`: call `resolve_image_source(...)` with the git/image kwargs, then `resolve_runner_config(registered, runner_mode=runner_mode, kubernetes_config=kube_cfg)`, then `create_built_job(..., runner=runner, image_source=source, ...)`.
- Grep sweeps (each must come back clean or be fixed): `grep -rn "build_task_id\|BuildTask\|BUILD_READY\|BUILD_BUILDING\|BUILD_FAILED\|BUILD_PENDING" aaiclick/ --include="*.py" | grep -v migrations`, `grep -rn "runner\[.image.\]\|\.runner\b" aaiclick/orchestration/registered_jobs.py`, `grep -rn "effective_image_tag" aaiclick/`.
- Check `aaiclick/view_models.py` `RegisterJobRequest`/`RunJobRequest` — field names are unchanged (`image`, `git_*`), so API surfaces need no change; run `pytest aaiclick/test_view_models.py aaiclick/internal_api/ -v` to confirm.

- [ ] **Step 4: Run the full suite**

Run: `pytest aaiclick/ -x -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A aaiclick/
git commit -m "Retire job-level image: per-task image_source everywhere, drop build_tasks model"
```

---

### Task 7: `is_image_build` flag on TaskView

**Files:**
- Modify: `aaiclick/orchestration/view_models.py` (`TaskView`, `task_to_view`)
- Test: `aaiclick/orchestration/test_view_models.py` if it exists, else `aaiclick/test_view_models.py` (check both; add where the other `TaskView` tests live)

**Interfaces:**
- Produces: `TaskView.is_image_build: bool = False`, populated via `is_image_build_task(task.entrypoint)`.

- [ ] **Step 1: Write the failing test** (place next to existing `task_to_view` tests)

```python
def test_task_to_view_flags_image_build_tasks():
    build = create_task(None, name="build-image:abc")
    build.entrypoint = IMAGE_BUILD_ENTRYPOINT
    assert task_to_view(build).is_image_build
    assert not task_to_view(create_task("m.f")).is_image_build
```

(Imports: `from aaiclick.orchestration.execution.image_build_task import IMAGE_BUILD_ENTRYPOINT` — adjust to the test file's import style.)

- [ ] **Step 2: Run to verify it fails** — `pytest <chosen test file> -v`; expected: FAIL (`is_image_build` missing).

- [ ] **Step 3: Implement** — add `is_image_build: bool = False` to `TaskView`; in `task_to_view` add `is_image_build=is_image_build_task(task.entrypoint)`; import `is_image_build_task` at top. This flows through `JobDetail.tasks` to the SPA, which can style build nodes and their outgoing edges later (no frontend work in this plan).

- [ ] **Step 4: Run tests** — `pytest aaiclick/ -q`; expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/view_models.py aaiclick/test_view_models.py aaiclick/orchestration/
git commit -m "Expose is_image_build on TaskView for build-node UI styling"
```

---

### Task 8: Alembic migration (generated — never hand-written)

**Files:**
- Create (generated): `aaiclick/orchestration/migrations/versions/<rev>_per_task_image_source.py`

Schema delta the generator must pick up from the models (verify the generated file contains all of it):
- `tasks`: add `image_source` JSON nullable; drop `build_task_id` (+ its FK/index)
- `registered_jobs`: add `image` string nullable; drop `runner`
- drop table `build_tasks`

- [ ] **Step 1: Push the branch** (the skill runs in GitHub Actions against the pushed models): `git push -u origin claude/task-based-image-build-qvr5x4`
- [ ] **Step 2: Invoke the `generate-migration` skill** and follow it exactly (it triggers the workflow and lands the migration file).
- [ ] **Step 3: Review the generated migration** against the delta list above; if a piece is missing (e.g. JSON column type quirk), re-run per the skill's instructions — do NOT hand-edit beyond what the skill permits.
- [ ] **Step 4: Run the suite** — `pytest aaiclick/ -q` (tests use `create_all`, so this validates models, not the migration; the migration is exercised by the distributed CI workflow).
- [ ] **Step 5: Commit/push** whatever the skill's flow leaves for you (often the workflow commits directly — follow the skill).

---

### Task 9: Docs — user guide, design docs, future.md, spec retirement

**Files:**
- Modify: `docs/user_guide/orchestration.md` (image/runner sections → per-task model), `docs/user_guide/container_images.md` (the "worker also needs Docker" warning: build now runs as a host-pinned build task in registry mode / inline for docker-no-registry)
- Modify: `docs/designs/orchestration.md`, `docs/designs/kubernetes_runner.md` (replace `ensure_image`/`build_tasks` references with the build-task + edges model; reference implementation by name: `aaiclick/orchestration/image_injection.py` — see `inject_build_tasks`)
- Modify: `docs/designs/future.md` — delete the "Non-Blocking Image-Build Wait (Release-and-Requeue)" section (registry mode: dependency gating replaces polling entirely; no-registry inline build is an accepted cost per spec, not planned work)
- Delete: `docs/designs/task_image.md` and this plan file (per CLAUDE.md: spec + plan are removed once the feature lands; the user guide + implementation are the record). Grep `task_image` for dangling references.

- [ ] **Step 1: Update the four docs** (use `markdown-style` + `shortify` skills; implementation references by symbol name, never line numbers).
- [ ] **Step 2: Grep sweep** — `grep -rn "build_tasks\|ensure_image\|build_task_id\|task_image" docs/ | grep -v superpowers` must come back clean.
- [ ] **Step 3: Commit and push**

```bash
git add -A docs/
git commit -m "Docs: per-task image model; retire build_tasks references and spec"
git push -u origin claude/task-based-image-build-qvr5x4
```

- [ ] **Step 4: Check CI** — use the `check-pr` skill (per CLAUDE.md) and fix any failures.
