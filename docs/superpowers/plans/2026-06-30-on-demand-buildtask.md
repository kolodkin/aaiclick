# On-Demand BuildTask Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace submission-time `docker_build` task injection with a first-class `BuildTask` entity that builds the image on-demand at dispatch, exactly once per image identity, shared across jobs and dynamic tasks.

**Architecture:** A new `build_tasks` table keyed by a content hash of `(git_remote, git_sha, dockerfile)` is the dedup primitive. A new `ensure_image()` seam runs in the docker/kubernetes workers before launch; it claims the build via `INSERT … ON CONFLICT DO NOTHING` (one winner builds, others poll), reusing the existing clone→build→push body. `create_built_job()` stops injecting the build task, and the old build-task code path is deleted.

**Tech Stack:** Python 3.12+, SQLModel/SQLAlchemy (async), Alembic, pytest (async), Pydantic discriminated unions.

## Global Constraints

- **All imports at the top of the file.** Three groups (stdlib / external / current package), blank-line separated. Never import inside functions, methods, or tests.
- **Prefer `Literal` over Enum** for closed string sets: define a `Literal` alias + module-level string constants; store DB-mapped fields as plain `Column(String, ...)`; no DB CHECK constraints.
- **Prefer `NamedTuple` over plain tuples** in APIs; named-attribute access internally.
- **No `__all__`** in `__init__.py`. **No history comments** (`# Removed: …`).
- **Never hand-write migrations** — use the `generate-migration` skill (GitHub Actions).
- **No `Any` as a typing shortcut.** Break cycles via neutral modules or `from __future__ import annotations`, not `TYPE_CHECKING`.
- **Tests:** flat layout next to code (`aaiclick/orchestration/.../test_*.py`); async tests need no decorator (the suite auto-runs coroutine tests); DB-touching tests use the `orch_ctx` / `orch_ctx_no_ch` fixtures and query through `async with get_sql_session() as session`.
- Snowflake IDs via `from aaiclick.snowflake import get_snowflake_id`. Timestamps via `from aaiclick.datetime_utils import utc_now`.

---

## File Structure

- **Create** `aaiclick/orchestration/execution/image_builder.py` — `ensure_image()`, the claim/lease/poll loop, `BuildFailed`, lease/poll constants, `EnsuredImage`.
- **Create** `aaiclick/orchestration/execution/test_image_builder.py` — claim/dedup/poll/retry tests.
- **Modify** `aaiclick/orchestration/models.py` — add `BuildStatus` literals + `BuildTask` model + `Task.build_task_id` column.
- **Modify** `aaiclick/orchestration/docker_config.py` — add `image_key()`.
- **Modify** `aaiclick/orchestration/docker_config.py` test → `aaiclick/orchestration/test_docker_config.py` — `image_key()` tests.
- **Modify** `aaiclick/orchestration/execution/docker_build.py` — replace the `@task build_image(job_id)` with a pure `build_image_to_tag(source, image_tag)`; keep the low-level helpers.
- **Modify** `aaiclick/orchestration/execution/test_docker_build.py` — point existing build-body tests at `build_image_to_tag`.
- **Modify** `aaiclick/orchestration/execution/worker.py` — add `image_source` to `JobDispatch`.
- **Modify** `aaiclick/orchestration/execution/dispatch.py` — populate `image_source`; later delete the `BUILD_TASK_ENTRYPOINT` special-case.
- **Modify** `aaiclick/orchestration/execution/docker_worker.py` + `kubernetes_worker.py` — call `ensure_image()` for build sources before launch; stamp `build_task_id`.
- **Modify** `aaiclick/orchestration/factories.py` — remove the build-task injection block.
- **Modify** `aaiclick/orchestration/test_orchestration_factories.py` + `execution/test_dispatch.py` — update expectations.
- **Migration:** new `build_tasks` table + `tasks.build_task_id` column via the `generate-migration` skill.

---

## Task 1: `BuildTask` model, `BuildStatus` literals, and `Task.build_task_id`

**Files:**
- Modify: `aaiclick/orchestration/models.py`
- Test: `aaiclick/orchestration/test_build_task_model.py` (create)

**Interfaces:**
- Produces: `BuildStatus` alias + constants `BUILD_PENDING`, `BUILD_BUILDING`, `BUILD_READY`, `BUILD_FAILED`; `class BuildTask(SQLModel, table=True)` with fields `id, image_key (UNIQUE), image_tag, git_remote, git_sha, dockerfile, status, holder_worker_id, lease_expires_at, log_path, error, attempts, max_retries, created_at, started_at, finished_at`; `Task.build_task_id: int | None`.

- [ ] **Step 1: Write the failing test**

Create `aaiclick/orchestration/test_build_task_model.py`:

```python
"""Tests for the BuildTask model and its uniqueness constraint."""

from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from aaiclick.orchestration.models import (
    BUILD_BUILDING,
    BUILD_READY,
    BuildTask,
)
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.snowflake import get_snowflake_id


def _build_task(**overrides) -> BuildTask:
    base = dict(
        id=get_snowflake_id(),
        image_key="k" * 64,
        image_tag="aaiclick-job:" + "a" * 40,
        git_remote="git@x:r.git",
        git_sha="a" * 40,
        dockerfile=None,
        status=BUILD_BUILDING,
        max_retries=2,
        attempts=1,
    )
    base.update(overrides)
    return BuildTask(**base)


async def test_build_task_round_trips(orch_ctx_no_ch):
    bt = _build_task()
    async with get_sql_session() as session:
        session.add(bt)
        await session.commit()
    async with get_sql_session() as session:
        row = (await session.execute(select(BuildTask).where(BuildTask.id == bt.id))).scalar_one()
    assert row.image_key == "k" * 64
    assert row.status == BUILD_BUILDING


async def test_image_key_is_unique(orch_ctx_no_ch):
    first = _build_task(status=BUILD_READY)
    async with get_sql_session() as session:
        session.add(first)
        await session.commit()
    duplicate = _build_task(id=get_snowflake_id())  # same image_key, different id
    with pytest.raises(IntegrityError):
        async with get_sql_session() as session:
            session.add(duplicate)
            await session.commit()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/test_build_task_model.py -v`
Expected: FAIL with `ImportError: cannot import name 'BuildTask'`.

- [ ] **Step 3: Add the model and constants**

In `aaiclick/orchestration/models.py`, after the `WorkerStatus` block (~line 75), add:

```python
BUILD_PENDING = "PENDING"
BUILD_BUILDING = "BUILDING"
BUILD_READY = "READY"
BUILD_FAILED = "FAILED"
BuildStatus = Literal["PENDING", "BUILDING", "READY", "FAILED"]
"""Lifecycle of an on-demand image build.

A ``BuildTask`` row is created at claim time (``BUILDING``), before the build
starts — so concurrent tasks see the in-flight build and attach instead of
starting a second one. ``READY`` once the image exists; ``FAILED`` after a
build error (retried while ``attempts <= max_retries``)."""
```

Then add the model after `RemoteTaskResult` (~line 484):

```python
class BuildTask(SQLModel, table=True):
    """On-demand image build, keyed by image identity (not by job).

    ``image_key`` is a sha256 of ``(git_remote, git_sha, dockerfile)``; its
    UNIQUE constraint makes a duplicate build record structurally impossible, so
    every job/dynamic task on the same image shares this one row. ``ImagePrebuilt``
    images never create a ``BuildTask``."""

    __tablename__: ClassVar[str] = "build_tasks"
    __table_args__ = (UniqueConstraint("image_key"),)

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    image_key: str = Field(sa_column=Column(String, nullable=False, index=True))
    image_tag: str = Field()
    git_remote: str = Field()
    git_sha: str = Field()
    dockerfile: str | None = Field(default=None)
    status: BuildStatus = Field(
        default=BUILD_PENDING,
        sa_column=Column(String, nullable=False, index=True),
    )
    holder_worker_id: int | None = Field(
        default=None, sa_column=Column(BigInteger, nullable=True)
    )
    lease_expires_at: datetime | None = Field(default=None)
    log_path: str | None = Field(default=None)
    error: str | None = Field(default=None)
    attempts: int = Field(default=0)
    max_retries: int = Field(default=2)
    created_at: datetime = Field(default_factory=utc_now)
    started_at: datetime | None = Field(default=None)
    finished_at: datetime | None = Field(default=None)
```

Then add the link column to `Task` (after `worker_id`, ~line 370):

```python
    build_task_id: int | None = Field(
        default=None, sa_column=Column(BigInteger, ForeignKey("build_tasks.id"), index=True, nullable=True)
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest aaiclick/orchestration/test_build_task_model.py -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/models.py aaiclick/orchestration/test_build_task_model.py
git commit -m "feat: add BuildTask model and Task.build_task_id link"
```

---

## Task 2: `image_key()` derivation

**Files:**
- Modify: `aaiclick/orchestration/docker_config.py`
- Test: `aaiclick/orchestration/test_docker_config.py`

**Interfaces:**
- Consumes: `ImageBuild` from `runner_config`.
- Produces: `def image_key(source: ImageBuild) -> str` — stable 64-char sha256 hex over `(git_remote, git_sha, dockerfile)`.

- [ ] **Step 1: Write the failing test**

Append to `aaiclick/orchestration/test_docker_config.py`:

```python
def test_image_key_stable_and_distinguishes_fields():
    from aaiclick.orchestration.docker_config import image_key
    from aaiclick.orchestration.runner_config import ImageBuild

    a = ImageBuild(git_remote="git@x:r.git", git_sha="a" * 40, dockerfile=None)
    a_again = ImageBuild(git_remote="git@x:r.git", git_sha="a" * 40, git_branch="ignored", dockerfile=None)
    b = ImageBuild(git_remote="git@x:r.git", git_sha="b" * 40, dockerfile=None)
    c = ImageBuild(git_remote="git@x:r.git", git_sha="a" * 40, dockerfile="Dockerfile.gpu")

    assert len(image_key(a)) == 64
    assert image_key(a) == image_key(a_again)          # git_branch is not part of identity
    assert image_key(a) != image_key(b)                # sha matters
    assert image_key(a) != image_key(c)                # dockerfile matters
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/test_docker_config.py::test_image_key_stable_and_distinguishes_fields -v`
Expected: FAIL with `ImportError: cannot import name 'image_key'`.

- [ ] **Step 3: Implement `image_key`**

In `aaiclick/orchestration/docker_config.py`, add `import hashlib` to the stdlib import group, and add after `compute_image_tag` (~line 85):

```python
def image_key(source: ImageBuild) -> str:
    """Stable sha256 identity of a build image over ``(git_remote, git_sha,
    dockerfile)``. ``git_branch`` is deliberately excluded — it does not change
    the built image, only where the SHA was found. This is the dedup key for
    ``build_tasks``."""
    parts = "\x00".join([source.git_remote, source.git_sha, source.dockerfile or ""])
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest aaiclick/orchestration/test_docker_config.py::test_image_key_stable_and_distinguishes_fields -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/docker_config.py aaiclick/orchestration/test_docker_config.py
git commit -m "feat: add image_key() identity hash for build dedup"
```

---

## Task 3: Extract a pure `build_image_to_tag()` build routine

The existing `build_image(job_id)` is a `@task` that fetches a `Job`, derives the source, computes the tag, then runs the pull→inspect→clone→build→push body. Split the reusable body out so `ensure_image()` can call it without a `Job`. Keep the `@task build_image` wrapper for now (deleted in Task 6) so nothing breaks mid-plan.

**Files:**
- Modify: `aaiclick/orchestration/execution/docker_build.py`
- Test: `aaiclick/orchestration/execution/test_docker_build.py`

**Interfaces:**
- Consumes: `ImageBuild`; `compute_image_tag`.
- Produces: `async def build_image_to_tag(source: ImageBuild, image_tag: str) -> None` — the registry-pull → local-inspect → clone → build → push body (idempotent; pushes after a local-cache hit when a registry is set).

- [ ] **Step 1: Write the failing test**

Replace `test_build_image_pushes_after_local_cache_hit_when_registry_set` and `test_build_image_missing_dockerfile_raises` in `test_docker_build.py` to drive `build_image_to_tag` directly (no job fetch):

```python
async def test_build_image_to_tag_pushes_after_local_cache_hit_when_registry_set(monkeypatch):
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40)
    expected_tag = docker_config.compute_image_tag("a" * 40)

    pull = AsyncMock(return_value=False)
    inspect = AsyncMock(return_value=True)
    clone = AsyncMock()
    build = AsyncMock()
    push = AsyncMock()
    monkeypatch.setattr(docker_build, "_docker_pull", pull)
    monkeypatch.setattr(docker_build, "_docker_image_exists_locally", inspect)
    monkeypatch.setattr(docker_build, "_git_clone_at_sha", clone)
    monkeypatch.setattr(docker_build, "_docker_build", build)
    monkeypatch.setattr(docker_build, "_docker_push", push)

    await docker_build.build_image_to_tag(source, expected_tag)

    pull.assert_awaited_once_with(expected_tag)
    inspect.assert_awaited_once_with(expected_tag)
    clone.assert_not_called()
    build.assert_not_called()
    push.assert_awaited_once_with(expected_tag)


async def test_build_image_to_tag_missing_dockerfile_raises(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40, dockerfile="Dockerfile.missing")

    monkeypatch.setattr(docker_build, "_docker_pull", AsyncMock(return_value=False))
    monkeypatch.setattr(docker_build, "_docker_image_exists_locally", AsyncMock(return_value=False))

    async def fake_clone(remote, sha, workdir):
        return None

    monkeypatch.setattr(docker_build, "_git_clone_at_sha", fake_clone)

    with pytest.raises(FileNotFoundError, match="Dockerfile not found"):
        await docker_build.build_image_to_tag(source, docker_config.compute_image_tag("a" * 40))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/execution/test_docker_build.py -v`
Expected: FAIL with `AttributeError: module 'aaiclick.orchestration.execution.docker_build' has no attribute 'build_image_to_tag'`.

- [ ] **Step 3: Implement `build_image_to_tag` and delegate the task to it**

In `docker_build.py`, add the pure routine (after `_docker_build`, ~line 125):

```python
async def build_image_to_tag(source: ImageBuild, image_tag: str) -> None:
    """Ensure ``image_tag`` exists in the local docker daemon, building from
    ``source`` if needed; push when a registry is configured.

    Idempotent and content-addressed by SHA. A local-cache hit short-circuits
    the *build* but not the push: a prior attempt that built locally then failed
    to push must re-push on retry, or other hosts could never pull the image."""
    registry = os.environ.get("AAICLICK_REGISTRY")

    if registry and await _docker_pull(image_tag):
        return

    if not await _docker_image_exists_locally(image_tag):
        with tempfile.TemporaryDirectory(prefix="aaiclick-build-") as workdir:
            await _git_clone_at_sha(source.git_remote, source.git_sha, workdir)

            context_dir = Path(workdir)
            dockerfile = context_dir / (source.dockerfile or "Dockerfile")
            if not dockerfile.is_file():
                raise FileNotFoundError(
                    f"Dockerfile not found at "
                    f"{source.dockerfile or 'Dockerfile'} "
                    f"in repo {source.git_remote}@{source.git_sha}. "
                    f"Run `python -m aaiclick docker init` in the user's repo "
                    f"to scaffold a starter Dockerfile."
                )

            build_args = _collect_build_args(source)
            await _docker_build(str(context_dir), str(dockerfile), image_tag, build_args)

    if registry:
        await _docker_push(image_tag)
```

Then collapse the existing `build_image` task body to delegate:

```python
@task(name="docker_build", max_retries=2)
async def build_image(job_id: int) -> None:
    """Deprecated host-side build task (still injected at submission until the
    on-demand build seam lands). Delegates to ``build_image_to_tag``."""
    job = await _fetch_job(job_id)
    source = _build_source(job)
    image_tag = effective_image_tag(parse_runner_config(job.runner))
    await build_image_to_tag(source, image_tag)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest aaiclick/orchestration/execution/test_docker_build.py -v`
Expected: PASS (all tests, including the unchanged `_collect_build_args` / `_build_source` ones).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/execution/docker_build.py aaiclick/orchestration/execution/test_docker_build.py
git commit -m "refactor: extract build_image_to_tag from the build task body"
```

---

## Task 4: `ensure_image()` — claim, lease, poll, build

**Files:**
- Create: `aaiclick/orchestration/execution/image_builder.py`
- Test: `aaiclick/orchestration/execution/test_image_builder.py`

**Interfaces:**
- Consumes: `ImageBuild`; `image_key`, `compute_image_tag`; `build_image_to_tag`; `BuildTask` + status constants; `get_sql_session`.
- Produces:
  - `class EnsuredImage(NamedTuple)` with `image_tag: str`, `build_task_id: int`.
  - `class BuildFailed(RuntimeError)`.
  - `async def ensure_image(source: ImageBuild, worker_id: int) -> EnsuredImage`.
  - Module constants `LEASE_SECONDS = 600`, `BUILD_POLL_INTERVAL = 2.0`.

**Claim/lease design.** `attempts` starts at 1 on the first claim and increments on each reclaim. A `FAILED` row is terminal once `attempts > max_retries` (default 2 → up to 3 attempts, matching today's `max_retries=2` task). The claim is a single atomic statement so the database, not timing, picks one winner:
- **New build:** `INSERT … ON CONFLICT(image_key) DO NOTHING RETURNING id` — a row back means you won.
- **Reclaim:** if the insert conflicted, an `UPDATE … WHERE image_key=:key AND (lease_expires_at < :now OR (status='FAILED' AND attempts <= max_retries)) RETURNING id` — at most one worker's update matches (row lock serializes them).

- [ ] **Step 1: Write the failing tests**

Create `aaiclick/orchestration/execution/test_image_builder.py`:

```python
"""Tests for the on-demand image build seam (ensure_image)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from .. import docker_config
from ..models import BUILD_FAILED, BUILD_READY, BuildTask
from ..orch_context import get_sql_session
from ..runner_config import ImageBuild
from . import image_builder
from .image_builder import BuildFailed, ensure_image


def _source(sha="a" * 40) -> ImageBuild:
    return ImageBuild(git_remote="git@x:r.git", git_sha=sha)


async def test_ensure_image_builds_once_and_marks_ready(orch_ctx_no_ch, monkeypatch):
    build = AsyncMock()
    monkeypatch.setattr(image_builder, "build_image_to_tag", build)
    source = _source()

    ensured = await ensure_image(source, worker_id=1)

    build.assert_awaited_once()
    assert ensured.image_tag == docker_config.compute_image_tag("a" * 40)
    async with get_sql_session() as session:
        row = (await session.execute(select(BuildTask).where(BuildTask.id == ensured.build_task_id))).scalar_one()
    assert row.status == BUILD_READY


async def test_ensure_image_reuses_ready_row_without_building(orch_ctx_no_ch, monkeypatch):
    build = AsyncMock()
    monkeypatch.setattr(image_builder, "build_image_to_tag", build)
    source = _source()

    first = await ensure_image(source, worker_id=1)
    second = await ensure_image(source, worker_id=2)

    build.assert_awaited_once()            # only the first call built
    assert second.build_task_id == first.build_task_id


async def test_ensure_image_raises_after_exhausting_retries(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setattr(image_builder, "BUILD_POLL_INTERVAL", 0.0)
    monkeypatch.setattr(image_builder, "build_image_to_tag", AsyncMock(side_effect=RuntimeError("boom")))
    source = _source()

    with pytest.raises(BuildFailed):
        await ensure_image(source, worker_id=1)

    async with get_sql_session() as session:
        row = (await session.execute(select(BuildTask).where(BuildTask.git_sha == "a" * 40))).scalar_one()
    assert row.status == BUILD_FAILED
    assert row.attempts == row.max_retries + 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest aaiclick/orchestration/execution/test_image_builder.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'aaiclick.orchestration.execution.image_builder'`.

- [ ] **Step 3: Implement `image_builder.py`**

```python
"""On-demand image build seam.

``ensure_image`` is called by the docker/kubernetes workers before launching a
container for a build-source job. It guarantees the image exists, building it
exactly once across all concurrent workers via the ``build_tasks`` row:

- ``UNIQUE(image_key)`` ⇒ one build record per image identity.
- An atomic claim (``INSERT … ON CONFLICT DO NOTHING`` / conditional reclaim
  ``UPDATE``) ⇒ one live builder; everyone else polls until ``READY``/``FAILED``.

The row is created at claim time (``BUILDING``), before the build starts, so an
in-flight build counts as "already in place" and concurrent tasks attach to it
instead of starting a second build.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import NamedTuple

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from aaiclick.datetime_utils import utc_now
from aaiclick.snowflake import get_snowflake_id

from ..docker_config import compute_image_tag, image_key
from ..models import (
    BUILD_BUILDING,
    BUILD_FAILED,
    BUILD_READY,
    BuildTask,
)
from ..orch_context import get_sql_session
from ..runner_config import ImageBuild
from .docker_build import build_image_to_tag

LEASE_SECONDS = 600
BUILD_POLL_INTERVAL = 2.0


class EnsuredImage(NamedTuple):
    image_tag: str
    build_task_id: int


class BuildFailed(RuntimeError):
    """An image build failed and exhausted its retries."""


def _lease_until():
    return utc_now() + timedelta(seconds=LEASE_SECONDS)


async def _get_row(key: str) -> BuildTask | None:
    async with get_sql_session() as session:
        return (await session.execute(select(BuildTask).where(BuildTask.image_key == key))).scalar_one_or_none()


async def _claim(source: ImageBuild, key: str, image_tag: str, worker_id: int) -> BuildTask | None:
    """Atomically claim the build. Returns the claimed row, or None if another
    worker holds a live lease."""
    now = utc_now()
    async with get_sql_session() as session:
        dialect = session.bind.dialect.name
        insert = pg_insert if dialect == "postgresql" else sqlite_insert
        stmt = (
            insert(BuildTask)
            .values(
                id=get_snowflake_id(),
                image_key=key,
                image_tag=image_tag,
                git_remote=source.git_remote,
                git_sha=source.git_sha,
                dockerfile=source.dockerfile,
                status=BUILD_BUILDING,
                holder_worker_id=worker_id,
                lease_expires_at=_lease_until(),
                attempts=1,
                started_at=now,
            )
            .on_conflict_do_nothing(index_elements=["image_key"])
            .returning(BuildTask.id)
        )
        inserted_id = (await session.execute(stmt)).scalar_one_or_none()
        if inserted_id is None:
            # Row exists — try to reclaim a stale (expired) or retryable-failed lease.
            reclaim = (
                update(BuildTask)
                .where(
                    BuildTask.image_key == key,
                    (BuildTask.lease_expires_at < now)
                    | ((BuildTask.status == BUILD_FAILED) & (BuildTask.attempts <= BuildTask.max_retries)),
                )
                .values(
                    status=BUILD_BUILDING,
                    holder_worker_id=worker_id,
                    lease_expires_at=_lease_until(),
                    attempts=BuildTask.attempts + 1,
                    started_at=now,
                    error=None,
                )
                .returning(BuildTask.id)
            )
            reclaimed_id = (await session.execute(reclaim)).scalar_one_or_none()
            await session.commit()
            if reclaimed_id is None:
                return None
            return (await session.execute(select(BuildTask).where(BuildTask.id == reclaimed_id))).scalar_one()
        await session.commit()
        return (await session.execute(select(BuildTask).where(BuildTask.id == inserted_id))).scalar_one()


async def _finish(build_task_id: int, *, status: str, error: str | None) -> None:
    async with get_sql_session() as session:
        await session.execute(
            update(BuildTask)
            .where(BuildTask.id == build_task_id)
            .values(status=status, error=error, finished_at=utc_now())
        )
        await session.commit()


async def ensure_image(source: ImageBuild, worker_id: int) -> EnsuredImage:
    """Build (or wait for) the image for ``source``; return its tag and the
    ``BuildTask`` id it resolved to. Exactly one build runs per image identity."""
    key = image_key(source)
    image_tag = compute_image_tag(source.git_sha)
    while True:
        row = await _get_row(key)
        if row is not None:
            if row.status == BUILD_READY:
                return EnsuredImage(image_tag, row.id)
            if row.status == BUILD_FAILED and row.attempts > row.max_retries:
                raise BuildFailed(f"image build failed (BuildTask {row.id}): {row.error}")

        claimed = await _claim(source, key, image_tag, worker_id)
        if claimed is None:
            await asyncio.sleep(BUILD_POLL_INTERVAL)
            continue

        try:
            await build_image_to_tag(source, image_tag)
        except BaseException as e:  # noqa: BLE001 — record the failure, then loop to retry-or-raise
            await _finish(claimed.id, status=BUILD_FAILED, error=f"{type(e).__name__}: {e}")
            continue

        await _finish(claimed.id, status=BUILD_READY, error=None)
        return EnsuredImage(image_tag, claimed.id)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest aaiclick/orchestration/execution/test_image_builder.py -v`
Expected: PASS (all three).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/execution/image_builder.py aaiclick/orchestration/execution/test_image_builder.py
git commit -m "feat: add ensure_image() on-demand build claim/lease seam"
```

---

## Task 5: Wire `ensure_image()` into the docker and kubernetes workers

Carry the resolved image source through `JobDispatch`, and have each image runner call `ensure_image()` for build sources before launch, stamping `build_task_id` on the task.

**Files:**
- Modify: `aaiclick/orchestration/execution/worker.py` (`JobDispatch`)
- Modify: `aaiclick/orchestration/execution/dispatch.py` (populate `image_source`)
- Modify: `aaiclick/orchestration/execution/docker_worker.py`, `kubernetes_worker.py`
- Test: `aaiclick/orchestration/execution/test_image_builder.py` (add a stamping test), `execution/test_dispatch.py` (update `JobDispatch` construction)

**Interfaces:**
- Consumes: `ensure_image`, `EnsuredImage`, `ImageBuild`.
- Produces: `JobDispatch.image_source: ImageSourceT | None` (new trailing field, default `None`); `async def _ensure_built_image(task, dispatch, worker_id) -> str` helper in each worker that returns the tag and records the link.

> **Caveat (intentional, single-commit window):** until Task 6 removes submission-time injection, a build job has *both* the injected `docker_build` task and this on-demand path. Both build the same content-addressed tag through the same idempotent cache, so the second is a no-op — correct, just briefly redundant.

- [ ] **Step 1: Write the failing test**

Add to `test_image_builder.py`:

```python
async def test_ensure_image_stamps_build_task_id_on_task(orch_ctx_no_ch, monkeypatch):
    from ..factories import create_task
    from ..models import Task
    from .docker_worker import _ensure_built_image
    from .worker import JobDispatch

    monkeypatch.setattr(image_builder, "build_image_to_tag", AsyncMock())
    source = _source(sha="d" * 40)
    task = create_task("mod.fn")
    async with get_sql_session() as session:
        session.add(task)
        await session.commit()

    dispatch = JobDispatch(
        runner_mode="docker",
        image_tag=docker_config.compute_image_tag("d" * 40),
        kubernetes_config=None,
        image_source=source,
    )
    tag = await _ensure_built_image(task, dispatch, worker_id=7)

    assert tag == docker_config.compute_image_tag("d" * 40)
    async with get_sql_session() as session:
        row = (await session.execute(select(Task).where(Task.id == task.id))).scalar_one()
    assert row.build_task_id is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/execution/test_image_builder.py::test_ensure_image_stamps_build_task_id_on_task -v`
Expected: FAIL — `JobDispatch` has no `image_source` / `_ensure_built_image` missing.

- [ ] **Step 3: Add `image_source` to `JobDispatch`**

In `worker.py`, add a trailing field to the `JobDispatch` NamedTuple (after `command_env`):

```python
    image_source: "ImageSourceT | None" = None
```

Add `from ..runner_config import ImageSourceT` to the import group (and keep `from __future__ import annotations` if the forward ref needs it — `worker.py` already declares it at the top).

- [ ] **Step 4: Populate `image_source` in dispatch**

In `dispatch.py` `_resolve_dispatch`, capture the source alongside the tag:

```python
    runner = parse_runner_config(job.runner) if job.runner else None
    image_tag = effective_image_tag(runner) if runner is not None else None
    image_source = getattr(runner, "image", None)
    return JobDispatch(
        job.runner_mode,
        image_tag,
        _kube_dict(runner),
        task.entry_type,
        task.command,
        task.command_env,
        image_source,
    )
```

- [ ] **Step 5: Add `_ensure_built_image` and call it in the docker worker**

In `docker_worker.py`, add to the imports: `update` to the existing `from sqlmodel import select` line's neighbours via `from sqlalchemy import update` (external group), and to the current-package group:

```python
from ..runner_config import ENTRY_SHELL, ImageBuild   # extend the existing ENTRY_SHELL import
from .image_builder import ensure_image
```

Then add a helper above `_run_task_in_container`:

```python
async def _ensure_built_image(task: Task, dispatch: JobDispatch, worker_id: int) -> str:
    """For a build-source job, build the image on demand (once, shared) and link
    the task to its BuildTask. For a prebuilt image, return the dispatch tag as-is."""
    if not isinstance(dispatch.image_source, ImageBuild):
        return _require_image_tag(task, dispatch.image_tag)
    ensured = await ensure_image(dispatch.image_source, worker_id)
    async with get_sql_session() as session:
        await session.execute(
            update(Task).where(Task.id == task.id).values(build_task_id=ensured.build_task_id)
        )
        await session.commit()
    return ensured.image_tag
```

Then in `_run_task_in_container`, replace:

```python
    image_tag = _require_image_tag(task, dispatch.image_tag)
```

with:

```python
    image_tag = await _ensure_built_image(task, dispatch, worker_id)
```

- [ ] **Step 6: Do the same in the kubernetes worker**

In `kubernetes_worker.py`, add `from sqlalchemy import update`, `from ..runner_config import ImageBuild`, `from .image_builder import ensure_image`, the same `_ensure_built_image` helper, and call it at the top of `_run_task_in_pod` before `_pod_spec_from`, passing the ensured tag into the spec. Simplest: build the image first, then construct the spec from a dispatch whose `image_tag` is the ensured tag:

```python
async def _run_task_in_pod(task, worker_id, dispatch):
    image_tag = await _ensure_built_image(task, dispatch, worker_id)
    spec = _pod_spec_from(task, dispatch._replace(image_tag=image_tag))
    ...
```

- [ ] **Step 7: Run the tests**

Run: `pytest aaiclick/orchestration/execution/test_image_builder.py aaiclick/orchestration/execution/test_dispatch.py -v`
Expected: PASS. (If `test_dispatch.py` constructs `JobDispatch` positionally, the new trailing default keeps it valid; no change needed there.)

- [ ] **Step 8: Commit**

```bash
git add aaiclick/orchestration/execution/worker.py aaiclick/orchestration/execution/dispatch.py \
        aaiclick/orchestration/execution/docker_worker.py aaiclick/orchestration/execution/kubernetes_worker.py \
        aaiclick/orchestration/execution/test_image_builder.py
git commit -m "feat: build images on demand at dispatch via ensure_image()"
```

---

## Task 6: Remove submission-time build-task injection

**Files:**
- Modify: `aaiclick/orchestration/factories.py`
- Test: `aaiclick/orchestration/test_orchestration_factories.py`

**Interfaces:**
- `create_built_job()` no longer creates a `docker_build` task or a build dependency; a docker/k8s job is `Job` + `entry_task` only.

- [ ] **Step 1: Update the failing test**

In `test_orchestration_factories.py`, replace `test_build_job_injects_build_task` with the inverse:

```python
async def test_build_job_injects_no_build_task(orch_ctx_no_ch):
    """Build images are produced on demand at dispatch, not injected at
    submission — so no docker_build task appears in the job graph."""
    runner = DockerRunner(image=ImageBuild(git_remote="git@x:r.git", git_sha="c" * 40))
    job = await create_built_job(name="j", entrypoint="mod.fn", runner=runner, entry_type="module")
    entrypoints = await _task_entrypoints(job.id)
    assert BUILD_TASK_ENTRYPOINT not in entrypoints
    assert entrypoints == ["mod.fn"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/test_orchestration_factories.py::test_build_job_injects_no_build_task -v`
Expected: FAIL — the build task is still injected.

- [ ] **Step 3: Remove the injection block**

In `factories.py` `create_built_job`, delete the whole `if isinstance(runner.image, ImageBuild):` block (the `build_task` creation, `entry_task.depends_on(build_task)`, and `to_add.append(build_task)`), leaving:

```python
    to_add = [job, entry_task]

    async with get_sql_session() as session:
        for obj in to_add:
            session.add(obj)
        await session.commit()
```

Remove the now-unused imports `BUILD_TASK_ENTRYPOINT` and `ImageBuild` from `factories.py` if they are no longer referenced.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest aaiclick/orchestration/test_orchestration_factories.py -v`
Expected: PASS (including `test_prebuilt_job_injects_no_build_task`, unchanged).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/factories.py aaiclick/orchestration/test_orchestration_factories.py
git commit -m "feat: stop injecting docker_build task at submission"
```

---

## Task 7: Delete the dead build-task code path

With injection gone and on-demand build live, the `@task build_image` entrypoint and its dispatch special-case are dead.

**Files:**
- Modify: `aaiclick/orchestration/execution/docker_build.py` (remove `build_image` task + now-unused job helpers)
- Modify: `aaiclick/orchestration/execution/dispatch.py` (remove `BUILD_TASK_ENTRYPOINT` branch)
- Modify: `aaiclick/orchestration/docker_config.py` (remove `BUILD_TASK_ENTRYPOINT` constant)
- Modify: `aaiclick/orchestration/execution/test_dispatch.py` (remove the build-task-subprocess test)
- Modify: `aaiclick/orchestration/models.py` docstring (`RunnerMode`) — drop "auto-injects a build task" wording.

**Interfaces:**
- Removes: `build_image` task, `BUILD_TASK_ENTRYPOINT`, the `_resolve_dispatch` build-task branch.

- [ ] **Step 1: Update the dispatch test**

In `test_dispatch.py`, delete `test_resolve_dispatch_build_task_always_subprocess` and the `BUILD_TASK_ENTRYPOINT` import.

- [ ] **Step 2: Remove the dispatch special-case**

In `dispatch.py`, delete:

```python
    if task.entrypoint == BUILD_TASK_ENTRYPOINT:
        return JobDispatch(RUNNER_SUBPROCESS, None, None)
```

and remove `BUILD_TASK_ENTRYPOINT` from the `from ..docker_config import …` line (keep `effective_image_tag`).

- [ ] **Step 3: Remove the build task and its job helpers**

In `docker_build.py`, delete `build_image` (the `@task`), and the now-unused `_build_source`, `_fetch_job`, and the `@task`/`Job`/`select`/`get_sql_session`/`parse_runner_config`/`effective_image_tag` imports that only it used. Keep `build_image_to_tag` and all low-level helpers (`_docker_*`, `_git_clone_at_sha`, `_collect_build_args`, `add_host_flags` import). Update `test_docker_build.py`: drop `test_build_source_from_runner` (it tested `_build_source`).

In `docker_config.py`, delete the `BUILD_TASK_ENTRYPOINT` constant and its docstring.

- [ ] **Step 4: Grep for stragglers**

Run: `grep -rn "BUILD_TASK_ENTRYPOINT\|build_image\b\|_build_source\|docker_build.build_image" aaiclick/`
Expected: no remaining references except `build_image_to_tag`. Fix any that remain.

- [ ] **Step 5: Run the affected suites**

Run: `pytest aaiclick/orchestration/execution/test_dispatch.py aaiclick/orchestration/execution/test_docker_build.py aaiclick/orchestration/test_docker_config.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/execution/docker_build.py aaiclick/orchestration/execution/dispatch.py \
        aaiclick/orchestration/docker_config.py aaiclick/orchestration/execution/test_dispatch.py \
        aaiclick/orchestration/execution/test_docker_build.py aaiclick/orchestration/models.py
git commit -m "refactor: remove dead submission-time build task path"
```

---

## Task 8: Generate the migration and run the full suite

**Files:**
- Create: `aaiclick/orchestration/migrations/versions/<rev>_add_build_tasks.py` (generated)

- [ ] **Step 1: Generate the migration**

Invoke the `generate-migration` skill (GitHub Actions) to autogenerate a migration for the new `build_tasks` table and the `tasks.build_task_id` column. Do not hand-write it.

- [ ] **Step 2: Review the generated migration**

Confirm it creates `build_tasks` with the `UNIQUE(image_key)` constraint and indexes (`image_key`, `status`), and adds `tasks.build_task_id` with its FK + index. No CHECK constraints on `status`.

- [ ] **Step 3: Run the full orchestration suite**

Run: `pytest aaiclick/orchestration -q`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add aaiclick/orchestration/migrations/versions/
git commit -m "chore: add build_tasks migration"
```

- [ ] **Step 5: Verify CI**

Use the `check-pr` skill after pushing to confirm GitHub Actions (local + distributed backends) are green; fix any failures.

---

## Self-Review

**Spec coverage**

| Spec section | Task |
|---|---|
| BuildTask model + table | Task 1 |
| `BuildStatus` Literal | Task 1 |
| `image_key` identity | Task 2 |
| `ensure_image()` dispatch seam | Task 4, wired in Task 5 |
| Dedup guarantee (UNIQUE + atomic claim) | Task 1 (constraint), Task 4 (claim) |
| Crashed-builder reclaim (lease) | Task 4 (`_claim` reclaim branch) |
| Submission-injection removal | Task 6 |
| Job↔build linkage / visibility | Task 1 (`build_task_id`, `log_path`), Task 5 (stamping) |
| Failure & retry semantics | Task 4 (`attempts`/`max_retries`), Task 5 |
| Reused vs deleted | Task 3 (reuse), Task 7 (delete) |
| Migration | Task 8 |

**Notes carried for the implementer**

- `log_path` is captured on the `BuildTask` for build-log visibility; wiring the build's stdout into it is a follow-up beyond this plan (the column and link exist now).
- Task 5 adds `from sqlalchemy import update` to the docker/kubernetes workers; keep it in the external import group per the all-imports-at-top constraint.
- `attempts > max_retries` is the terminal-failure check; first claim sets `attempts=1`, reclaim increments — giving `max_retries + 1` total attempts (3 by default), matching today's `max_retries=2` task.
