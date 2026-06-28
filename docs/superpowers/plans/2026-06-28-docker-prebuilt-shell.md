# Prebuilt Images & Shell Tasks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let docker/kubernetes jobs run against a prebuilt image (no build stage) and add a `shell` task entry type that runs an arbitrary argv in the container, folding the per-job docker/k8s columns into one typed `runner` config.

**Architecture:** Two orthogonal, discriminated-union configs — `entry_type` (`module`/`shell`) on `Task`, and a `runner` config (`subprocess`/`docker`/`kubernetes`, with a nested `build`/`prebuilt` image source) on `Job`/`RegisteredJob`. The build task is injected only for a `build` image source; `shell` tasks bypass the in-container bootstrap shim and `execute_task`, succeeding on container exit code 0.

**Tech Stack:** Python 3.12, SQLModel/SQLAlchemy, Pydantic v2 (discriminated unions + `TypeAdapter`), Alembic, pytest (`pytest-asyncio` auto mode), Docker/kubectl CLIs.

**Spec:** `docs/superpowers/specs/2026-06-28-docker-prebuilt-shell-design.md`

**Ordering invariant:** Each task leaves the full suite green. New columns are added *alongside* the old ones (Phase 2), every reader/writer is swapped to the new `runner` config (Phases 3–5), then the now-dead flat columns are dropped last (Phase 8). Never drop a column while code still reads it.

**Migrations:** Per `CLAUDE.md`, never hand-write Alembic files — use the `generate-migration` skill. Tasks below say *when* to generate; the skill produces the file.

**Run tests with:** `pytest <path>::<test> -v` (local default backend = chdb + SQLite, no infra). Distributed-only paths are covered by GitHub Actions and `test_e2e/docker/`.

---

## File Structure

| File | Responsibility | Change |
| --- | --- | --- |
| `aaiclick/orchestration/runner_config.py` | Pydantic discriminated unions (`ImageSource`, `RunnerConfig`, `EntryType` constants) + parse/dump/validate helpers | **Create** |
| `aaiclick/orchestration/test_runner_config.py` | Unit tests for the unions + validation | **Create** |
| `aaiclick/orchestration/models.py` | `Task.entry_type/command/command_env`; `Job.runner`; `RegisteredJob.runner`; constants | Modify |
| `aaiclick/orchestration/docker_config.py` | Build `RunnerConfig` from inputs; `effective_image_tag`; prebuilt path | Modify |
| `aaiclick/orchestration/factories.py` | `create_task(entry_type=…)`; conditional build-task injection; consume `RunnerConfig` | Modify |
| `aaiclick/orchestration/execution/dispatch.py` | Read `runner` config / effective tag into `JobDispatch` | Modify |
| `aaiclick/orchestration/execution/worker.py` | `JobDispatch` carries entry/runner info | Modify |
| `aaiclick/orchestration/execution/docker_worker.py` | Shell branch in `_build_docker_run_cmd`; exit-code result for shell | Modify |
| `aaiclick/orchestration/execution/docker_build.py` | Read git fields from the `build` source | Modify |
| `aaiclick/orchestration/execution/kubernetes_worker.py` | Mirror prebuilt + shell | Modify |
| `aaiclick/orchestration/registered_jobs.py` | `run_job`/`register_job`/`upsert_registered_job` params | Modify |
| `aaiclick/view_models.py` | `RunJobRequest`/`RegisterJobRequest` fields | Modify |
| CLI module (see Task 12) | `--entry-type`/`--command`/`--command-env`/`--image` flags | Modify |
| `docs/orchestration.md` | Prebuilt + shell + **Execution layers** section | Modify |
| `docs/future.md` | Out-of-scope items | Modify |
| Alembic migration(s) | Additive (Phase 2) + drop (Phase 8) | Generate |

---

## Phase 1 — Config models (pure, no wiring)

### Task 1: `EntryType` constants and discriminated-union config models

**Files:**
- Create: `aaiclick/orchestration/runner_config.py`
- Test: `aaiclick/orchestration/test_runner_config.py`

- [ ] **Step 1: Write the failing test**

```python
# aaiclick/orchestration/test_runner_config.py
import pytest
from pydantic import ValidationError

from aaiclick.orchestration.runner_config import (
    DockerRunner,
    ImageBuild,
    ImagePrebuilt,
    KubernetesRunner,
    SubprocessRunner,
    dump_runner_config,
    parse_runner_config,
)


def test_parse_docker_build_runner_roundtrips():
    cfg = parse_runner_config(
        {"type": "docker", "image": {"type": "build", "git_remote": "git@x:r.git", "git_sha": "a" * 40}}
    )
    assert isinstance(cfg, DockerRunner)
    assert isinstance(cfg.image, ImageBuild)
    assert dump_runner_config(cfg)["image"]["git_sha"] == "a" * 40


def test_parse_docker_prebuilt_runner():
    cfg = parse_runner_config({"type": "docker", "image": {"type": "prebuilt", "image_tag": "python:3.12"}})
    assert isinstance(cfg.image, ImagePrebuilt)
    assert cfg.image.image_tag == "python:3.12"


def test_subprocess_runner_has_no_image():
    cfg = parse_runner_config({"type": "subprocess"})
    assert isinstance(cfg, SubprocessRunner)
    assert not hasattr(cfg, "image")


def test_unknown_runner_type_rejected():
    with pytest.raises(ValidationError):
        parse_runner_config({"type": "nope"})


def test_prebuilt_requires_nonempty_image_tag():
    with pytest.raises(ValidationError, match="image_tag"):
        ImagePrebuilt(image_tag="")


def test_kubernetes_runner_optional_cluster_fields():
    cfg = parse_runner_config(
        {"type": "kubernetes", "image": {"type": "prebuilt", "image_tag": "python:3.12"}, "namespace": "ml"}
    )
    assert isinstance(cfg, KubernetesRunner)
    assert cfg.namespace == "ml"
    assert cfg.service_account is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/test_runner_config.py -v`
Expected: FAIL — `ModuleNotFoundError: aaiclick.orchestration.runner_config`

- [ ] **Step 3: Write minimal implementation**

```python
# aaiclick/orchestration/runner_config.py
"""Typed, discriminated configs for a job's runner and a task's entry.

A job's image/runner settings (formerly the flat ``git_*``/``image_tag``/
``kubernetes_config`` columns) collapse into one ``RunnerConfig`` serialized to
a JSON column; the ``entry_type`` discriminator selects how the container is
invoked. Pure data + validation — no env, no I/O — so any layer can import it.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, TypeAdapter, field_validator

# --- entry_type discriminator (lives on Task) -----------------------------
ENTRY_MODULE = "module"
ENTRY_SHELL = "shell"
EntryType = Literal["module", "shell"]
ENTRY_TYPES: list[EntryType] = [ENTRY_MODULE, ENTRY_SHELL]


# --- image source (nested in docker/kubernetes runners) -------------------
class ImageBuild(BaseModel):
    """Build the image from a git repo at a SHA. ``image_tag`` is computed
    (``aaiclick-job:<sha>``), not stored here."""

    type: Literal["build"] = "build"
    git_remote: str
    git_sha: str
    git_branch: str | None = None
    dockerfile: str | None = None


class ImagePrebuilt(BaseModel):
    """Use an existing image verbatim; no build task is injected."""

    type: Literal["prebuilt"] = "prebuilt"
    image_tag: str

    @field_validator("image_tag")
    @classmethod
    def _nonempty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("image_tag must be a non-empty image reference")
        return v


ImageSource = Annotated[ImageBuild | ImagePrebuilt, Field(discriminator="type")]


# --- runner (lives on Job / RegisteredJob) --------------------------------
class SubprocessRunner(BaseModel):
    type: Literal["subprocess"] = "subprocess"


class DockerRunner(BaseModel):
    type: Literal["docker"] = "docker"
    image: ImageSource


class KubernetesRunner(BaseModel):
    type: Literal["kubernetes"] = "kubernetes"
    image: ImageSource
    namespace: str | None = None
    service_account: str | None = None
    image_pull_secret: str | None = None


RunnerConfig = Annotated[
    SubprocessRunner | DockerRunner | KubernetesRunner,
    Field(discriminator="type"),
]

_RUNNER_ADAPTER: TypeAdapter[RunnerConfig] = TypeAdapter(RunnerConfig)
_IMAGE_ADAPTER: TypeAdapter[ImageSource] = TypeAdapter(ImageSource)

RunnerConfigT = SubprocessRunner | DockerRunner | KubernetesRunner
ImageSourceT = ImageBuild | ImagePrebuilt


def parse_runner_config(data: dict) -> RunnerConfigT:
    """Validate a JSON dict into the matching runner model."""
    return _RUNNER_ADAPTER.validate_python(data)


def dump_runner_config(cfg: RunnerConfigT) -> dict:
    """Serialize a runner model to a JSON-safe dict for the DB column."""
    return _RUNNER_ADAPTER.dump_python(cfg, mode="json")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest aaiclick/orchestration/test_runner_config.py -v`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/runner_config.py aaiclick/orchestration/test_runner_config.py
git commit -m "feat: add typed runner/entry config models"
```

### Task 2: Boundary validation helper (`validate_task_entry`)

Centralizes the cross-field rules from the spec so `run_job`/`register_job` and CLI share one check.

**Files:**
- Modify: `aaiclick/orchestration/runner_config.py`
- Test: `aaiclick/orchestration/test_runner_config.py`

- [ ] **Step 1: Write the failing test**

```python
# append to test_runner_config.py
from aaiclick.orchestration.runner_config import validate_task_entry


def test_shell_entry_requires_command():
    with pytest.raises(ValueError, match="shell.*requires.*command"):
        validate_task_entry(entry_type="shell", command=None, runner_type="docker")


def test_shell_entry_rejected_on_subprocess():
    with pytest.raises(ValueError, match="shell.*subprocess"):
        validate_task_entry(entry_type="shell", command=["echo", "hi"], runner_type="subprocess")


def test_module_entry_rejects_command():
    with pytest.raises(ValueError, match="module.*command"):
        validate_task_entry(entry_type="module", command=["echo", "hi"], runner_type="docker")


def test_valid_shell_entry_passes():
    validate_task_entry(entry_type="shell", command=["python", "main.py"], runner_type="docker")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/test_runner_config.py -k validate -v`
Expected: FAIL — `ImportError: cannot import name 'validate_task_entry'`

- [ ] **Step 3: Write minimal implementation**

```python
# add to runner_config.py
def validate_task_entry(
    *,
    entry_type: EntryType,
    command: list[str] | None,
    runner_type: Literal["subprocess", "docker", "kubernetes"],
) -> None:
    """Enforce the entry/runner cross-field rules (spec "Validation").

    Raises ``ValueError`` on violation; returns ``None`` when valid."""
    if entry_type == ENTRY_SHELL:
        if not command:
            raise ValueError("shell entry_type requires a non-empty command list")
        if runner_type == "subprocess":
            raise ValueError("shell entry_type is container-only; not valid on a subprocess runner")
    elif entry_type == ENTRY_MODULE:
        if command:
            raise ValueError("module entry_type does not take a command")
    else:
        raise ValueError(f"unknown entry_type {entry_type!r}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest aaiclick/orchestration/test_runner_config.py -k validate -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/runner_config.py aaiclick/orchestration/test_runner_config.py
git commit -m "feat: add validate_task_entry boundary check"
```

---

## Phase 2 — Additive schema (new columns alongside old)

### Task 3: Add new columns to models (keep old columns)

**Files:**
- Modify: `aaiclick/orchestration/models.py`
- Test: `aaiclick/orchestration/test_models_runner_columns.py` (Create)

- [ ] **Step 1: Write the failing test**

```python
# aaiclick/orchestration/test_models_runner_columns.py
from aaiclick.orchestration.models import Job, RegisteredJob, Task


def test_task_has_entry_columns():
    cols = Task.__table__.columns.keys()
    assert {"entry_type", "command", "command_env"} <= set(cols)


def test_job_has_runner_column():
    assert "runner" in Job.__table__.columns.keys()


def test_registered_job_has_runner_column():
    assert "runner" in RegisteredJob.__table__.columns.keys()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/test_models_runner_columns.py -v`
Expected: FAIL — `entry_type` / `runner` missing

- [ ] **Step 3: Write minimal implementation**

In `models.py`, import the entry constants near the top imports:

```python
from .runner_config import ENTRY_MODULE, EntryType
```

Add to `Task` (after the `kwargs` field, ~line 359). `entry_type` has **no** default — see spec; every creation site sets it. Make it nullable at the column level for the additive migration; Phase 8 finalizes not-null after backfill:

```python
    entry_type: EntryType = Field(sa_column=Column(String, nullable=True))
    command: list[str] | None = Field(default=None, sa_column=Column(JSON, nullable=True))
    command_env: dict[str, str] | None = Field(default=None, sa_column=Column(JSON, nullable=True))
```

Add to `Job` (after `kubernetes_config`, ~line 176) and `RegisteredJob` (after `kubernetes_config`, ~line 135):

```python
    runner: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON, nullable=True))
```

Leave the existing flat `git_*`/`dockerfile`/`image_tag`/`kubernetes_config` columns in place for now.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest aaiclick/orchestration/test_models_runner_columns.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/models.py aaiclick/orchestration/test_models_runner_columns.py
git commit -m "feat: add entry_type/command/command_env + runner columns (additive)"
```

### Task 4: Generate the additive migration

**Files:**
- Generate: `alembic/versions/<rev>_add_runner_and_entry_columns.py`

- [ ] **Step 1: Invoke the migration skill**

Use the `generate-migration` skill. It autogenerates from the model diff in Task 3 (three nullable `Task` columns + nullable `Job.runner` + nullable `RegisteredJob.runner`). Do **not** hand-write.

- [ ] **Step 2: Verify the migration applies**

Run: `python -m aaiclick migrate upgrade` (or the project's documented migration command)
Expected: upgrades to head with no error; `pytest aaiclick/orchestration/test_models_runner_columns.py -v` still PASS against the migrated schema.

- [ ] **Step 3: Commit**

```bash
git add alembic/versions/
git commit -m "feat: migration adding runner/entry columns (additive)"
```

---

## Phase 3 — Resolution & creation (write the new config; old columns become dead writes)

### Task 5: `docker_config` builds a `RunnerConfig`; add `effective_image_tag`

Replaces `DockerJobConfig`/`resolve_docker_config` (which returned the flat snapshot) with resolution into a `DockerRunner`/`KubernetesRunner`. Keeps the same precedence for `build`; routes to `prebuilt` when an `image` is supplied.

**Files:**
- Modify: `aaiclick/orchestration/docker_config.py`
- Test: `aaiclick/orchestration/test_docker_config.py` (extend; create if absent)

- [ ] **Step 1: Write the failing test**

```python
# aaiclick/orchestration/test_docker_config.py
import pytest

from aaiclick.orchestration.runner_config import DockerRunner, ImageBuild, ImagePrebuilt
from aaiclick.orchestration.docker_config import effective_image_tag, resolve_runner_config


async def test_resolve_prebuilt_image_skips_git(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    cfg = await resolve_runner_config(
        registered=None, runner_mode="docker", image="python:3.12", git_remote=None, git_sha=None
    )
    assert isinstance(cfg, DockerRunner)
    assert isinstance(cfg.image, ImagePrebuilt)
    assert effective_image_tag(cfg) == "python:3.12"


async def test_resolve_build_image_computes_tag(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    cfg = await resolve_runner_config(
        registered=None, runner_mode="docker", image=None,
        git_remote="git@x:r.git", git_sha="b" * 40, git_branch="main",
    )
    assert isinstance(cfg.image, ImageBuild)
    assert effective_image_tag(cfg) == f"aaiclick-job:{'b' * 40}"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/test_docker_config.py -v`
Expected: FAIL — `resolve_runner_config` / `effective_image_tag` not defined

- [ ] **Step 3: Write minimal implementation**

In `docker_config.py` add (keep `compute_image_tag`, `auto_detect_*`, `add_host_flags`, `_validate_sha`):

```python
from .runner_config import (
    DockerRunner,
    ImageBuild,
    ImagePrebuilt,
    ImageSourceT,
    KubernetesRunner,
    RunnerConfigT,
)


def effective_image_tag(runner: RunnerConfigT) -> str | None:
    """The image a container/pod actually runs: prebuilt tag verbatim, or the
    computed ``aaiclick-job:<sha>`` for a build source. ``None`` for subprocess."""
    image = getattr(runner, "image", None)
    if image is None:
        return None
    if isinstance(image, ImagePrebuilt):
        return image.image_tag
    return compute_image_tag(image.git_sha)


async def _resolve_image_source(
    registered,  # RegisteredJob | None
    *,
    image: str | None,
    git_remote: str | None,
    git_sha: str | None,
    git_branch: str | None,
    dockerfile: str | None,
) -> ImageSourceT:
    """Prebuilt when an explicit ``image`` is given (here or on the registered
    job); otherwise resolve the build coordinates via the existing precedence."""
    registered_image = _registered_image(registered)
    if image is not None:
        return ImagePrebuilt(image_tag=image)
    if registered_image is not None:
        return ImagePrebuilt(image_tag=registered_image)

    remote = git_remote
    if remote is None and registered is not None:
        remote = registered.git_remote
    if remote is None:
        remote = await auto_detect_git_remote()
    sha = _validate_sha(git_sha) if git_sha else await auto_detect_git_sha()
    branch = git_branch if git_branch is not None else await auto_detect_git_branch()
    dfile = dockerfile
    if dfile is None and registered is not None:
        dfile = registered.dockerfile
    return ImageBuild(git_remote=remote, git_sha=sha, git_branch=branch, dockerfile=dfile)


async def resolve_runner_config(
    registered,  # RegisteredJob | None
    *,
    runner_mode,  # RunnerMode
    image: str | None = None,
    git_remote: str | None = None,
    git_sha: str | None = None,
    git_branch: str | None = None,
    dockerfile: str | None = None,
    kubernetes_config: dict | None = None,
) -> RunnerConfigT:
    """Resolve the per-run runner config. ``image`` (prebuilt) and the git
    fields (build) are mutually exclusive; the caller enforces that."""
    source = await _resolve_image_source(
        registered, image=image, git_remote=git_remote, git_sha=git_sha,
        git_branch=git_branch, dockerfile=dockerfile,
    )
    if runner_mode == "kubernetes":
        kc = kubernetes_config or {}
        return KubernetesRunner(
            image=source,
            namespace=kc.get("namespace"),
            service_account=kc.get("service_account"),
            image_pull_secret=kc.get("image_pull_secret"),
        )
    return DockerRunner(image=source)
```

Add the small `_registered_image` helper that reads the registered job's prebuilt image off its `runner` config once Task 11 populates it; until then it returns `None`:

```python
def _registered_image(registered) -> str | None:
    """Prebuilt image_tag default from a RegisteredJob's runner config, if any."""
    if registered is None or registered.runner is None:
        return None
    img = registered.runner.get("image") if isinstance(registered.runner, dict) else None
    if isinstance(img, dict) and img.get("type") == "prebuilt":
        return img.get("image_tag")
    return None
```

Keep the old `DockerJobConfig`/`resolve_docker_config` temporarily so unported callers compile; they are removed in Task 7.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest aaiclick/orchestration/test_docker_config.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/docker_config.py aaiclick/orchestration/test_docker_config.py
git commit -m "feat: resolve_runner_config + effective_image_tag (prebuilt/build)"
```

### Task 6: `create_task` accepts `entry_type`/`command`/`command_env`

**Files:**
- Modify: `aaiclick/orchestration/factories.py:127-173`
- Test: `aaiclick/orchestration/test_orchestration_factories.py` (extend)

- [ ] **Step 1: Write the failing test**

```python
# in test_orchestration_factories.py
from aaiclick.orchestration.factories import create_task
from aaiclick.orchestration.runner_config import ENTRY_MODULE, ENTRY_SHELL


def test_create_task_module_default_explicit():
    t = create_task("mod.fn", {"a": 1}, entry_type=ENTRY_MODULE)
    assert t.entry_type == ENTRY_MODULE
    assert t.command is None


def test_create_task_shell_carries_command():
    t = create_task(None, name="run", entry_type=ENTRY_SHELL, command=["python", "main.py"], command_env={"K": "v"})
    assert t.entry_type == ENTRY_SHELL
    assert t.command == ["python", "main.py"]
    assert t.command_env == {"K": "v"}
    assert t.entrypoint == ""  # shell tasks have no module entrypoint
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/test_orchestration_factories.py -k create_task -v`
Expected: FAIL — `create_task() got an unexpected keyword argument 'entry_type'`

- [ ] **Step 3: Write minimal implementation**

Update `create_task` signature and body. `callback` becomes optional (shell tasks have no module path); `entry_type` is required (no implicit default, per spec):

```python
def create_task(
    callback: str | Callable | None = None,
    kwargs: dict | None = None,
    *,
    name: str | None = None,
    max_retries: int = 0,
    entry_type: EntryType = ENTRY_MODULE,
    command: list[str] | None = None,
    command_env: dict[str, str] | None = None,
) -> Task:
    task_id = get_snowflake_id()
    if entry_type == ENTRY_SHELL:
        entrypoint = ""
        resolved_name = name or (command[0] if command else "shell")
    elif callable(callback):
        entrypoint = _callable_to_string(callback)
        resolved_name = name or getattr(callback, "__name__", entrypoint.rsplit(".", 1)[-1])
    else:
        entrypoint = callback or ""
        resolved_name = name or entrypoint.rsplit(".", 1)[-1]

    task = Task(
        id=task_id,
        entrypoint=entrypoint,
        name=resolved_name,
        kwargs=kwargs or {},
        entry_type=entry_type,
        command=command,
        command_env=command_env,
        status=TASK_PENDING,
        created_at=utc_now(),
        max_retries=max_retries,
    )
    registry = get_task_registry()
    if registry is not None:
        registry[task_id] = task
    return task
```

Add to the `factories.py` imports: `from .runner_config import ENTRY_MODULE, ENTRY_SHELL, EntryType`.

Note: `create_job` (line 231) calls `create_task(entry)` — still valid since `entry_type` defaults to `ENTRY_MODULE` for the in-code subprocess path. The "no implicit default" rule from the spec is enforced at the *submission boundary* (`run_job`/CLI/API), which always passes `entry_type` explicitly; the function keeps a `module` default only so internal subprocess callers stay terse.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest aaiclick/orchestration/test_orchestration_factories.py -k create_task -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/factories.py aaiclick/orchestration/test_orchestration_factories.py
git commit -m "feat: create_task supports shell entry_type"
```

### Task 7: Conditional build-task injection + write `runner` config in factories

**Files:**
- Modify: `aaiclick/orchestration/factories.py:254-340`
- Modify: `aaiclick/orchestration/registered_jobs.py:355-389` (pass `RunnerConfig` through)
- Test: `aaiclick/orchestration/test_orchestration_factories.py`

- [ ] **Step 1: Write the failing test**

```python
# in test_orchestration_factories.py — uses the orch fixtures (see conftest)
import pytest
from sqlmodel import select

from aaiclick.orchestration.factories import create_built_job
from aaiclick.orchestration.docker_config import BUILD_TASK_ENTRYPOINT
from aaiclick.orchestration.runner_config import DockerRunner, ImageBuild, ImagePrebuilt
from aaiclick.orchestration.models import Task
from aaiclick.orchestration.orch_context import get_sql_session


async def _task_entrypoints(job_id: int) -> list[str]:
    async with get_sql_session() as session:
        rows = (await session.execute(select(Task).where(Task.job_id == job_id))).scalars().all()
    return [t.entrypoint for t in rows]


async def test_prebuilt_job_injects_no_build_task(orch):
    runner = DockerRunner(image=ImagePrebuilt(image_tag="python:3.12"))
    job = await create_built_job(
        name="j", entrypoint="", runner=runner, entry_type="shell", command=["echo", "hi"]
    )
    assert BUILD_TASK_ENTRYPOINT not in await _task_entrypoints(job.id)
    assert job.runner["image"]["type"] == "prebuilt"


async def test_build_job_injects_build_task(orch):
    runner = DockerRunner(image=ImageBuild(git_remote="git@x:r.git", git_sha="c" * 40))
    job = await create_built_job(name="j", entrypoint="mod.fn", runner=runner, entry_type="module")
    assert BUILD_TASK_ENTRYPOINT in await _task_entrypoints(job.id)
```

(Use whatever the existing factory tests use for the DB fixture; name it to match `conftest.py`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/test_orchestration_factories.py -k build_job -v`
Expected: FAIL — `create_built_job` signature mismatch / not exported

- [ ] **Step 3: Write minimal implementation**

Rewrite `_create_built_job` → public `create_built_job` taking a `RunnerConfig` + entry fields, injecting the build task only for an `ImageBuild` source:

```python
from .runner_config import ENTRY_MODULE, EntryType, RunnerConfigT, dump_runner_config
from .runner_config import ImageBuild  # for the isinstance check


async def create_built_job(
    *,
    name: str,
    entrypoint: str,
    runner: RunnerConfigT,
    entry_type: EntryType = ENTRY_MODULE,
    command: list[str] | None = None,
    command_env: dict[str, str] | None = None,
    kwargs: dict | None = None,
    run_type: RunType = RUN_MANUAL,
    registered_job_id: int | None = None,
    preservation_mode: PreservationMode | None = None,
    registered: RegisteredJob | None = None,
) -> Job:
    """Create a docker/kubernetes Job from a resolved RunnerConfig. The build
    task is injected only when the image source is ``build``."""
    mode = resolve_job_config(preservation_mode, registered)
    job_id = get_snowflake_id()
    job = Job(
        id=job_id,
        name=name,
        status=JOB_PENDING,
        run_type=run_type,
        registered_job_id=registered_job_id,
        preservation_mode=mode,
        runner_mode=runner.type,
        runner=dump_runner_config(runner),
        created_at=utc_now(),
    )

    entry_task = create_task(
        entrypoint or None, kwargs or {}, name=name,
        entry_type=entry_type, command=command, command_env=command_env,
    )
    entry_task.job_id = job_id

    to_add = [job, entry_task]
    if isinstance(runner.image, ImageBuild):
        build_task = create_task(
            BUILD_TASK_ENTRYPOINT, {"job_id": job_id}, name="docker_build",
            max_retries=2, entry_type=ENTRY_MODULE,
        )
        build_task.job_id = job_id
        entry_task.depends_on(build_task)
        to_add.append(build_task)

    async with get_sql_session() as session:
        for obj in to_add:
            session.add(obj)
        await session.commit()

    registry = get_task_registry()
    if registry is not None:
        for obj in to_add[1:]:
            registry.pop(obj.id, None)
    return job
```

Delete the now-unused `create_docker_job`/`create_kubernetes_job` wrappers (and the `DockerJobConfig` import); update `registered_jobs.run_job` (Task 11 covers the param surface, but make it compile now) to call `create_built_job` with the `RunnerConfig` from `resolve_runner_config`. Remove the stale `DockerJobConfig`/`resolve_docker_config` from `docker_config.py`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest aaiclick/orchestration/test_orchestration_factories.py -k build_job -v`
Expected: PASS

- [ ] **Step 5: Run the broader suite to catch ripple**

Run: `pytest aaiclick/orchestration/ -q`
Expected: PASS (fix any caller still importing `create_docker_job`/`DockerJobConfig`).

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/factories.py aaiclick/orchestration/docker_config.py aaiclick/orchestration/registered_jobs.py aaiclick/orchestration/test_orchestration_factories.py
git commit -m "feat: conditional build-task injection from RunnerConfig"
```

---

## Phase 4 — Dispatch & docker worker

### Task 8: `JobDispatch` + `dispatch` read entry/runner; effective tag

**Files:**
- Modify: `aaiclick/orchestration/execution/worker.py:71-81`
- Modify: `aaiclick/orchestration/execution/dispatch.py`
- Test: `aaiclick/orchestration/execution/test_dispatch.py` (extend; create if absent)

- [ ] **Step 1: Write the failing test**

```python
# test_dispatch.py
from aaiclick.orchestration.execution.worker import JobDispatch


def test_jobdispatch_carries_entry_fields():
    d = JobDispatch(
        runner_mode="docker", image_tag="python:3.12", kubernetes_config=None,
        entry_type="shell", command=["echo", "hi"], command_env={"K": "v"},
    )
    assert d.entry_type == "shell"
    assert d.command == ["echo", "hi"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/execution/test_dispatch.py -v`
Expected: FAIL — unexpected keyword args to `JobDispatch`

- [ ] **Step 3: Write minimal implementation**

Extend `JobDispatch`:

```python
class JobDispatch(NamedTuple):
    runner_mode: RunnerMode
    image_tag: str | None
    kubernetes_config: dict | None
    entry_type: EntryType = ENTRY_MODULE
    command: list[str] | None = None
    command_env: dict[str, str] | None = None
```

Import `from ..runner_config import ENTRY_MODULE, EntryType` in `worker.py`.

In `dispatch.py:_resolve_dispatch`, read the task's entry fields and the job's runner config; derive image_tag via `effective_image_tag`, and keep `kubernetes_config` as the dict the k8s worker expects:

```python
from ..docker_config import effective_image_tag
from ..runner_config import parse_runner_config

async def _resolve_dispatch(task: Task) -> JobDispatch:
    if task.entrypoint == BUILD_TASK_ENTRYPOINT:
        return JobDispatch(RUNNER_SUBPROCESS, None, None)
    async with get_sql_session() as session:
        job = (await session.execute(select(Job).where(Job.id == task.job_id))).scalar_one_or_none()
    if job is None:
        return JobDispatch(RUNNER_SUBPROCESS, None, None)
    runner = parse_runner_config(job.runner) if job.runner else None
    image_tag = effective_image_tag(runner) if runner else None
    kube = getattr(runner, "kubernetes_config_dict", None)  # see note
    return JobDispatch(
        job.runner_mode, image_tag,
        _kube_dict(runner),
        task.entry_type, task.command, task.command_env,
    )
```

Add a small `_kube_dict(runner)` helper in `dispatch.py` returning `{"namespace":…, "service_account":…, "image_pull_secret":…}` from a `KubernetesRunner`, else `None`, so `kubernetes_worker._pod_spec_from` keeps its current `dispatch.kubernetes_config` contract.

- [ ] **Step 4: Run test + dispatch suite**

Run: `pytest aaiclick/orchestration/execution/test_dispatch.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/execution/worker.py aaiclick/orchestration/execution/dispatch.py aaiclick/orchestration/execution/test_dispatch.py
git commit -m "feat: dispatch reads entry/runner config into JobDispatch"
```

### Task 9: Docker worker — shell container command + exit-code result

**Files:**
- Modify: `aaiclick/orchestration/execution/docker_worker.py:80-127, 200-315`
- Test: `aaiclick/orchestration/execution/test_docker_worker.py` (extend)

- [ ] **Step 1: Write the failing test (command construction — pure, no docker)**

```python
# in test_docker_worker.py
from aaiclick.orchestration.execution.docker_worker import _build_docker_run_cmd
from aaiclick.orchestration.models import Task
from aaiclick.orchestration.runner_config import ENTRY_MODULE, ENTRY_SHELL


def _task(**kw):
    return Task(id=1, job_id=1, name="t", entrypoint=kw.get("entrypoint", ""), entry_type=kw["entry_type"],
               command=kw.get("command"), command_env=kw.get("command_env"))


def test_module_cmd_uses_bootstrap_shim():
    cmd = _build_docker_run_cmd(_task(entry_type=ENTRY_MODULE, entrypoint="m.f"),
                                "python:3.12", "/ipc", "/logs", {"A": "1"})
    assert cmd[-4:] == ["aaiclick.orchestration.execution.docker_worker", "--task-id", "1"][-3:] or "docker_worker" in " ".join(cmd)
    assert "-v" in cmd  # IPC mount present for module


def test_shell_cmd_runs_argv_no_ipc_no_runner_env():
    cmd = _build_docker_run_cmd(
        _task(entry_type=ENTRY_SHELL, command=["python", "main.py"], command_env={"K": "v"}),
        "python:3.12", "/ipc", "/logs", {"AAICLICK_SQL_URL": "secret"},
    )
    assert cmd[-3:] == ["python:3.12", "python", "main.py"]
    joined = " ".join(cmd)
    assert "AAICLICK_SQL_URL" not in joined  # runner env NOT injected for shell
    assert "K=v" in joined                   # only command_env injected
    assert "/aaiclick-ipc" not in joined     # no IPC mount
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/execution/test_docker_worker.py -k cmd -v`
Expected: FAIL — `_build_docker_run_cmd` takes the old positional signature

- [ ] **Step 3: Write minimal implementation**

Change `_build_docker_run_cmd` to take the `Task` and branch on `entry_type`. Module keeps today's behavior; shell runs the argv directly, mounts only the log base, and injects only `command_env`:

```python
def _build_docker_run_cmd(task, image_tag, ipc_dir, log_base, env):
    base = [_docker_bin(), "run", "--detach"]
    if task.entry_type == ENTRY_SHELL:
        cmd = [*base, "-v", f"{log_base}:{log_base}", *add_host_flags("AAICLICK_DOCKER_RUN_ADD_HOST")]
        for k, v in (task.command_env or {}).items():
            cmd.extend(["-e", f"{k}={v}"])
        cmd.append(image_tag)
        cmd.extend(task.command or [])
        return cmd
    # module: unchanged bootstrap shim with IPC mount + runner env
    cmd = [
        *base,
        "-v", f"{ipc_dir}:{CONTAINER_IPC_DIR}",
        "-v", f"{log_base}:{log_base}",
        "-e", f"AAICLICK_LOG_DIR={log_base}",
        *add_host_flags("AAICLICK_DOCKER_RUN_ADD_HOST"),
    ]
    for k, v in env.items():
        cmd.extend(["-e", f"{k}={v}"])
    cmd.extend([image_tag, "python", "-m", "aaiclick.orchestration.execution.docker_worker",
                "--task-id", str(task.id)])
    return cmd
```

Update `_DockerVehicle.launch` to pass `task` into `_build_docker_run_cmd`, and `_run_task_in_container` to build `env = build_runner_env()` only for module tasks (pass `{}` for shell; the env arg is ignored on the shell branch anyway). Update `collect`: for a shell task, synthesize the result from the exit code instead of reading `result.json`:

```python
# in _DockerVehicle.collect
if self._entry_type == ENTRY_SHELL:
    if was_cancelled:
        return RunnerResult(False, None, None, "cancelled")
    if error is not None:
        return RunnerResult(False, None, None, error)
    return RunnerResult(exit_code == 0, None, handle.log_path, None if exit_code == 0 else f"exit {exit_code}")
```

Capture container stdout/stderr to `handle.log_path` for shell: after `docker wait`, run `docker logs <id>` (stream=False) and write it to the per-task log file. Add a `_capture_container_logs(container_id, log_path)` host helper mirroring the k8s `_capture_pod_logs`, and have the shell branch compute a `log_path` under `get_logs_dir()/<job_id>/<task_id>/docker-<run_epoch>.log` (parents created).

Pass `entry_type`/`command`/`command_env` into `_DockerVehicle` (store on the vehicle) from `dispatch.image_tag`/`JobDispatch`; thread the `JobDispatch` into `_run_task_in_container` (it already receives `dispatch`).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest aaiclick/orchestration/execution/test_docker_worker.py -k cmd -v`
Expected: PASS

- [ ] **Step 5: Run the docker_worker suite**

Run: `pytest aaiclick/orchestration/execution/test_docker_worker.py -v`
Expected: PASS (update any test asserting the old positional `_build_docker_run_cmd` signature).

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/execution/docker_worker.py aaiclick/orchestration/execution/test_docker_worker.py
git commit -m "feat: docker worker shell branch (argv, no IPC, exit-code result)"
```

### Task 10: `docker_build` reads git fields from the build source

**Files:**
- Modify: `aaiclick/orchestration/execution/docker_build.py:116-156`
- Test: `aaiclick/orchestration/execution/test_docker_build.py` (extend)

- [ ] **Step 1: Write the failing test**

```python
# in test_docker_build.py — assert build reads git fields off job.runner
from aaiclick.orchestration.execution.docker_build import _build_source
from aaiclick.orchestration.models import Job


def test_build_source_from_runner():
    job = Job(id=1, name="j", run_type="MANUAL", runner_mode="docker",
              runner={"type": "docker", "image": {"type": "build", "git_remote": "r", "git_sha": "d" * 40}})
    src = _build_source(job)
    assert src.git_remote == "r" and src.git_sha == "d" * 40
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/execution/test_docker_build.py -k build_source -v`
Expected: FAIL — `_build_source` not defined

- [ ] **Step 3: Write minimal implementation**

Add `_build_source(job)` returning the `ImageBuild` from `parse_runner_config(job.runner).image` (raise if it's prebuilt — a build task must never be scheduled for a prebuilt job). Replace `job.git_remote`/`job.git_sha`/`job.dockerfile` reads in `build_image` and `_collect_build_args` with the `ImageBuild` fields and `effective_image_tag`.

- [ ] **Step 4: Run test + build suite**

Run: `pytest aaiclick/orchestration/execution/test_docker_build.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/execution/docker_build.py aaiclick/orchestration/execution/test_docker_build.py
git commit -m "feat: docker_build reads git fields from runner build source"
```

---

## Phase 5 — Kubernetes worker

### Task 11: Kubernetes worker — prebuilt + shell

**Files:**
- Modify: `aaiclick/orchestration/execution/kubernetes_worker.py:57-90, 183-249`
- Test: `aaiclick/orchestration/execution/test_kubernetes_worker.py` (extend; create if absent)

- [ ] **Step 1: Write the failing test (manifest construction — pure)**

```python
from aaiclick.orchestration.execution.kubernetes_worker import _build_pod_manifest


def test_shell_pod_runs_argv_only_command_env():
    m = _build_pod_manifest(
        name="p", namespace="default", image_tag="python:3.12", task_id=1, run_epoch=0,
        env={"AAICLICK_SQL_URL": "secret"}, service_account=None, image_pull_secret=None,
        resources=None, entry_type="shell", command=["python", "main.py"], command_env={"K": "v"},
    )
    c = m["spec"]["containers"][0]
    assert c["command"] == ["python", "main.py"]
    names = {e["name"] for e in c["env"]}
    assert names == {"K"}  # runner env excluded for shell
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/execution/test_kubernetes_worker.py -k shell_pod -v`
Expected: FAIL — `_build_pod_manifest` lacks `entry_type`/`command` params

- [ ] **Step 3: Write minimal implementation**

Add `entry_type`/`command`/`command_env` params to `_build_pod_manifest`. For `shell`, set `container["command"] = command` and `env` from `command_env` only; for `module`, keep the `POD_ENTRYPOINT … --task-id … --run-epoch …` shim with the runner env. Thread `entry_type`/`command`/`command_env` from `JobDispatch` through `_PodSpec`/`_KubernetesVehicle.launch`. In `collect`, for a shell pod synthesize the result from `exit_code` (no `RemoteTaskResult` row is written by a vanilla image), mirroring docker.

Also confirm `_pod_spec_from` reads `dispatch.image_tag` (now the effective tag) — no change needed beyond the entry fields.

- [ ] **Step 4: Run test + k8s suite**

Run: `pytest aaiclick/orchestration/execution/test_kubernetes_worker.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/execution/kubernetes_worker.py aaiclick/orchestration/execution/test_kubernetes_worker.py
git commit -m "feat: kubernetes worker prebuilt + shell support"
```

---

## Phase 6 — Submission surface (API + register)

### Task 12: `run_job` / `register_job` / `upsert_registered_job` params + validation

**Files:**
- Modify: `aaiclick/orchestration/registered_jobs.py:58-120, 137-…, 291-400`
- Test: `aaiclick/orchestration/test_registered_jobs.py` (extend; create if absent)

- [ ] **Step 1: Write the failing test**

```python
import pytest
from aaiclick.orchestration.registered_jobs import run_job


async def test_run_job_shell_requires_command(orch_distributed_or_skip):
    with pytest.raises(ValueError, match="shell.*command"):
        await run_job("j", "", entry_type="shell", command=None, image="python:3.12", runner_mode="docker")
```

(Match the existing distributed-mode guard/fixtures in the repo; if those paths require Postgres/CH, mark the test to skip locally like the other distributed tests do.)

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/test_registered_jobs.py -k shell -v`
Expected: FAIL — `run_job()` has no `entry_type`/`command`/`image` params

- [ ] **Step 3: Write minimal implementation**

Add `entry_type: EntryType = ENTRY_MODULE`, `command`, `command_env`, and `image` params to `run_job`. Before resolving, call `validate_task_entry(entry_type=…, command=…, runner_type=runner_mode)` and reject `image` together with any git field. Resolve via `resolve_runner_config(...)`, then `create_built_job(... runner=cfg, entry_type=…, command=…, command_env=…)`. Add `image`/prebuilt support to `register_job`/`upsert_registered_job` by storing a `runner` config dict (build the `RunnerConfig` from `runner_mode` + `image`/git defaults and `dump_runner_config`).

- [ ] **Step 4: Run test + suite**

Run: `pytest aaiclick/orchestration/test_registered_jobs.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/registered_jobs.py aaiclick/orchestration/test_registered_jobs.py
git commit -m "feat: run_job/register_job accept entry_type/command/image"
```

### Task 13: View models — `RunJobRequest` / `RegisterJobRequest`

**Files:**
- Modify: `aaiclick/view_models.py:110-156`
- Test: `aaiclick/test_view_models.py` (extend; create if absent)

- [ ] **Step 1: Write the failing test**

```python
from aaiclick.view_models import RegisterJobRequest, RunJobRequest


def test_run_job_request_has_shell_fields():
    r = RunJobRequest(name="j", entry_type="shell", command=["echo", "hi"], image="python:3.12")
    assert r.command == ["echo", "hi"] and r.image == "python:3.12"


def test_register_job_request_has_image_default():
    r = RegisterJobRequest(entrypoint="m.f", runner_mode="docker", image="python:3.12")
    assert r.image == "python:3.12"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/test_view_models.py -k shell -v`
Expected: FAIL — fields missing

- [ ] **Step 3: Write minimal implementation**

Add to `RunJobRequest`: `entry_type: EntryType = "module"`, `command: list[str] | None = None`, `command_env: dict[str, str] | None = None`, `image: str | None = None`. Add to `RegisterJobRequest`: `image: str | None = None` (and `command`/`command_env`/`entry_type` if registering shell defaults). Import `EntryType` from `aaiclick.orchestration.runner_config`. Wire the new fields through wherever these requests call into `run_job`/`register_job` (the internal_api adapter).

- [ ] **Step 4: Run test + view-model suite**

Run: `pytest aaiclick/test_view_models.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add aaiclick/view_models.py aaiclick/test_view_models.py
git commit -m "feat: request models accept entry_type/command/image"
```

### Task 14: CLI flags

**Files:**
- Modify: the CLI module that builds the `run`/`register` argparse commands (find with `grep -rln "runner_mode" aaiclick/ | grep -i cli`)
- Test: the CLI test file alongside it

- [ ] **Step 1: Write the failing test**

Add a parser test asserting `--entry-type shell --command 'python main.py' --image python:3.12` parses into the values forwarded to `run_job` (mirror the existing CLI arg tests; assert the parsed namespace / forwarded kwargs).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest <cli test path> -k entry_type -v`
Expected: FAIL — unknown argument `--entry-type`

- [ ] **Step 3: Write minimal implementation**

Add `--entry-type` (`choices=ENTRY_TYPES`), `--command` (repeatable or shell-split into argv), `--command-env` (`KEY=VALUE`, repeatable → dict), and `--image` to the `run` and `register` subcommands; forward them to `run_job`/`register_job`. Follow the existing flag/forwarding pattern in that module.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest <cli test path> -k entry_type -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add <cli module> <cli test>
git commit -m "feat: CLI --entry-type/--command/--command-env/--image"
```

---

## Phase 7 — Docs

### Task 15: Update `docs/orchestration.md` (prebuilt + shell + Execution layers)

**Files:**
- Modify: `docs/orchestration.md`

- [ ] **Step 1: Add the content**

Document: (a) the `prebuilt` image source (`--image`, no build task); (b) the `shell` `entry_type` (argv, `command_env`, exit-code result, no aaiclick required in the image); (c) a new **Execution layers** subsection — host worker / container command / task execution — stating that `python -m …docker_worker --task-id N` is the layer-2 bootstrap shim (runner plumbing), not task execution, and that `shell` bypasses both the shim and `execute_task`. Add `**Implementation**:` references to the new code (`runner_config.py`, `docker_config.effective_image_tag`, `factories.create_built_job`).

- [ ] **Step 2: Apply doc skills**

Use the `markdown-style` skill, then the `shortify` skill (per `CLAUDE.md`, these apply to subdirectory docs).

- [ ] **Step 3: Commit**

```bash
git add docs/orchestration.md
git commit -m "docs: document prebuilt images, shell tasks, execution layers"
```

### Task 16: Record out-of-scope items in `docs/future.md`

**Files:**
- Modify: `docs/future.md`

- [ ] **Step 1: Add entries**

Add: capturing shell stdout as a data result; shell tasks on the subprocess (host-local) runner; string-form (`sh -c`) commands; and the optional later split of the in-container module shim out of `docker_worker.py`.

- [ ] **Step 2: Commit**

```bash
git add docs/future.md
git commit -m "docs: record prebuilt/shell follow-ups in future.md"
```

---

## Phase 8 — Drop the dead flat columns

### Task 17: Finalize `Task.entry_type` not-null; remove flat Job/RegisteredJob columns

**Files:**
- Modify: `aaiclick/orchestration/models.py`
- Generate: drop migration (via `generate-migration` skill)
- Test: `aaiclick/orchestration/test_models_runner_columns.py`

- [ ] **Step 1: Write the failing test**

```python
def test_flat_runner_columns_removed():
    from aaiclick.orchestration.models import Job, RegisteredJob
    gone = {"git_remote", "git_sha", "git_branch", "dockerfile", "image_tag", "kubernetes_config"}
    assert gone.isdisjoint(Job.__table__.columns.keys())
    assert {"dockerfile", "git_remote", "kubernetes_config"}.isdisjoint(RegisteredJob.__table__.columns.keys())


def test_entry_type_not_null():
    assert Task.__table__.columns["entry_type"].nullable is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/test_models_runner_columns.py -k "removed or not_null" -v`
Expected: FAIL — columns still present / nullable

- [ ] **Step 3: Update models + generate migration**

Remove the flat `git_*`/`dockerfile`/`image_tag` columns from `Job`, the `kubernetes_config` column from `Job`, and `dockerfile`/`git_remote`/`kubernetes_config` from `RegisteredJob`. Change `Task.entry_type` to `nullable=False`. Then use the `generate-migration` skill; in the generated upgrade, **backfill** before the drop/alter:
- `UPDATE tasks SET entry_type = 'module' WHERE entry_type IS NULL;` then alter to NOT NULL.
- Backfill `jobs.runner` / `registered_jobs.runner` from the old flat columns for any rows that predate Phase 3 (compose the `{"type": runner_mode, "image": {...}}` JSON), then drop the flat columns.

(The agent must edit the autogenerated migration's data-migration step to add the backfill SQL — autogenerate emits schema ops only. This is the one allowed hand-edit: data backfill, not schema authoring.)

- [ ] **Step 4: Apply + verify**

Run: `python -m aaiclick migrate upgrade` then `pytest aaiclick/orchestration/ -q`
Expected: migration applies; suite PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/models.py alembic/versions/ aaiclick/orchestration/test_models_runner_columns.py
git commit -m "feat: drop flat runner columns; finalize entry_type not-null"
```

### Task 18: Full-suite green + distributed e2e note

- [ ] **Step 1: Run the whole suite**

Run: `pytest -q`
Expected: PASS. Fix any straggler reading a dropped column.

- [ ] **Step 2: Confirm distributed coverage**

The `shell`+`prebuilt` and `module`+`build` round-trips need real Docker/Postgres/CH and run in GitHub Actions / `test_e2e/docker/`. Add an e2e case under `test_e2e/docker/` running `python:3.12` with `command=["python","-c","print('ok')"]`, asserting success on exit 0 and failure on a non-zero command, and that `command_env` reaches the container while `AAICLICK_SQL_URL` does not. Then use the `check-pr` skill after pushing.

- [ ] **Step 3: Commit**

```bash
git add test_e2e/docker/
git commit -m "test: e2e shell job on prebuilt python:3.12 image"
```

---

## Self-Review Notes

- **Spec coverage:** entry_type/shell (Tasks 1,6,9,11,13,14); prebuilt/build image source (Tasks 1,5,7,10); runner unification + dropped columns (Tasks 3,4,7,17); conditional build injection (Task 7); shell env isolation (Tasks 9,11); exit-code result (Tasks 9,11); validation rules (Task 2,12); API/CLI (Tasks 12–14); execution-layers + orchestration.md (Task 15); future.md (Task 16); migration backfill/no-default (Tasks 4,17); tests incl. distributed e2e (Task 18).
- **Green-at-each-step:** additive columns (Phase 2) precede reader swaps (Phases 3–5); the drop migration is last (Phase 8). `create_task` keeps a `module` default so internal subprocess callers don't break; the "no implicit default" rule is enforced at the submission boundary.
- **Type consistency:** `RunnerConfig`/`ImageSource`/`EntryType`, `resolve_runner_config`, `effective_image_tag`, `create_built_job`, `validate_task_entry`, `parse_runner_config`/`dump_runner_config` are used with the same signatures across tasks.
