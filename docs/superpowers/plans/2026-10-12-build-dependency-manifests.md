# Default Build Image — Dependency Manifests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A git build whose repo has no `Dockerfile` installs the dependencies its root manifest declares, and a `pom.xml`-only repo builds a JVM image with the SDK shim as entrypoint.

**Architecture:** `docker_scaffold.py` gains two pure functions — `detect_manifest(context_dir)` classifies the checkout root into a `ManifestKind`, `render_default_dockerfile(kind)` assembles the Dockerfile text from shared head/tail parts plus one install block. `docker_build._resolve_dockerfile()` calls both where it writes the default today. Nothing changes on the `ImageBuild` source, CLI, or API.

**Tech Stack:** Python, docker CLI (stubbed in unit tests), `uv` + `pip` inside the built image, Maven + Temurin 21 images for the JVM template.

**Spec:** `docs/superpowers/specs/2026-10-12-build-dependency-manifests-design.md`

## Global Constraints

- `requires-python = ">=3.10"`: no `tomllib` (3.11+). The `[project]` table check is a line-anchored regex, not a TOML parse.
- Imports at top of file only; `Literal` + module constants for `ManifestKind`, never an Enum.
- Markdown edits in `docs/` follow the `markdown-style` skill (setext title, `#`/`##` only, padded tables); run the `shortify` skill after.
- Every pip call in a rendered template carries the same index flags the starter `DOCKERFILE_TEMPLATE` uses: `--index-url "${PIP_INDEX_URL:-https://pypi.org/simple/}" --extra-index-url https://pypi.org/simple/ ${PIP_TRUSTED_HOST:+--trusted-host ${PIP_TRUSTED_HOST}}`.
- `DEFAULT_BUILD_DOCKERFILE_TEMPLATE` stays exported and byte-identical to `render_default_dockerfile(MANIFEST_NONE)`.

## Review Focus

- A `requirements.txt` that is a directory, or an empty file: detection keys on `is_file()`; an empty file renders the install block and `pip install -r` of an empty file succeeds. Pinned in Task 1.
- `pyproject.toml` with `[project]` inside a multi-line string or as `[project.scripts]` only: the regex matches `^\[project\]\s*$` exactly, so `[project.scripts]` alone does not count. Pinned in Task 1.
- `pom.xml` plus a `pyproject.toml` that has no `[project]` table (tool config only): not a Python manifest, so JVM wins and no error. Pinned in Task 1.
- An explicit `dockerfile=` path that is missing must still raise before any classification runs. Existing test, re-run in Task 3.
- A checked-in `.dockerignore` that excludes `requirements.txt` makes the `COPY` fail inside docker; out of scope for code, named in the user-guide limits (Task 5).

---

### Task 1: Manifest detection

**Files:**
- Modify: `aaiclick/orchestration/execution/docker_scaffold.py`
- Test: `aaiclick/orchestration/execution/test_docker_scaffold.py`

**Interfaces:**
- Produces:
  ```python
  MANIFEST_NONE = "none"
  MANIFEST_UV_LOCK = "uv_lock"
  MANIFEST_REQUIREMENTS = "requirements"
  MANIFEST_PYPROJECT = "pyproject"
  MANIFEST_POM = "pom"
  ManifestKind = Literal["none", "uv_lock", "requirements", "pyproject", "pom"]

  class AmbiguousManifests(ValueError): ...

  def detect_manifest(context_dir: Path) -> ManifestKind: ...
  ```
  `AmbiguousManifests` message names the files: `"pom.xml alongside <python manifest> in the checkout: check in a Dockerfile to choose the image"`.

- [ ] **Step 1: Write the failing tests**

```python
@pytest.mark.parametrize(
    "files, expected",
    [
        pytest.param({}, MANIFEST_NONE, id="empty"),
        pytest.param({"job.py": ""}, MANIFEST_NONE, id="code-only"),
        pytest.param({"requirements.txt": ""}, MANIFEST_REQUIREMENTS, id="requirements-empty-file"),
        pytest.param({"requirements.txt": "six\n"}, MANIFEST_REQUIREMENTS, id="requirements"),
        pytest.param({"pyproject.toml": "[project]\nname = 'x'\n", "uv.lock": ""}, MANIFEST_UV_LOCK, id="uv-lock"),
        pytest.param({"pyproject.toml": "[project]\nname = 'x'\n", "uv.lock": "", "requirements.txt": ""}, MANIFEST_UV_LOCK, id="uv-lock-beats-requirements"),
        pytest.param({"pyproject.toml": "[project]\nname = 'x'\n", "requirements.txt": ""}, MANIFEST_REQUIREMENTS, id="requirements-beats-pyproject"),
        pytest.param({"pyproject.toml": "[project]\nname = 'x'\n"}, MANIFEST_PYPROJECT, id="pyproject"),
        pytest.param({"pyproject.toml": "[tool.ruff]\nline-length = 120\n"}, MANIFEST_NONE, id="pyproject-tool-config-only"),
        pytest.param({"pyproject.toml": "[project.scripts]\nx = 'x:main'\n"}, MANIFEST_NONE, id="pyproject-subtable-only"),
        pytest.param({"pom.xml": "<project/>"}, MANIFEST_POM, id="pom"),
        pytest.param({"pom.xml": "<project/>", "pyproject.toml": "[tool.ruff]\n"}, MANIFEST_POM, id="pom-with-tool-config-pyproject"),
        pytest.param({"sub/requirements.txt": ""}, MANIFEST_NONE, id="subdirectory-ignored"),
    ],
)
def test_detect_manifest(tmp_path, files, expected):
    for name, content in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    assert detect_manifest(tmp_path) == expected


def test_detect_manifest_requirements_directory_is_not_a_manifest(tmp_path):
    (tmp_path / "requirements.txt").mkdir()
    assert detect_manifest(tmp_path) == MANIFEST_NONE


@pytest.mark.parametrize("python_manifest", ["requirements.txt", "uv.lock", "pyproject.toml"])
def test_detect_manifest_pom_with_python_manifest_is_ambiguous(tmp_path, python_manifest):
    (tmp_path / "pom.xml").write_text("<project/>")
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'x'\n")
    (tmp_path / python_manifest).write_text("" if python_manifest != "pyproject.toml" else "[project]\nname = 'x'\n")
    with pytest.raises(AmbiguousManifests, match="pom.xml alongside"):
        detect_manifest(tmp_path)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest aaiclick/orchestration/execution/test_docker_scaffold.py -k detect_manifest -v`
Expected: FAIL with `ImportError` on `detect_manifest`

- [ ] **Step 3: Implement `detect_manifest(context_dir: Path) -> ManifestKind` in `docker_scaffold.py`**

Python manifest = `uv.lock`, else `requirements.txt`, else `pyproject.toml` whose text matches `re.search(r"^\[project\]\s*$", text, re.MULTILINE)`; each must be `is_file()`. If `pom.xml` is a file: raise `AmbiguousManifests` when a Python manifest was found, else return `MANIFEST_POM`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest aaiclick/orchestration/execution/test_docker_scaffold.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/execution/docker_scaffold.py aaiclick/orchestration/execution/test_docker_scaffold.py
git commit -m "Detect dependency manifests at a build checkout root"
```

---

### Task 2: Template rendering

**Files:**
- Modify: `aaiclick/orchestration/execution/docker_scaffold.py`
- Test: `aaiclick/orchestration/execution/test_docker_scaffold.py`

**Interfaces:**
- Consumes: `ManifestKind` constants from Task 1.
- Produces: `def render_default_dockerfile(kind: ManifestKind) -> str`, and `DEFAULT_BUILD_DOCKERFILE_TEMPLATE = render_default_dockerfile(MANIFEST_NONE)` (module constant, computed once at import — allowed: it never changes).

- [ ] **Step 1: Write the failing tests**

```python
def test_render_default_dockerfile_none_is_the_thin_layer():
    assert render_default_dockerfile(MANIFEST_NONE) == DEFAULT_BUILD_DOCKERFILE_TEMPLATE
    assert "USER root" not in DEFAULT_BUILD_DOCKERFILE_TEMPLATE
    assert DEFAULT_BUILD_DOCKERFILE_TEMPLATE.endswith("COPY --chown=aaiclick:aaiclick . /src\nWORKDIR /src\n")


@pytest.mark.parametrize(
    "kind, copy_line, resolve_fragment",
    [
        pytest.param(MANIFEST_REQUIREMENTS, "COPY requirements.txt /tmp/aaiclick-deps/", "-r /tmp/aaiclick-deps/requirements.txt", id="requirements"),
        pytest.param(MANIFEST_UV_LOCK, "COPY pyproject.toml uv.lock /tmp/aaiclick-deps/", "uv export --frozen --no-dev --no-emit-project --no-hashes", id="uv-lock"),
        pytest.param(MANIFEST_PYPROJECT, "COPY pyproject.toml /tmp/aaiclick-deps/", "uv pip compile pyproject.toml", id="pyproject"),
    ],
)
def test_render_default_dockerfile_python_install_block(kind, copy_line, resolve_fragment):
    text = render_default_dockerfile(kind)
    assert text.startswith(DEFAULT_BUILD_DOCKERFILE_TEMPLATE.split("USER", 1)[0].rsplit("COPY --chown", 1)[0])
    assert copy_line in text
    assert resolve_fragment in text
    # Install runs as root, the runtime user is restored before the tree is copied.
    assert text.index("USER root") < text.index(copy_line) < text.index("USER aaiclick") < text.index("COPY --chown=aaiclick:aaiclick . /src")
    assert text.count('--index-url "${PIP_INDEX_URL:-https://pypi.org/simple/}"') == text.count("pip install")
    assert text.endswith("COPY --chown=aaiclick:aaiclick . /src\nWORKDIR /src\n")


def test_render_default_dockerfile_requirements_skips_uv():
    assert "uv" not in render_default_dockerfile(MANIFEST_REQUIREMENTS).replace("uvicorn", "")


def test_render_default_dockerfile_jvm():
    text = render_default_dockerfile(MANIFEST_POM)
    assert text.startswith("# Default image for a git build whose repo has a pom.xml and no Dockerfile.\nFROM maven:3.9-eclipse-temurin-21 AS build\n")
    assert "mvn -B -q -DskipTests package dependency:copy-dependencies" in text
    assert "FROM eclipse-temurin:21-jre\n" in text
    assert text.endswith('ENTRYPOINT ["java", "-cp", "/app/*:/app/libs/*", "io.github.kolodkin.aaiclick.task.AaiTaskShim"]\n')
    assert "BASE_IMAGE" not in text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest aaiclick/orchestration/execution/test_docker_scaffold.py -k render_default -v`
Expected: FAIL with `ImportError` on `render_default_dockerfile`

- [ ] **Step 3: Implement `render_default_dockerfile(kind: ManifestKind) -> str` in `docker_scaffold.py`**

Replace the `DEFAULT_BUILD_DOCKERFILE_TEMPLATE` string literal with parts. The exact text the spec fixes:

Python head (unchanged comment, then):
```dockerfile
ARG BASE_IMAGE=ghcr.io/kolodkin/aaiclick:latest
FROM ${BASE_IMAGE}
ARG PIP_INDEX_URL
ARG PIP_EXTRA_INDEX_URL
ARG PIP_TRUSTED_HOST
```
Python tail: `COPY --chown=aaiclick:aaiclick . /src\nWORKDIR /src\n`.

Install block per kind, between head and tail (`_PIP_FLAGS` is the Global Constraints fragment, one shell line continuation per flag as in `DOCKERFILE_TEMPLATE`):

- `requirements`: `USER root` / `COPY requirements.txt /tmp/aaiclick-deps/` / `RUN pip install --no-cache-dir <flags> -r /tmp/aaiclick-deps/requirements.txt` / `USER aaiclick`
- `uv_lock`: `USER root` / `COPY pyproject.toml uv.lock /tmp/aaiclick-deps/` / `RUN pip install --no-cache-dir <flags> uv && uv export --frozen --no-dev --no-emit-project --no-hashes --directory /tmp/aaiclick-deps -o /tmp/aaiclick-deps/requirements.txt && pip install --no-cache-dir <flags> -r /tmp/aaiclick-deps/requirements.txt` / `USER aaiclick`
- `pyproject`: as `uv_lock` but `COPY pyproject.toml /tmp/aaiclick-deps/` and `uv pip compile --directory /tmp/aaiclick-deps pyproject.toml -o /tmp/aaiclick-deps/requirements.txt`
- `none`: no block.

`pom` returns the JVM template verbatim from the spec "JVM template" section. The head comment for Python variants stays the current three-line comment.

- [ ] **Step 4: Run the scaffold and build tests**

Run: `pytest aaiclick/orchestration/execution/test_docker_scaffold.py aaiclick/orchestration/execution/test_docker_build.py -v`
Expected: all PASS (the build tests still import `DEFAULT_BUILD_DOCKERFILE_TEMPLATE`)

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/execution/docker_scaffold.py aaiclick/orchestration/execution/test_docker_scaffold.py
git commit -m "Render the default build Dockerfile per manifest kind"
```

---

### Task 3: Wire detection into the build

**Files:**
- Modify: `aaiclick/orchestration/execution/docker_build.py` — `_resolve_dockerfile()`
- Test: `aaiclick/orchestration/execution/test_docker_build.py`

**Interfaces:**
- Consumes: `detect_manifest`, `render_default_dockerfile`, `AmbiguousManifests`, `MANIFEST_*` from Tasks 1–2.

- [ ] **Step 1: Write the failing tests** (next to `test_build_image_to_tag_repo_without_dockerfile_builds_with_default`)

```python
@pytest.mark.parametrize(
    "clone_files, kind",
    [
        pytest.param({"requirements.txt": "six\n"}, MANIFEST_REQUIREMENTS, id="requirements"),
        pytest.param({"pyproject.toml": "[project]\nname = 'x'\n", "uv.lock": "version = 1\n"}, MANIFEST_UV_LOCK, id="uv-lock"),
        pytest.param({"pyproject.toml": "[project]\nname = 'x'\n"}, MANIFEST_PYPROJECT, id="pyproject"),
        pytest.param({"pom.xml": "<project/>"}, MANIFEST_POM, id="pom"),
    ],
)
async def test_build_image_to_tag_default_honours_manifest(monkeypatch, clone_files, kind):
    built = _stub_build_path(monkeypatch, clone_files)
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40)

    await docker_build.build_image_to_tag(source, docker_config.compute_image_tag("a" * 40))

    assert built[0]["Dockerfile"] == render_default_dockerfile(kind)
    assert built[0][".dockerignore"] == ".git\n"


async def test_build_image_to_tag_ambiguous_manifests_raise(monkeypatch):
    _stub_build_path(monkeypatch, {"pom.xml": "<project/>", "requirements.txt": ""})
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40)

    with pytest.raises(AmbiguousManifests, match="check in a Dockerfile"):
        await docker_build.build_image_to_tag(source, docker_config.compute_image_tag("a" * 40))


async def test_build_image_to_tag_checked_in_dockerfile_wins_over_manifest(monkeypatch):
    built = _stub_build_path(monkeypatch, {"Dockerfile": "FROM python:3.12\n", "requirements.txt": "six\n"})
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40)

    await docker_build.build_image_to_tag(source, docker_config.compute_image_tag("a" * 40))

    assert built[0]["Dockerfile"] == "FROM python:3.12\n"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest aaiclick/orchestration/execution/test_docker_build.py -k "manifest" -v`
Expected: `default_honours_manifest[requirements|uv-lock|pyproject|pom]` FAIL on Dockerfile text; `ambiguous` FAIL (no raise)

- [ ] **Step 3: Update `_resolve_dockerfile()`**

After the explicit-path check, replace `dockerfile.write_text(DEFAULT_BUILD_DOCKERFILE_TEMPLATE)` with `dockerfile.write_text(render_default_dockerfile(detect_manifest(context_dir)))`. Drop the now-unused `DEFAULT_BUILD_DOCKERFILE_TEMPLATE` import from `docker_build.py`; extend the docstring with one sentence on manifest selection. `AmbiguousManifests` propagates: the build task fails with its message.

- [ ] **Step 4: Run the execution package tests and lint**

Run: `pytest aaiclick/orchestration/execution -q && ruff check aaiclick && ruff format --check aaiclick`
Expected: all PASS, no lint findings

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/execution/docker_build.py aaiclick/orchestration/execution/test_docker_build.py
git commit -m "Build the default image from the checkout's dependency manifest"
```

---

### Task 4: Docker e2e for the requirements layer

**Files:**
- Create: `test_e2e/fixtures/deps_job/deps_jobs.py`, `test_e2e/fixtures/deps_job/requirements.txt`
- Modify: `test_e2e/docker/conftest.py` (fixture `docker_e2e_deps_repo`), `test_e2e/docker/test_docker_runner_e2e.py`

**Interfaces:**
- Consumes: `publish_user_repo`, `run_build_job` (existing).

- [ ] **Step 1: Add the fixture repo**

`requirements.txt`: `tabulate==0.9.0` (not in `aaiclick[all]`; verify with `pip show tabulate` in a fresh `aaiclick[all]` env or by grepping `pyproject.toml`).

`deps_jobs.py`, mirroring `bare_jobs.py`:
```python
@task
async def entry_task() -> dict:
    """Prove the default image installed ``requirements.txt``."""
    return {"tabulate": importlib.metadata.version("tabulate")}
```

- [ ] **Step 2: Add the session fixture and the test**

In `conftest.py`: `_DEPS_JOB = _FIXTURES / "deps_job"` and `docker_e2e_deps_repo` publishing it, same shape as `docker_e2e_bare_repo`.

```python
@pytest.mark.docker_e2e
async def test_docker_runner_default_dockerfile_installs_requirements(orch_ctx, docker_e2e_deps_repo):
    """A user repo with ``requirements.txt`` and no Dockerfile gets it installed into the default image."""
    tasks = await run_build_job("docker_e2e_default_requirements", "deps_jobs.entry_task", docker_e2e_deps_repo)

    entry = next(t for t in tasks if t.entrypoint == "deps_jobs.entry_task")
    assert entry.result == {"native_value": {"tabulate": "0.9.0"}}, (entry.result, entry.error)
```

- [ ] **Step 3: Run what runs locally**

Run: `pytest test_e2e/docker/test_docker_runner_e2e.py -k requirements -v`
Expected: SKIPPED without a docker daemon (collection hook), or PASS with one. The real check is the `docker-e2e` workflow after push (`check-pr` skill).

- [ ] **Step 4: Commit**

```bash
git add test_e2e/fixtures/deps_job test_e2e/docker/conftest.py test_e2e/docker/test_docker_runner_e2e.py
git commit -m "E2E: default build image installs requirements.txt"
```

---

### Task 5: Documentation

**Files:**
- Modify: `docs/user_guide/container_images.md` ("Runner base"), `docs/user_guide/sandbox.md` ("Limits"), `docs/designs/orchestration.md` ("Image source"), `docs/designs/future.md`
- Delete: `docs/superpowers/specs/2026-10-12-build-dependency-manifests-design.md`, `docs/superpowers/plans/2026-10-12-build-dependency-manifests.md`

- [ ] **Step 1: `container_images.md` "Runner base"**

After the existing default-layer paragraph, add the manifest table (columns: Found at repo root, Template, Install step) from the spec, then one short paragraph of limits: precedence `uv.lock` > `requirements.txt` > `pyproject.toml`; `pyproject.toml` needs a `[project]` table; the project is never `pip install .`-ed; JVM default is single-module, JDK/JRE 21, `aaiclick-task-api` declared in the pom; a `.dockerignore` that excludes the manifest breaks the `COPY`; anything else is a checked-in `Dockerfile`. End with the implementation reference: `aaiclick/orchestration/execution/docker_scaffold.py` — see `detect_manifest()`, `render_default_dockerfile()`.

- [ ] **Step 2: `sandbox.md` "Limits"**

Replace the "ships only `aaiclick[all]`" bullet with: a `requirements.txt` (or `pyproject.toml` / `uv.lock`) checked into the sandbox repo is installed into the default image — see Container Images "Runner base".

- [ ] **Step 3: `orchestration.md` "Image source"**

One sentence after the source table: a `build` without a `Dockerfile` renders a default one from the checkout's manifests (`docs/user_guide/container_images.md` "Runner base").

- [ ] **Step 4: `future.md`**

Remove the "Default Build Image — Dependency Manifests" section and its `---` separator.

- [ ] **Step 5: Remove the spec and plan, run doc skills**

`git rm` both superpowers files. Run the `shortify` skill on the edited docs. `grep -rn "build-dependency-manifests" docs` must return nothing.

- [ ] **Step 6: Commit and push, then `check-pr`**

```bash
git add -A docs
git commit -m "Document dependency manifests for the default build image"
git push -u origin ccr-c9da9163-3q9q2b
```
Then use the `check-pr` skill; fix any failing workflow.
