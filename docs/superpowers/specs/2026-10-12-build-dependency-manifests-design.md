Default Build Image — Dependency Manifests
---

A git build (`--build`) whose repo has no `Dockerfile` gets the default thin
layer over the aaiclick base image and installs nothing, so a job can only
import what `aaiclick[all]` ships. This design teaches that default to honour
dependency manifests found at the checkout root: Python manifests add a
`pip install` layer to the existing template; a `pom.xml` selects a second,
JVM default template. A checked-in `Dockerfile` still wins over both.

Current behaviour: `aaiclick/orchestration/execution/docker_build.py` — see
`_resolve_dockerfile()`; `aaiclick/orchestration/execution/docker_scaffold.py`
— see `DEFAULT_BUILD_DOCKERFILE_TEMPLATE`. User-facing description:
`docs/user_guide/container_images.md` "Runner base".

# Manifest detection

The worker classifies a checkout with no `Dockerfile` by the files at its root.
Subdirectory manifests are ignored. The build task runs with only the
`ImageBuild` source (git remote, SHA, optional `dockerfile=`), so the
template is chosen from the tree alone — no new field on `run_job`, the CLI,
or the API.

| Found at repo root                                   | Template | Install step                                                             |
|------------------------------------------------------|----------|--------------------------------------------------------------------------|
| none of the below                                    | python   | none — today's thin layer                                                |
| `uv.lock` (with `pyproject.toml`)                    | python   | `uv export --frozen --no-dev --no-emit-project --no-hashes` → `pip install -r` |
| `requirements.txt` (no `uv.lock`)                    | python   | `pip install -r requirements.txt`                                        |
| `pyproject.toml` with a `[project]` table (no lock, no `requirements.txt`) | python | `uv pip compile pyproject.toml` → `pip install -r`           |
| `pom.xml`, no Python manifest                        | jvm      | `mvn package` in a build stage, shim as `ENTRYPOINT`                     |
| `pom.xml` **and** a Python manifest                  | error    | build fails: ambiguous checkout, check in a `Dockerfile`                 |

Rules:

- Precedence among Python manifests: `uv.lock` > `requirements.txt` >
  `pyproject.toml`. Exactly one install step runs.
- A `pyproject.toml` without a `[project]` table (tool config only) adds
  nothing; the checkout stays on the thin layer. The worker checks table
  presence with `tomllib` and never parses dependencies itself.
- `dynamic = ["dependencies"]` is left to `uv`, which builds metadata from the
  copied manifest and fails the build with its own message if it cannot.
- Dependencies are installed; the project is never `pip install .`-ed.
  Entrypoints keep resolving from `WORKDIR /src` exactly as today.
- Every Python step resolves with `uv` (installed into the image with
  `pip install uv`) and installs with plain `pip install -r`, so the
  `PIP_INDEX_URL` / `PIP_EXTRA_INDEX_URL` / `PIP_TRUSTED_HOST` build args the
  worker already forwards apply to the user's packages too. `requirements.txt`
  skips the resolve step.

Verified with a probe (dynamic-version `pyproject.toml`, no `.git`):
`uv export --frozen` works from `pyproject.toml` + `uv.lock` alone, and
`uv pip compile pyproject.toml` resolves static dependencies without building
the project. Both therefore run on the manifests copied ahead of the tree.

# Python template

Assembled from parts by a function in `docker_scaffold.py` (one template per
variant would duplicate the shared head and tail):

```dockerfile
# Default image for a git build whose repo has no Dockerfile: a thin layer on
# the aaiclick base image. The build task forwards BASE_IMAGE pinned to the
# worker's aaiclick version (or AAICLICK_BASE_IMAGE).
ARG BASE_IMAGE=ghcr.io/kolodkin/aaiclick:latest
FROM ${BASE_IMAGE}
ARG PIP_INDEX_URL
ARG PIP_EXTRA_INDEX_URL
ARG PIP_TRUSTED_HOST

# --- only when a Python manifest is present ---
USER root
COPY pyproject.toml uv.lock /tmp/aaiclick-deps/        # or: requirements.txt
RUN pip install --no-cache-dir <index flags> uv \
 && uv export --frozen --no-dev --no-emit-project --no-hashes \
      --directory /tmp/aaiclick-deps -o /tmp/aaiclick-deps/requirements.txt \
 && pip install --no-cache-dir <index flags> -r /tmp/aaiclick-deps/requirements.txt
USER aaiclick
# --- end ---

COPY --chown=aaiclick:aaiclick . /src
WORKDIR /src
```

- `<index flags>` is the same shell fragment the starter `DOCKERFILE_TEMPLATE`
  uses: `--index-url "${PIP_INDEX_URL:-https://pypi.org/simple/}"`, the
  extra index and the trusted host when set.
- Manifests are copied before the tree so the dependency layer caches across
  commits that only change code.
- `USER root` around the install: the base image runs as `aaiclick` and the
  system site-packages are root-owned. Switching back keeps the runtime user.
- The `requirements.txt` variant copies only that file and runs
  `pip install -r` directly; the `pyproject.toml`-only variant copies that
  file and replaces `uv export …` with `uv pip compile pyproject.toml -o …`.

# JVM template

Selected when `pom.xml` is the only manifest. The user's `pom.xml` declares
`io.github.kolodkin:aaiclick-task-api` (Maven Central, lockstep with the
Python version — `docs/designs/java-sdk.md` "Publishing"), so the shim lands in
`libs/` and the `ENTRYPOINT` shape matches what that design prescribes for a
user image.

```dockerfile
# Default image for a git build whose repo has a pom.xml and no Dockerfile.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY . /build
RUN mvn -B -q -DskipTests package dependency:copy-dependencies \
      -DincludeScope=runtime -DoutputDirectory=target/libs

FROM eclipse-temurin:21-jre
COPY --from=build /build/target/*.jar /app/
COPY --from=build /build/target/libs /app/libs
ENTRYPOINT ["java", "-cp", "/app/*:/app/libs/*", "io.github.kolodkin.aaiclick.task.AaiTaskShim"]
```

Limits, documented rather than handled: single-module poms only, JDK/JRE 21
fixed, no fat-jar plugins. Anything else is a checked-in `Dockerfile`. No
`BASE_IMAGE` or pip build args apply; the git labels are not emitted (the
Python default does not emit them either).

# Resolution flow

`_resolve_dockerfile()` keeps its contract — return the path to build with,
inside the checkout — and gains the classification step:

1. `Dockerfile` (or the explicit `dockerfile=`) exists → return it. A missing
   explicit path still raises `FileNotFoundError`.
2. Classify the root per the table. `pom.xml` alongside a Python manifest
   raises `ValueError` naming both files and asking for a `Dockerfile`.
3. Write the rendered template to `Dockerfile` and `.git` to `.dockerignore`
   when the repo has none (unchanged).

Classification and rendering live in `docker_scaffold.py` as pure functions of
a directory and a manifest kind, so unit tests cover them without docker:

- `detect_manifest(context_dir: Path) -> ManifestKind` where `ManifestKind`
  is a `Literal["none", "uv_lock", "requirements", "pyproject", "pom"]` with
  module-level constants, per the project's `Literal` convention.
- `render_default_dockerfile(kind: ManifestKind) -> str`.

`DEFAULT_BUILD_DOCKERFILE_TEMPLATE` becomes the `"none"` rendering; the
existing tests that compare against it keep passing.

# Testing

- Unit (`aaiclick/orchestration/execution/test_docker_build.py`, docker
  stubbed as today): one parametrized test over fixture trees → expected
  `Dockerfile` text and generated files, covering every row of the table and
  the precedence between Python manifests. Error cases: `pom.xml` plus a
  Python manifest; `pyproject.toml` without `[project]` renders the thin
  layer.
- E2E docker (`test_e2e/docker/`): a second bare fixture whose
  `requirements.txt` names a package the base image lacks and whose task
  imports it, published into the CI git daemon like `bare_job`. One run on
  the docker runner proves the install layer end to end.
- No JVM default-template e2e: it would need the SDK on Maven Central at the
  version under test. The `jvm_task` fixture keeps its checked-in `Dockerfile`;
  the JVM template is covered by the unit rendering test only.

# Documentation

- `docs/user_guide/container_images.md` "Runner base": add the manifest table
  (the user-facing columns: found at repo root, template, install step) and
  the limits of each template.
- `docs/user_guide/sandbox.md` "Limits": drop the "ships only `aaiclick[all]`"
  line; a sandbox repo with a `requirements.txt` now installs it.
- `docs/designs/future.md`: remove the item.
- `docs/designs/orchestration.md` "Image source": one sentence pointing at the
  user guide table.
