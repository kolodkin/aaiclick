"""Dockerfile templates for docker-runner jobs.

- ``DOCKERFILE_TEMPLATE`` — the starter ``python -m aaiclick docker init``
  scaffolds into the user's working directory; from there the user owns it.
- ``render_default_dockerfile(detect_manifest(checkout))`` — what a ``build``
  source writes into a checkout that has no ``Dockerfile``: a thin layer on
  the aaiclick base image plus the install step its root manifest calls for
  (``docker_build._resolve_dockerfile``; user guide:
  docs/user_guide/container_images.md "Runner base"). Check in a Dockerfile
  to customize."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

from ...ghcr import BASE_IMAGE_REPO

DOCKERFILE_TEMPLATE = """\
# Starter Dockerfile for an aaiclick docker-runner job.
#
# Customize freely — base image, install method, dependencies, USER, etc.
# The framework only requires that:
#   - `python` is on PATH and `aaiclick` is importable
#   - the entrypoint module(s) are importable
#   - the build-args below (if you use them) come through as ARG declarations
#
# Build-args the framework forwards (optional — declare only what you use):
#   GIT_REMOTE, GIT_SHA, GIT_BRANCH
#   PIP_INDEX_URL, PIP_EXTRA_INDEX_URL  (e.g. corporate / test pypi)
#   PIP_TRUSTED_HOST                    (allow plain HTTP for the index host)
#   AAICLICK_VERSION                    (matches the host's installed version)
#   BASE_IMAGE                          (ghcr.io/kolodkin/aaiclick at that version,
#                                        or AAICLICK_BASE_IMAGE on the worker)

FROM python:3.10-slim

ARG GIT_REMOTE
ARG GIT_SHA
ARG GIT_BRANCH
ARG PIP_INDEX_URL
ARG PIP_TRUSTED_HOST
ARG AAICLICK_VERSION

# Image metadata (visible via `docker inspect <image>`).
LABEL org.opencontainers.image.source="${GIT_REMOTE}"
LABEL org.opencontainers.image.revision="${GIT_SHA}"
LABEL org.opencontainers.image.ref.name="${GIT_BRANCH}"

# Runtime env (visible to task code via os.environ).
ENV GIT_REMOTE=${GIT_REMOTE} \\
    GIT_SHA=${GIT_SHA} \\
    GIT_BRANCH=${GIT_BRANCH}

# Install aaiclick. Pinned to the host's version so the host worker and the
# container speak the same IPC protocol.
RUN pip install --no-cache-dir \\
    --index-url "${PIP_INDEX_URL:-https://pypi.org/simple/}" \\
    --extra-index-url https://pypi.org/simple/ \\
    ${PIP_TRUSTED_HOST:+--trusted-host ${PIP_TRUSTED_HOST}} \\
    "aaiclick[distributed]==${AAICLICK_VERSION}"

# Install the user's repo as a package so `importlib` can resolve task
# entrypoints. Replace this with the install method that suits your project
# (uv pip, poetry install --only main, etc.). WORKDIR is the project root so
# the container resolves entrypoints from the workdir — the same way the host
# CLI does when run from the repo.
COPY . /src
WORKDIR /src
RUN pip install --no-cache-dir /src
"""

MANIFEST_NONE = "none"
MANIFEST_UV_LOCK = "uv_lock"
MANIFEST_REQUIREMENTS = "requirements"
MANIFEST_PYPROJECT = "pyproject"
MANIFEST_POM = "pom"
ManifestKind = Literal["none", "uv_lock", "requirements", "pyproject", "pom"]

# A ``[project]`` table header line (comment allowed); ``[project.scripts]``
# alone does not make a tool-config-only pyproject a dependency manifest.
_PROJECT_TABLE_RE = re.compile(r"^\s*\[\s*project\s*\]\s*(#.*)?$", re.MULTILINE)


_MANIFEST_FILES: dict[ManifestKind, str] = {
    MANIFEST_UV_LOCK: "uv.lock",
    MANIFEST_REQUIREMENTS: "requirements.txt",
    MANIFEST_PYPROJECT: "pyproject.toml",
}


def _python_manifest(context_dir: Path) -> ManifestKind:
    """The Python manifest at the root, by precedence; ``MANIFEST_NONE`` when there is none."""
    pyproject = context_dir / "pyproject.toml"
    if (context_dir / "uv.lock").is_file() and pyproject.is_file():
        return MANIFEST_UV_LOCK
    if (context_dir / "requirements.txt").is_file():
        return MANIFEST_REQUIREMENTS
    if pyproject.is_file() and _PROJECT_TABLE_RE.search(pyproject.read_text()):
        return MANIFEST_PYPROJECT
    return MANIFEST_NONE


def detect_manifest(context_dir: Path) -> ManifestKind:
    """Classify a checkout root by the dependency manifest it carries.

    Python manifests take precedence ``uv.lock`` (with its ``pyproject.toml``)
    > ``requirements.txt`` > ``pyproject.toml`` (only with a ``[project]`` table). A
    ``pom.xml`` selects the JVM template; one next to a Python manifest is
    ambiguous and raises ``ValueError``. Subdirectories are ignored."""
    python = _python_manifest(context_dir)
    if (context_dir / "pom.xml").is_file():
        if python != MANIFEST_NONE:
            raise ValueError(
                f"pom.xml alongside {_MANIFEST_FILES[python]} at the checkout root: check in a Dockerfile to choose the image"
            )
        return MANIFEST_POM
    return python


_PYTHON_HEAD = f"""\
# Default image for a git build whose repo has no Dockerfile: a thin layer on
# the aaiclick base image. The build task forwards BASE_IMAGE pinned to the
# worker's aaiclick version (or AAICLICK_BASE_IMAGE).
ARG BASE_IMAGE={BASE_IMAGE_REPO}:latest
FROM ${{BASE_IMAGE}}
"""

# --chown: the base image runs as USER aaiclick; a root-owned /src would break
# tasks that write relative paths.
_PYTHON_TAIL = """\
COPY --chown=aaiclick:aaiclick . /src
WORKDIR /src
"""

_PYPI = "https://pypi.org/simple/"
_CTX = "/ctx"
_UV_DIR = "/tmp/uv"
_UV = f"{_UV_DIR}/bin/uv"
_BASE = "/tmp/base.txt"
_EXPORTED = "/tmp/exported.txt"
# Pinned so a build is reproducible; bump deliberately.
_UV_PIN = "uv==0.12.7"

# The worker forwards PIP_INDEX_URL / PIP_EXTRA_INDEX_URL / PIP_TRUSTED_HOST
# as build args; pip and uv each get them in their own spelling. uv ranks
# ``--index`` above ``--default-index`` and stops at the first index that
# has a name, so it also gets pip's merge-all strategy.
_PIP_INDEX = (
    f'--index-url "${{PIP_INDEX_URL:-{_PYPI}}}" '
    '${PIP_EXTRA_INDEX_URL:+--extra-index-url "${PIP_EXTRA_INDEX_URL}"} '
    '${PIP_TRUSTED_HOST:+--trusted-host "${PIP_TRUSTED_HOST}"}'
)
_UV_INDEX = (
    f'--index-strategy unsafe-best-match --default-index "${{PIP_INDEX_URL:-{_PYPI}}}" '
    '${PIP_EXTRA_INDEX_URL:+--index "${PIP_EXTRA_INDEX_URL}"} '
    '${PIP_TRUSTED_HOST:+--allow-insecure-host "${PIP_TRUSTED_HOST}"}'
)

# A ``uv.lock`` is honoured as is; the project and workspace members are not
# installed editable, since the mounted checkout is gone at runtime.
_UV_EXPORT = f"{_UV} export --frozen --no-dev --no-hashes --no-emit-project --no-editable -o {_EXPORTED}"


def _python_dockerfile(requirements: str, *, export: bool = False) -> str:
    """Thin layer plus one install layer for ``requirements`` (a path in the
    bind-mounted checkout, or the ``uv.lock`` export when ``export`` is set).

    The checkout is mounted, not copied, and is the cwd, so ``-r`` / ``-c``
    includes and path dependencies resolve and no manifest lands in a layer;
    the cache mount keeps downloads across builds on the worker. ``base.txt``
    — what the base image already has, aaiclick included — overrides every
    requirement, so the base image's packages win: a user lock can neither
    downgrade aaiclick nor swap the dependencies it was tested with, and a
    manifest naming aaiclick resolves to the installed copy without an index
    lookup. uv lives in its own tree outside site-packages and is deleted
    after use, so it is neither in ``base.txt`` nor in the image."""
    steps = [
        f"cd {_CTX}",
        f"pip install --target {_UV_DIR} {_PIP_INDEX} {_UV_PIN}",
        f"pip list --format=freeze > {_BASE}",
        *([_UV_EXPORT] if export else []),
        f"{_UV} pip install --system {_UV_INDEX} --overrides {_BASE} -r {requirements}",
        f"rm -rf {_UV_DIR} {_BASE} {_EXPORTED}",
    ]
    run = " \\\n && ".join(steps)
    install = f"""\
ARG PIP_INDEX_URL
ARG PIP_EXTRA_INDEX_URL
ARG PIP_TRUSTED_HOST
USER root
RUN --mount=type=bind,target={_CTX} --mount=type=cache,target=/root/.cache \\
    {run}
USER aaiclick
"""
    return _PYTHON_HEAD + install + _PYTHON_TAIL


# The shape docs/designs/java-sdk.md prescribes for a user image: the SDK shim
# (declared in the user's pom as aaiclick-task-api) as ENTRYPOINT, so the
# runner's ``--task-id N --run-epoch M`` arguments reach it. The BuildKit
# cache mount keeps ~/.m2 across builds without landing in a layer.
_JVM_TEMPLATE = """\
# Default image for a git build whose repo has a pom.xml and no Dockerfile.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY . /build
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -Dmaven.test.skip=true package dependency:copy-dependencies \\
      -DincludeScope=runtime -DoutputDirectory=target/libs

FROM eclipse-temurin:21-jre
COPY --from=build /build/target/*.jar /app/
COPY --from=build /build/target/libs /app/libs
ENTRYPOINT ["java", "-cp", "/app/*:/app/libs/*", "io.github.kolodkin.aaiclick.task.AaiTaskShim"]
"""

_DEFAULT_DOCKERFILES: dict[ManifestKind, str] = {
    MANIFEST_NONE: _PYTHON_HEAD + _PYTHON_TAIL,
    MANIFEST_REQUIREMENTS: _python_dockerfile("requirements.txt"),
    MANIFEST_UV_LOCK: _python_dockerfile(_EXPORTED, export=True),
    MANIFEST_PYPROJECT: _python_dockerfile("pyproject.toml"),
    MANIFEST_POM: _JVM_TEMPLATE,
}


def render_default_dockerfile(kind: ManifestKind) -> str:
    """The default Dockerfile for a checkout without one, per its manifest kind."""
    return _DEFAULT_DOCKERFILES[kind]


class DockerfileExists(FileExistsError):
    """Raised when the target path already exists and ``force`` is False."""


def init_dockerfile(target: Path, *, force: bool = False) -> Path:
    """Write the starter Dockerfile to ``target``.

    Returns the resolved target path. Raises :class:`DockerfileExists`
    when the file already exists and ``force`` is False — silent
    overwrite would clobber whatever Dockerfile the user already had."""
    if target.exists() and not force:
        raise DockerfileExists(f"{target} already exists. Pass --force to overwrite.")
    target.write_text(DOCKERFILE_TEMPLATE)
    return target.resolve()
