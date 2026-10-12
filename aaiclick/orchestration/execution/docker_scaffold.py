"""Dockerfile templates for docker-runner jobs.

- ``DOCKERFILE_TEMPLATE`` — the starter ``python -m aaiclick docker init``
  scaffolds into the user's working directory; from there the user owns it.
- ``DEFAULT_BUILD_DOCKERFILE_TEMPLATE`` — the thin layer on the aaiclick base
  image a ``build`` source writes into a checkout that has no ``Dockerfile``
  (``docker_build.build_image_to_tag``; spec: docs/designs/orchestration.md
  "Image source"). Check in a Dockerfile to customize."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

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

DEFAULT_BUILD_DOCKERFILE_TEMPLATE = """\
# Default image for a git build whose repo has no Dockerfile: a thin layer on
# the aaiclick base image. The build task forwards BASE_IMAGE pinned to the
# worker's aaiclick version (or AAICLICK_BASE_IMAGE).
# --chown: the base image runs as USER aaiclick; a root-owned /src would break
# tasks that write relative paths.
ARG BASE_IMAGE=ghcr.io/kolodkin/aaiclick:latest
FROM ${BASE_IMAGE}
COPY --chown=aaiclick:aaiclick . /src
WORKDIR /src
"""


MANIFEST_NONE = "none"
MANIFEST_UV_LOCK = "uv_lock"
MANIFEST_REQUIREMENTS = "requirements"
MANIFEST_PYPROJECT = "pyproject"
MANIFEST_POM = "pom"
ManifestKind = Literal["none", "uv_lock", "requirements", "pyproject", "pom"]

# A ``[project]`` table header on its own line; ``[project.scripts]`` alone
# does not make a tool-config-only pyproject a dependency manifest.
_PROJECT_TABLE_RE = re.compile(r"^\[project\]\s*$", re.MULTILINE)


class AmbiguousManifests(ValueError):
    """Raised when a checkout root carries both a ``pom.xml`` and a Python manifest."""


def _python_manifest(context_dir: Path) -> ManifestKind:
    """The Python manifest at the root, by precedence; ``MANIFEST_NONE`` when there is none."""
    if (context_dir / "uv.lock").is_file():
        return MANIFEST_UV_LOCK
    if (context_dir / "requirements.txt").is_file():
        return MANIFEST_REQUIREMENTS
    pyproject = context_dir / "pyproject.toml"
    if pyproject.is_file() and _PROJECT_TABLE_RE.search(pyproject.read_text()):
        return MANIFEST_PYPROJECT
    return MANIFEST_NONE


def detect_manifest(context_dir: Path) -> ManifestKind:
    """Classify a checkout root by the dependency manifest it carries.

    Python manifests take precedence ``uv.lock`` > ``requirements.txt`` >
    ``pyproject.toml`` (the latter only with a ``[project]`` table). A
    ``pom.xml`` selects the JVM template; one next to a Python manifest is
    ambiguous and raises :class:`AmbiguousManifests`. Subdirectories are
    ignored."""
    python = _python_manifest(context_dir)
    if (context_dir / "pom.xml").is_file():
        if python != MANIFEST_NONE:
            raise AmbiguousManifests(
                f"pom.xml alongside a {python} manifest in the checkout: check in a Dockerfile to choose the image"
            )
        return MANIFEST_POM
    return python


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
