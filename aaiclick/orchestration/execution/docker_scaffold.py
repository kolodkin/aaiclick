"""Scaffold a starter Dockerfile for docker-runner jobs.

The framework deliberately does not bundle a runtime-default Dockerfile
(see ``docs/docker_runner_plan.md`` — image reproducibility requires that
the Dockerfile be checked in to the user's repo and versioned by the
git SHA the build task clones from). This module provides a CLI scaffold
that drops a sensible starter into the user's working directory; from
there the user owns it.

Invoked via ``python -m aaiclick docker init``."""

from __future__ import annotations

from pathlib import Path

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
#   GIT_REMOTE, GIT_SHA, GIT_BRANCH, BUILD_CONTEXT
#   PIP_INDEX_URL, PIP_EXTRA_INDEX_URL  (e.g. corporate / test pypi)
#   AAICLICK_VERSION                    (matches the host's installed version)

FROM python:3.10-slim

ARG GIT_REMOTE
ARG GIT_SHA
ARG GIT_BRANCH
ARG BUILD_CONTEXT
ARG PIP_INDEX_URL
ARG AAICLICK_VERSION

# Image metadata (visible via `docker inspect <image>`).
LABEL org.opencontainers.image.source="${GIT_REMOTE}"
LABEL org.opencontainers.image.revision="${GIT_SHA}"
LABEL org.opencontainers.image.ref.name="${GIT_BRANCH}"

# Runtime env (visible to task code via os.environ).
ENV GIT_REMOTE=${GIT_REMOTE} \\
    GIT_SHA=${GIT_SHA} \\
    GIT_BRANCH=${GIT_BRANCH} \\
    BUILD_CONTEXT=${BUILD_CONTEXT}

# Install aaiclick. Pinned to the host's version so the host worker and the
# container speak the same IPC protocol.
RUN pip install --no-cache-dir \\
    --index-url "${PIP_INDEX_URL:-https://pypi.org/simple/}" \\
    --extra-index-url https://pypi.org/simple/ \\
    "aaiclick[distributed]==${AAICLICK_VERSION}"

# Install the user's repo as a package so `importlib` can resolve task
# entrypoints. Replace this with the install method that suits your project
# (uv pip, poetry install --only main, etc.).
COPY . /src
RUN pip install --no-cache-dir /src
"""


class DockerfileExists(FileExistsError):
    """Raised when the target path already exists and ``force`` is False."""


def init_dockerfile(target: Path, *, force: bool = False) -> Path:
    """Write the starter Dockerfile to ``target``.

    Returns the resolved target path. Raises :class:`DockerfileExists`
    when the file already exists and ``force`` is False — silent
    overwrite would clobber whatever Dockerfile the user already had,
    which is exactly the kind of "magic" we're trying to avoid by not
    bundling a runtime default."""
    if target.exists() and not force:
        raise DockerfileExists(f"{target} already exists. Pass --force to overwrite.")
    target.write_text(DOCKERFILE_TEMPLATE)
    return target.resolve()
