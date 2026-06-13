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
ENV GIT_REMOTE=${GIT_REMOTE} \
    GIT_SHA=${GIT_SHA} \
    GIT_BRANCH=${GIT_BRANCH}

# Install aaiclick. Pinned to the host's version so the host worker and the
# container speak the same IPC protocol.
RUN pip install --no-cache-dir \
    --index-url "${PIP_INDEX_URL:-https://pypi.org/simple/}" \
    --extra-index-url https://pypi.org/simple/ \
    ${PIP_TRUSTED_HOST:+--trusted-host ${PIP_TRUSTED_HOST}} \
    "aaiclick[distributed]==${AAICLICK_VERSION}"

# Install the user's repo as a package so `importlib` can resolve task
# entrypoints. Replace this with the install method that suits your project
# (uv pip, poetry install --only main, etc.). WORKDIR is the project root so
# the container resolves entrypoints from the workdir — the same way the host
# CLI does when run from the repo.
COPY . /src
WORKDIR /src
RUN pip install --no-cache-dir /src
