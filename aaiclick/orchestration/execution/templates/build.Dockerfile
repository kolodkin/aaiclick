# Default image for a git build whose repo has no Dockerfile: a thin layer on
# the aaiclick base image. BASE_IMAGE is forwarded by the build task
# (AAICLICK_BASE_IMAGE on the worker, else the GHCR tag matching its version).
# --chown: the base image runs as USER aaiclick; a root-owned /src would break
# tasks that write relative paths.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
COPY --chown=aaiclick:aaiclick . /src
WORKDIR /src
