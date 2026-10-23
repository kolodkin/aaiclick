# syntax=docker/dockerfile:1
# Leak probe: declares the index ARGs an old-style Dockerfile would, and
# snapshots its build environment for the task to return.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG PIP_INDEX_URL
ARG PIP_EXTRA_INDEX_URL
USER root
RUN env > /build-env.txt && chmod 0644 /build-env.txt
USER aaiclick
COPY --chown=aaiclick:aaiclick . /src
WORKDIR /src
