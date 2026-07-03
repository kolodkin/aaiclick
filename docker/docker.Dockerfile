# syntax=docker/dockerfile:1
# aaiclick + Docker CLI (client only). For the docker-dispatching worker,
# which runs `docker build`/`docker run` against a mounted host daemon.
ARG BASE_REF=ghcr.io/kolodkin/aaiclick:latest
FROM ${BASE_REF}

ARG DOCKER_CLI_VERSION=27.5.1
# Buildx sets TARGETARCH to amd64 / arm64; the static tarball uses x86_64 / aarch64.
ARG TARGETARCH

USER root
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) dockerarch=x86_64 ;; \
      arm64) dockerarch=aarch64 ;; \
      *) echo "unsupported arch ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    apt-get update; apt-get install -y --no-install-recommends curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    curl -fsSL "https://download.docker.com/linux/static/stable/${dockerarch}/docker-${DOCKER_CLI_VERSION}.tgz" -o /tmp/docker.tgz; \
    tar -xzf /tmp/docker.tgz -C /tmp; \
    install -m 0755 /tmp/docker/docker /usr/local/bin/docker; \
    rm -rf /tmp/docker /tmp/docker.tgz; \
    docker --version
USER aaiclick
