# syntax=docker/dockerfile:1
# aaiclick + docker CLI (inherited) + kubectl. For the k8s-dispatching worker,
# which builds/pushes the task image with docker, then creates pods with kubectl.
ARG BASE_REF=ghcr.io/kolodkin/aaiclick-docker:latest
FROM ${BASE_REF}

ARG KUBECTL_VERSION=v1.32.2
ARG TARGETARCH

USER root
# curl + ca-certificates are inherited from the docker variant this FROMs.
RUN set -eux; \
    curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl" -o /usr/local/bin/kubectl; \
    curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl.sha256" -o /tmp/kubectl.sha256; \
    echo "$(cat /tmp/kubectl.sha256)  /usr/local/bin/kubectl" | sha256sum -c -; \
    chmod 0755 /usr/local/bin/kubectl; \
    rm -f /tmp/kubectl.sha256; \
    kubectl version --client
USER aaiclick
