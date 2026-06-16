"""Kubernetes runner configuration.

Resolves the cluster-specific config (namespace, service account, image-pull
secret, resource requests/limits) into a snapshot stored on the ``Job`` row's
``kubernetes_config`` JSON column. Git/image resolution is shared with the
Docker runner (``docker_config.resolve_docker_config``) and is not duplicated
here.
"""

from __future__ import annotations

from typing import NamedTuple

from .models import RegisteredJob


def build_kubernetes_config(
    *,
    namespace: str | None = None,
    service_account: str | None = None,
    image_pull_secret: str | None = None,
) -> dict | None:
    """Build the ``kubernetes_config`` JSON dict from individual fields,
    dropping unset ones. Returns ``None`` when nothing is set (so a non-k8s
    registration stores no cluster config). Used by the CLI / API boundary."""
    config = {
        "namespace": namespace,
        "service_account": service_account,
        "image_pull_secret": image_pull_secret,
    }
    return {k: v for k, v in config.items() if v is not None} or None


class KubernetesConfig(NamedTuple):
    """Snapshot of Kubernetes cluster config resolved at submission time."""

    namespace: str
    service_account: str | None
    image_pull_secret: str | None
    resources: dict | None


def resolve_kubernetes_config(
    registered: RegisteredJob | None,
    *,
    namespace: str | None = None,
    service_account: str | None = None,
    image_pull_secret: str | None = None,
    resources: dict | None = None,
) -> KubernetesConfig:
    """Resolve cluster config for a single ``run_job`` call.

    Precedence (highest first) per field: explicit kwarg →
    ``registered.kubernetes_config`` default → hardcoded default.
    """
    defaults = registered.kubernetes_config if registered is not None and registered.kubernetes_config else {}

    def pick(value: object | None, key: str) -> object | None:
        return value if value is not None else defaults.get(key)

    return KubernetesConfig(
        namespace=pick(namespace, "namespace") or "default",
        service_account=pick(service_account, "service_account"),
        image_pull_secret=pick(image_pull_secret, "image_pull_secret"),
        resources=pick(resources, "resources"),
    )
