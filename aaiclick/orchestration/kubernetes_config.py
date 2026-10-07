"""Kubernetes Pod configuration, resolved on the worker at dispatch.

Namespace, service account and image-pull secret describe the cluster the
worker runs in and come from its environment; ``resources`` is the one
per-job setting (``Job.resources``).
"""

from __future__ import annotations

import os
from typing import NamedTuple

# Cluster settings sourced from the worker's environment, mirroring
# AAICLICK_REGISTRY: service account / image-pull-secret / namespace are
# deployment properties (the same across every job in a cluster) and bound by
# the worker's RBAC, so an operator sets them once. This matches Argo's
# workflowDefaults and Airflow's AIRFLOW__KUBERNETES__* config layer.
ENV_NAMESPACE = "AAICLICK_K8S_NAMESPACE"
ENV_SERVICE_ACCOUNT = "AAICLICK_K8S_SERVICE_ACCOUNT"
ENV_IMAGE_PULL_SECRET = "AAICLICK_K8S_IMAGE_PULL_SECRET"


class KubernetesConfig(NamedTuple):
    """Pod settings for one container task, resolved on the worker."""

    namespace: str
    service_account: str | None
    image_pull_secret: str | None
    resources: dict | None


def resolve_pod_config(*, resources: dict | None) -> KubernetesConfig:
    """Pod config for a container task, resolved on the worker at dispatch.

    Cluster settings come from the worker's environment (``AAICLICK_K8S_*``);
    ``resources`` is the job's own snapshot (``Job.resources``)."""
    return KubernetesConfig(
        namespace=os.environ.get(ENV_NAMESPACE) or "default",
        service_account=os.environ.get(ENV_SERVICE_ACCOUNT) or None,
        image_pull_secret=os.environ.get(ENV_IMAGE_PULL_SECRET) or None,
        resources=resources,
    )
