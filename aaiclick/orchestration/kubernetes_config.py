"""Kubernetes Pod configuration, resolved on the worker at dispatch."""

from __future__ import annotations

import os
from typing import NamedTuple

# Cluster settings are worker env, like AAICLICK_REGISTRY: they are the same
# for every job and bound by the worker's RBAC (cf. Argo workflowDefaults,
# Airflow AIRFLOW__KUBERNETES__*). The one per-job setting, ``resources``,
# rides on ``JobDispatch`` since the docker runner honours it too.
ENV_NAMESPACE = "AAICLICK_K8S_NAMESPACE"
ENV_SERVICE_ACCOUNT = "AAICLICK_K8S_SERVICE_ACCOUNT"
ENV_IMAGE_PULL_SECRET = "AAICLICK_K8S_IMAGE_PULL_SECRET"


class KubernetesConfig(NamedTuple):
    """Cluster settings for a container task's Pod, resolved on the worker."""

    namespace: str
    service_account: str | None
    image_pull_secret: str | None


def resolve_pod_config() -> KubernetesConfig:
    """Pod config for a container task, read from the worker's environment
    (``AAICLICK_K8S_*``) at dispatch."""
    return KubernetesConfig(
        namespace=os.environ.get(ENV_NAMESPACE) or "default",
        service_account=os.environ.get(ENV_SERVICE_ACCOUNT) or None,
        image_pull_secret=os.environ.get(ENV_IMAGE_PULL_SECRET) or None,
    )
