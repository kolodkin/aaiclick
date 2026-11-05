"""Kubernetes Pod configuration, resolved on the worker at dispatch.

See docs/designs/kubernetes_runner.md — "Configuration".
"""

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

# Concurrency limits of the kubernetes worker (spec: concurrent worker design,
# "Capacity gate"). Unscheduled Pods are the backpressure signal; the per-job
# cap keeps one job stuck on capacity from blocking the rest.
ENV_MAX_PENDING = "AAICLICK_K8S_MAX_PENDING"
ENV_MAX_PENDING_TOTAL = "AAICLICK_K8S_MAX_PENDING_TOTAL"
ENV_MAX_PODS = "AAICLICK_K8S_MAX_PODS"
ENV_PENDING_TIMEOUT = "AAICLICK_K8S_PENDING_TIMEOUT"
DEFAULT_MAX_PENDING = 10
DEFAULT_PENDING_TIMEOUT = 900.0


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


class K8sLimits(NamedTuple):
    """Concurrency limits of the kubernetes worker, read from its environment."""

    max_pending: int
    """Unscheduled Pods a single job may have before its tasks stop being claimed."""
    max_pending_total: int
    """Unscheduled Pods across all jobs before claiming pauses."""
    max_pods: int | None
    """Optional cap on running Pods."""
    pending_timeout: float
    """Seconds a task may stay not-started before it fails (normal retries apply)."""


def _positive_int(var: str) -> int | None:
    """``int(os.environ[var])`` when set and > 0; None when unset; else ``ValueError`` naming ``var``."""
    raw = os.environ.get(var)
    if not raw:
        return None
    if not raw.isdigit() or int(raw) <= 0:
        raise ValueError(f"{var}={raw!r} is not a positive integer")
    return int(raw)


def resolve_k8s_limits() -> K8sLimits:
    """Limits from ``AAICLICK_K8S_MAX_PENDING`` (10), ``AAICLICK_K8S_MAX_PENDING_TOTAL``
    (4 × per-job), ``AAICLICK_K8S_MAX_PODS`` (unset) and
    ``AAICLICK_K8S_PENDING_TIMEOUT`` (900 s). A non-positive or unparsable value
    raises ``ValueError`` naming the variable."""
    max_pending = _positive_int(ENV_MAX_PENDING) or DEFAULT_MAX_PENDING
    max_pending_total = _positive_int(ENV_MAX_PENDING_TOTAL) or 4 * max_pending
    raw_timeout = os.environ.get(ENV_PENDING_TIMEOUT)
    try:
        pending_timeout = float(raw_timeout) if raw_timeout else DEFAULT_PENDING_TIMEOUT
    except ValueError:
        raise ValueError(f"{ENV_PENDING_TIMEOUT}={raw_timeout!r} is not a number of seconds") from None
    if pending_timeout <= 0:
        raise ValueError(f"{ENV_PENDING_TIMEOUT}={raw_timeout!r} is not a positive number of seconds")
    return K8sLimits(max_pending, max_pending_total, _positive_int(ENV_MAX_PODS), pending_timeout)
