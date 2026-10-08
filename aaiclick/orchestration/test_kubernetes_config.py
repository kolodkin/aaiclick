"""Tests for the worker-side Pod config (env + per-job resources)."""

from __future__ import annotations

from aaiclick.orchestration.kubernetes_config import (
    ENV_IMAGE_PULL_SECRET,
    ENV_NAMESPACE,
    ENV_SERVICE_ACCOUNT,
    KubernetesConfig,
    resolve_pod_config,
)


def test_resolve_pod_config_defaults(monkeypatch):
    for var in (ENV_NAMESPACE, ENV_SERVICE_ACCOUNT, ENV_IMAGE_PULL_SECRET):
        monkeypatch.delenv(var, raising=False)
    assert resolve_pod_config(resources=None) == KubernetesConfig(
        namespace="default", service_account=None, image_pull_secret=None, resources=None
    )


def test_resolve_pod_config_reads_env_and_resources(monkeypatch):
    monkeypatch.setenv(ENV_NAMESPACE, "env-ns")
    monkeypatch.setenv(ENV_SERVICE_ACCOUNT, "env-sa")
    monkeypatch.setenv(ENV_IMAGE_PULL_SECRET, "env-pull")
    cfg = resolve_pod_config(resources={"limits": {"cpu": "1"}})
    assert cfg == KubernetesConfig(
        namespace="env-ns", service_account="env-sa", image_pull_secret="env-pull", resources={"limits": {"cpu": "1"}}
    )
