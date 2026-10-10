"""Tests for the worker-side Pod config (cluster env)."""

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
    assert resolve_pod_config() == KubernetesConfig(namespace="default", service_account=None, image_pull_secret=None)


def test_resolve_pod_config_reads_env(monkeypatch):
    monkeypatch.setenv(ENV_NAMESPACE, "env-ns")
    monkeypatch.setenv(ENV_SERVICE_ACCOUNT, "env-sa")
    monkeypatch.setenv(ENV_IMAGE_PULL_SECRET, "env-pull")
    cfg = resolve_pod_config()
    assert cfg == KubernetesConfig(namespace="env-ns", service_account="env-sa", image_pull_secret="env-pull")
