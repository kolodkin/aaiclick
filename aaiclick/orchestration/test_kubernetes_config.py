"""Tests for the worker-side Pod config (cluster env)."""

from __future__ import annotations

import pytest

from aaiclick.orchestration.kubernetes_config import (
    ENV_IMAGE_PULL_SECRET,
    ENV_MAX_PENDING,
    ENV_MAX_PENDING_TOTAL,
    ENV_MAX_PODS,
    ENV_NAMESPACE,
    ENV_PENDING_TIMEOUT,
    ENV_SERVICE_ACCOUNT,
    K8sLimits,
    KubernetesConfig,
    resolve_k8s_limits,
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


def test_k8s_limits_defaults(monkeypatch):
    for var in (ENV_MAX_PENDING, ENV_MAX_PENDING_TOTAL, ENV_MAX_PODS, ENV_PENDING_TIMEOUT):
        monkeypatch.delenv(var, raising=False)
    assert resolve_k8s_limits() == K8sLimits(max_pending=10, max_pending_total=40, max_pods=None, pending_timeout=900.0)


def test_k8s_limits_total_follows_per_job(monkeypatch):
    monkeypatch.delenv(ENV_MAX_PENDING_TOTAL, raising=False)
    monkeypatch.setenv(ENV_MAX_PENDING, "3")
    assert resolve_k8s_limits().max_pending_total == 12


def test_k8s_limits_reads_env(monkeypatch):
    monkeypatch.setenv(ENV_MAX_PENDING, "2")
    monkeypatch.setenv(ENV_MAX_PENDING_TOTAL, "5")
    monkeypatch.setenv(ENV_MAX_PODS, "7")
    monkeypatch.setenv(ENV_PENDING_TIMEOUT, "30")
    assert resolve_k8s_limits() == K8sLimits(max_pending=2, max_pending_total=5, max_pods=7, pending_timeout=30.0)


@pytest.mark.parametrize(
    "var,value",
    [(ENV_MAX_PENDING, "0"), (ENV_MAX_PODS, "-1"), (ENV_PENDING_TIMEOUT, "abc"), (ENV_MAX_PENDING_TOTAL, "x")],
)
def test_k8s_limits_reject_invalid(monkeypatch, var, value):
    monkeypatch.setenv(var, value)
    with pytest.raises(ValueError, match=var):
        resolve_k8s_limits()
