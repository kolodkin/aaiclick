"""Tests for Kubernetes config resolution (four-layer precedence)."""

from __future__ import annotations

import pytest

from aaiclick.orchestration.kubernetes_config import (
    ENV_IMAGE_PULL_SECRET,
    ENV_NAMESPACE,
    ENV_SERVICE_ACCOUNT,
    KubernetesConfig,
    resolve_kubernetes_config,
)
from aaiclick.orchestration.models import RegisteredJob


def test_defaults_when_nothing_set():
    cfg = resolve_kubernetes_config(None)
    assert cfg == KubernetesConfig(namespace="default", service_account=None, image_pull_secret=None, resources=None)


def test_registered_defaults_used():
    reg = RegisteredJob(name="r", entrypoint="m.f", kubernetes_config={"namespace": "ml", "service_account": "sa"})
    cfg = resolve_kubernetes_config(reg)
    assert cfg.namespace == "ml"
    assert cfg.service_account == "sa"


def test_kwargs_override_registered():
    reg = RegisteredJob(name="r", entrypoint="m.f", kubernetes_config={"namespace": "ml"})
    cfg = resolve_kubernetes_config(reg, namespace="prod", resources={"limits": {"cpu": "1"}})
    assert cfg.namespace == "prod"
    assert cfg.resources == {"limits": {"cpu": "1"}}


def test_env_used_when_registered_unset(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(ENV_NAMESPACE, "env-ns")
    monkeypatch.setenv(ENV_SERVICE_ACCOUNT, "env-sa")
    monkeypatch.setenv(ENV_IMAGE_PULL_SECRET, "env-pull")
    cfg = resolve_kubernetes_config(None)
    assert cfg == KubernetesConfig(
        namespace="env-ns", service_account="env-sa", image_pull_secret="env-pull", resources=None
    )


def test_registered_outranks_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(ENV_SERVICE_ACCOUNT, "env-sa")
    reg = RegisteredJob(name="r", entrypoint="m.f", kubernetes_config={"service_account": "job-sa"})
    cfg = resolve_kubernetes_config(reg)
    assert cfg.service_account == "job-sa"


def test_kwarg_outranks_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(ENV_NAMESPACE, "env-ns")
    cfg = resolve_kubernetes_config(None, namespace="kwarg-ns")
    assert cfg.namespace == "kwarg-ns"
