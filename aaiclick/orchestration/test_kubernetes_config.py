"""Tests for Kubernetes config resolution (three-layer precedence)."""

from __future__ import annotations

from aaiclick.orchestration.kubernetes_config import KubernetesConfig, resolve_kubernetes_config
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
