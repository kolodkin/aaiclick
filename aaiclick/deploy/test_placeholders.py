"""Tests for the scaffold placeholder marker and its startup warning."""

from __future__ import annotations

from unittest.mock import patch

import pytest
import yaml

from . import placeholders
from .compose_scaffold import init_compose
from .k8s_scaffold import init_helm
from .placeholders import PLACEHOLDER_MARKER, placeholder_env_vars, warn_if_placeholder_credentials


def test_placeholder_env_vars_lists_only_unreplaced_values(monkeypatch):
    monkeypatch.setenv("AAICLICK_JWT_SECRET", f"{PLACEHOLDER_MARKER}-jwt-secret")
    monkeypatch.setenv("AAICLICK_ADMIN_PASSWORD", "a-real-password")
    monkeypatch.delenv("AAICLICK_SQL_URL", raising=False)

    assert placeholder_env_vars() == ["AAICLICK_JWT_SECRET"]


def test_warn_names_every_unreplaced_variable(monkeypatch):
    monkeypatch.setenv("AAICLICK_JWT_SECRET", f"{PLACEHOLDER_MARKER}-jwt-secret")
    monkeypatch.setenv("AAICLICK_ADMIN_PASSWORD", f"{PLACEHOLDER_MARKER}-admin-password")

    with patch.object(placeholders.logger, "warning") as warning:
        warn_if_placeholder_credentials()

    message = warning.call_args[0][0] % warning.call_args[0][1:]
    assert "AAICLICK_JWT_SECRET" in message
    assert "AAICLICK_ADMIN_PASSWORD" in message


def test_warn_is_silent_once_credentials_are_replaced(monkeypatch):
    for name in placeholders.CREDENTIAL_ENV_VARS:
        monkeypatch.setenv(name, "a-real-value")

    with patch.object(placeholders.logger, "warning") as warning:
        warn_if_placeholder_credentials()

    warning.assert_not_called()


def _compose_credentials(text: str) -> dict[str, str]:
    """Every credential-bearing env value in the scaffolded compose file."""
    services = yaml.safe_load(text)["services"]
    return {
        f"{name}.{key}": value
        for name, service in services.items()
        for key, value in (service.get("environment") or {}).items()
        if "PASSWORD" in key or "SECRET" in key or key.endswith("_URL")
    }


def test_scaffolded_compose_credentials_all_carry_the_marker(tmp_path):
    """The marker is the contract between the templates and the startup
    warning: a credential shipped without it is never flagged as unreplaced."""
    target = tmp_path / "docker-compose.yaml"
    init_compose(target, image_tag="v1.0.0")

    credentials = _compose_credentials(target.read_text())
    assert credentials
    for where, value in credentials.items():
        assert PLACEHOLDER_MARKER in value, where


@pytest.mark.parametrize(
    "path",
    [
        pytest.param(("env", "sqlUrl"), id="sql-url"),
        pytest.param(("env", "chUrl"), id="ch-url"),
        pytest.param(("auth", "jwtSecret"), id="jwt-secret"),
        pytest.param(("auth", "adminPassword"), id="admin-password"),
        pytest.param(("devDependencies", "postgres", "password"), id="dev-postgres"),
        pytest.param(("devDependencies", "clickhouse", "password"), id="dev-clickhouse"),
    ],
)
def test_scaffolded_helm_credentials_all_carry_the_marker(tmp_path, path):
    target = tmp_path / "aaiclick-chart"
    init_helm(target, image_tag="v1.0.0")

    value = yaml.safe_load((target / "values.yaml").read_text())
    for key in path:
        value = value[key]
    assert PLACEHOLDER_MARKER in value
