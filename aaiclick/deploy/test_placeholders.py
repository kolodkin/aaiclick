"""Tests for the scaffold placeholder marker and its startup warning."""

from __future__ import annotations

from unittest.mock import patch

import yaml

from . import placeholders
from .compose_scaffold import init_compose
from .k8s_scaffold import init_helm
from .placeholders import (
    JWT_SECRET_PLACEHOLDER,
    PLACEHOLDER_MARKER,
    placeholder_env_vars,
    warn_if_placeholder_credentials,
)


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


def _assert_marked_except_jwt_secret(credentials: dict[str, str], jwt_key: str) -> None:
    """Every credential carries the marker (the contract with the startup
    warning) except the JWT secret, which the scaffold fills with a random value."""
    assert credentials
    jwt_secret = credentials.pop(jwt_key)
    assert PLACEHOLDER_MARKER not in jwt_secret
    assert len(jwt_secret) >= 32
    for where, value in credentials.items():
        assert PLACEHOLDER_MARKER in value, where


def test_scaffolded_compose_credentials(tmp_path):
    target = tmp_path / "docker-compose.yaml"
    init_compose(target, image_tag="v1.0.0")

    _assert_marked_except_jwt_secret(_compose_credentials(target.read_text()), "server.AAICLICK_JWT_SECRET")


def test_scaffolded_helm_credentials(tmp_path):
    target = tmp_path / "aaiclick-chart"
    init_helm(target, image_tag="v1.0.0")

    values = yaml.safe_load((target / "values.yaml").read_text())
    credentials = {
        "env.sqlUrl": values["env"]["sqlUrl"],
        "env.chUrl": values["env"]["chUrl"],
        "auth.jwtSecret": values["auth"]["jwtSecret"],
        "auth.adminPassword": values["auth"]["adminPassword"],
        "devDependencies.postgres.password": values["devDependencies"]["postgres"]["password"],
        "devDependencies.clickhouse.password": values["devDependencies"]["clickhouse"]["password"],
    }
    _assert_marked_except_jwt_secret(credentials, "auth.jwtSecret")


def test_each_scaffold_gets_its_own_jwt_secret(tmp_path):
    first, second = tmp_path / "a.yaml", tmp_path / "b.yaml"
    init_compose(first, image_tag="v1.0.0")
    init_compose(second, image_tag="v1.0.0")

    secrets = {_compose_credentials(t.read_text())["server.AAICLICK_JWT_SECRET"] for t in (first, second)}
    assert len(secrets) == 2
    assert JWT_SECRET_PLACEHOLDER not in secrets
