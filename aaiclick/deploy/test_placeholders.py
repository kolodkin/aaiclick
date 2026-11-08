"""Tests for scaffold placeholder filling and the startup refusal."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse

import pytest
import yaml

from .compose_scaffold import init_compose
from .k8s_scaffold import init_helm
from .placeholders import (
    CREDENTIAL_ENV_VARS,
    PLACEHOLDER_MARKER,
    fill_placeholders,
    placeholder_env_vars,
    require_no_placeholder_credentials,
)


def test_placeholder_env_vars_lists_only_unreplaced_values(monkeypatch):
    monkeypatch.setenv("AAICLICK_JWT_SECRET", f"{PLACEHOLDER_MARKER}-jwt-secret")
    monkeypatch.setenv("AAICLICK_ADMIN_PASSWORD", "a-real-password")
    monkeypatch.delenv("AAICLICK_SQL_URL", raising=False)

    assert placeholder_env_vars() == ["AAICLICK_JWT_SECRET"]


def test_refusal_names_every_unreplaced_variable(monkeypatch):
    monkeypatch.setenv("AAICLICK_JWT_SECRET", f"{PLACEHOLDER_MARKER}-jwt-secret")
    monkeypatch.setenv("AAICLICK_ADMIN_PASSWORD", f"{PLACEHOLDER_MARKER}-admin-password")

    with pytest.raises(RuntimeError, match="AAICLICK_JWT_SECRET, AAICLICK_ADMIN_PASSWORD"):
        require_no_placeholder_credentials()


def test_refusal_is_silent_once_credentials_are_replaced(monkeypatch):
    for name in CREDENTIAL_ENV_VARS:
        monkeypatch.setenv(name, "a-real-value")

    require_no_placeholder_credentials()


def test_fill_gives_a_repeated_token_one_value():
    values: dict[str, str] = {}
    text = fill_placeholders("pw: change-me-pg\nurl: x://u:change-me-pg@h\nother: change-me-ch", values)

    assert PLACEHOLDER_MARKER not in text
    assert set(values) == {"change-me-pg", "change-me-ch"}
    assert text.count(values["change-me-pg"]) == 2


def _dotenv(path: Path) -> dict[str, str]:
    lines = [line for line in path.read_text().splitlines() if line and not line.startswith("#")]
    return dict(line.split("=", 1) for line in lines)


def _compose_env(compose_path: Path) -> dict[str, dict[str, str]]:
    """Each service's environment with ``${VAR}`` / ``${VAR:?msg}`` resolved from the ``.env``."""
    dotenv = _dotenv(compose_path.with_name(".env"))
    text = re.sub(r"\$\{(\w+)(?::\?[^}]*)?\}", lambda m: dotenv[m.group(1)], compose_path.read_text())
    return {name: service.get("environment") or {} for name, service in yaml.safe_load(text)["services"].items()}


def test_scaffolded_compose_has_no_placeholders_and_matching_passwords(tmp_path):
    target = tmp_path / "docker-compose.yaml"
    scaffold = init_compose(target, image_tag="v1.0.0")

    assert f"{PLACEHOLDER_MARKER}-" not in target.read_text() + (tmp_path / ".env").read_text()
    env = _compose_env(target)
    server = env["server"]
    assert server["AAICLICK_ADMIN_PASSWORD"] == scaffold.admin_password
    assert urlparse(server["AAICLICK_SQL_URL"]).password == env["postgres"]["POSTGRES_PASSWORD"]
    assert urlparse(server["AAICLICK_CH_URL"]).password == env["clickhouse"]["CLICKHOUSE_PASSWORD"]


def test_scaffolded_helm_has_no_placeholders_and_matching_passwords(tmp_path):
    target = tmp_path / "aaiclick-chart"
    scaffold = init_helm(target, image_tag="v1.0.0")

    assert all(f"{PLACEHOLDER_MARKER}-" not in p.read_text() for p in target.rglob("*") if p.is_file())
    values = yaml.safe_load((target / "values.yaml").read_text())
    assert values["auth"]["adminPassword"] == scaffold.admin_password
    # The URLs are built from the devDependencies passwords, set nowhere else.
    assert values["env"]["sqlUrl"] == values["env"]["chUrl"] == ""
    for name in ("server.yaml", "worker.yaml", "background.yaml"):
        template = (target / "templates" / name).read_text()
        assert ".Values.env.sqlUrl" not in template and ".Values.env.chUrl" not in template
        assert 'include "aaiclick.sqlUrl"' in template and 'include "aaiclick.chUrl"' in template


def test_each_scaffold_gets_its_own_credentials(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    first = init_compose(tmp_path / "a" / "docker-compose.yaml", image_tag="v1.0.0")
    second = init_compose(tmp_path / "b" / "docker-compose.yaml", image_tag="v1.0.0")

    assert first.admin_password != second.admin_password
    secrets = {_compose_env(s.path)["server"]["AAICLICK_JWT_SECRET"] for s in (first, second)}
    assert len(secrets) == 2
