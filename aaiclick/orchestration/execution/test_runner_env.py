"""Tests for the shared remote-executor env builder."""

from __future__ import annotations

import pytest

from .runner_env import (
    ENV_WORKER_RUNNER,
    RUNNER_DOCKER,
    RUNNER_KUBERNETES,
    build_runner_env,
    get_worker_runner,
    validate_worker_runner,
)


def test_build_runner_env_includes_always_passed(monkeypatch):
    monkeypatch.setenv("AAICLICK_SQL_URL", "postgresql+asyncpg://pg/x")
    monkeypatch.setenv("AAICLICK_CH_URL", "clickhouse://ch/x")
    monkeypatch.setenv("AAICLICK_TASK_TIMEOUT", "60")
    monkeypatch.delenv("AAICLICK_DEFAULT_PRESERVATION_MODE", raising=False)
    monkeypatch.delenv("AAICLICK_PASSTHROUGH_ENV", raising=False)

    env = build_runner_env()
    assert env["AAICLICK_SQL_URL"] == "postgresql+asyncpg://pg/x"
    assert env["AAICLICK_CH_URL"] == "clickhouse://ch/x"
    assert env["AAICLICK_TASK_TIMEOUT"] == "60"
    assert "AAICLICK_DEFAULT_PRESERVATION_MODE" not in env


def test_build_runner_env_passthrough(monkeypatch):
    monkeypatch.setenv("AAICLICK_SQL_URL", "u")
    monkeypatch.setenv("AAICLICK_CH_URL", "u")
    monkeypatch.setenv("AAICLICK_PASSTHROUGH_ENV", "FOO,BAR,UNSET")
    monkeypatch.setenv("FOO", "1")
    monkeypatch.setenv("BAR", "2")
    monkeypatch.delenv("UNSET", raising=False)

    env = build_runner_env()
    assert env["FOO"] == "1"
    assert env["BAR"] == "2"
    assert "UNSET" not in env


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param(None, None, id="unset"),
        pytest.param("", None, id="empty"),
        pytest.param("docker", RUNNER_DOCKER, id="docker"),
        pytest.param("kubernetes", RUNNER_KUBERNETES, id="kubernetes"),
    ],
)
def test_get_worker_runner(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv(ENV_WORKER_RUNNER, raising=False)
    else:
        monkeypatch.setenv(ENV_WORKER_RUNNER, value)
    assert get_worker_runner() == expected


def test_validate_worker_runner_rejects_unknown(monkeypatch):
    monkeypatch.setenv(ENV_WORKER_RUNNER, "podman")
    with pytest.raises(ValueError, match=r"AAICLICK_RUNNER.*docker.*kubernetes"):
        validate_worker_runner()


def test_validate_worker_runner_rejects_kubernetes_with_local_build(monkeypatch):
    monkeypatch.setenv(ENV_WORKER_RUNNER, "kubernetes")
    monkeypatch.setenv("AAICLICK_LOCAL_BUILD", "1")
    with pytest.raises(ValueError, match="AAICLICK_LOCAL_BUILD"):
        validate_worker_runner()


@pytest.mark.parametrize("value", [None, "docker", "kubernetes"])
def test_validate_worker_runner_accepts(monkeypatch, value):
    monkeypatch.delenv("AAICLICK_LOCAL_BUILD", raising=False)
    if value is None:
        monkeypatch.delenv(ENV_WORKER_RUNNER, raising=False)
    else:
        monkeypatch.setenv(ENV_WORKER_RUNNER, value)
    validate_worker_runner()
