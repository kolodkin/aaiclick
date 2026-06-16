"""Tests for the shared remote-executor env builder."""

from __future__ import annotations

from .runner_env import build_runner_env


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
