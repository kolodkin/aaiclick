"""Tests for ``aaiclick.internal_api.setup``."""

from __future__ import annotations

import json
import urllib.error

import pytest

from aaiclick.view_models import (
    MigrationAction,
    OllamaBootstrapResult,
    OllamaBootstrapStatus,
    SetupResult,
)

from . import errors, setup


def _stub_chdb_and_sqlalchemy(monkeypatch):
    """Replace chdb Session + SQLAlchemy DDL so ``setup()`` has no side effects.

    The ``pin_chdb_session`` session fixture already owns the one chdb Session
    the process is allowed to hold; spinning up a second one here would race
    chdb's native threadpool. The SQLAlchemy stub likewise avoids touching the
    test DB (whose schema is owned by the fixture-run alembic migration).
    """

    class _FakeSession:
        def __init__(self, path):
            pass

        def query(self, _sql):
            return None

        def cleanup(self):
            return None

    class _FakeEngine:
        def dispose(self):
            return None

    monkeypatch.setattr(setup, "Session", _FakeSession)
    monkeypatch.setattr(setup, "create_engine", lambda _url: _FakeEngine())
    monkeypatch.setattr(setup.SQLModel.metadata, "create_all", lambda _engine: None)


def test_setup_local_writes_marker_and_returns_ok_steps(tmp_path, monkeypatch):
    monkeypatch.setenv("AAICLICK_LOCAL_ROOT", str(tmp_path))
    monkeypatch.delenv("AAICLICK_SQL_URL", raising=False)
    monkeypatch.delenv("AAICLICK_CH_URL", raising=False)
    _stub_chdb_and_sqlalchemy(monkeypatch)

    result = setup.setup()

    assert isinstance(result, SetupResult)
    assert result.mode == "local"
    assert (tmp_path / "setup_done").exists()
    step_names = [s.name for s in result.steps]
    assert "chdb" in step_names and "sqlite" in step_names
    assert all(s.status == "ok" for s in result.steps if s.name in {"chdb", "sqlite"})
    assert setup.is_setup_done() is True


def test_setup_distributed_skips_local_steps(tmp_path, monkeypatch):
    monkeypatch.setenv("AAICLICK_LOCAL_ROOT", str(tmp_path))
    monkeypatch.setenv("AAICLICK_SQL_URL", "postgresql+asyncpg://u:p@h/db")
    monkeypatch.setenv("AAICLICK_CH_URL", "clickhouse://u:p@h:8123/default")

    result = setup.setup()

    assert result.mode == "distributed"
    statuses = {s.name: s.status for s in result.steps}
    assert statuses["clickhouse"] == "skipped"
    assert statuses["postgres"] == "skipped"
    assert (tmp_path / "setup_done").exists()


def test_setup_with_ai_non_ollama_populates_ollama_field(tmp_path, monkeypatch):
    monkeypatch.setenv("AAICLICK_LOCAL_ROOT", str(tmp_path))
    monkeypatch.delenv("AAICLICK_SQL_URL", raising=False)
    monkeypatch.delenv("AAICLICK_CH_URL", raising=False)
    monkeypatch.setenv("AAICLICK_AI_MODEL", "openai/gpt-4")
    _stub_chdb_and_sqlalchemy(monkeypatch)

    result = setup.setup(ai=True)

    assert result.ollama is not None
    assert result.ollama.status == OllamaBootstrapStatus.NOT_OLLAMA
    assert not any(s.name == "ollama" for s in result.steps)


def test_is_setup_done_false_without_marker(tmp_path, monkeypatch):
    monkeypatch.setenv("AAICLICK_LOCAL_ROOT", str(tmp_path))
    assert setup.is_setup_done() is False


def test_bootstrap_ollama_non_ollama_model():
    result = setup.bootstrap_ollama("openai/gpt-4")

    assert isinstance(result, OllamaBootstrapResult)
    assert result.status == OllamaBootstrapStatus.NOT_OLLAMA
    assert result.model == "openai/gpt-4"


def test_bootstrap_ollama_server_unreachable():
    result = setup.bootstrap_ollama("ollama/llama3.1:8b", base_url="http://127.0.0.1:1")

    assert result.status == OllamaBootstrapStatus.SERVER_UNREACHABLE
    assert "not reachable" in (result.detail or "")


def test_bootstrap_ollama_already_present(monkeypatch):
    monkeypatch.setattr(setup.urllib.request, "urlopen", lambda *a, **k: None)

    result = setup.bootstrap_ollama("ollama/llama3.1:8b")

    assert result.status == OllamaBootstrapStatus.ALREADY_PRESENT


def test_bootstrap_ollama_pulled(monkeypatch):
    def fake_urlopen(req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        if url.endswith("/api/show"):
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
        return _StubResponse(b'{"status": "success"}')

    monkeypatch.setattr(setup.urllib.request, "urlopen", fake_urlopen)

    result = setup.bootstrap_ollama("ollama/llama3.1:8b")

    assert result.status == OllamaBootstrapStatus.PULLED


def test_bootstrap_ollama_pull_unexpected_response(monkeypatch):
    def fake_urlopen(req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        if url.endswith("/api/show"):
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
        return _StubResponse(json.dumps({"status": "error"}).encode())

    monkeypatch.setattr(setup.urllib.request, "urlopen", fake_urlopen)

    result = setup.bootstrap_ollama("ollama/llama3.1:8b")

    assert result.status == OllamaBootstrapStatus.FAILED


def test_migrate_upgrade_invokes_alembic(monkeypatch):
    calls: list[tuple] = []
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, "upgrade", lambda config, revision: calls.append(("upgrade", revision)))

    result = setup.migrate(MigrationAction.UPGRADE)

    assert result.action == MigrationAction.UPGRADE
    assert result.revision == "head"
    assert calls == [("upgrade", "head")]


def test_migrate_downgrade_requires_revision(monkeypatch):
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, "downgrade", lambda *a, **k: None)

    with pytest.raises(errors.Invalid):
        setup.migrate(MigrationAction.DOWNGRADE)


def test_migrate_show_requires_revision(monkeypatch):
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, "show", lambda *a, **k: None)

    with pytest.raises(errors.Invalid):
        setup.migrate(MigrationAction.SHOW)


def test_migrate_current_runs_without_revision(monkeypatch):
    calls: list[tuple] = []
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(
        setup.command,
        "current",
        lambda config, verbose=False: calls.append(("current", verbose)),
    )

    result = setup.migrate(MigrationAction.CURRENT)

    assert result.action == MigrationAction.CURRENT
    assert result.revision is None
    assert calls == [("current", True)]


class _StubResponse:
    """Minimal ``urlopen`` stand-in that supports ``read()`` + context manager."""

    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False
