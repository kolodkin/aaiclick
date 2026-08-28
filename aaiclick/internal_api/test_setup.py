"""Tests for ``aaiclick.internal_api.setup``."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine as sa_create_engine
from sqlalchemy import select as sa_select
from sqlalchemy import text as sa_text
from sqlmodel import SQLModel

from aaiclick.auth.models import Tenant
from aaiclick.oplog.migrate import ChVersionState
from aaiclick.tenancy import DEFAULT_TENANT_ID
from aaiclick.view_models import (
    MIGRATE_CURRENT,
    MIGRATE_DOWNGRADE,
    MIGRATE_HISTORY,
    MIGRATE_SHOW,
    MIGRATE_UPGRADE,
    OLLAMA_NOT_OLLAMA,
    ChVersionStatus,
    SetupResult,
)

from . import errors, setup


def _stub_chdb_and_sqlalchemy(monkeypatch):
    """Replace chdb Session + SQLAlchemy DDL so ``setup()`` has no side effects.

    chdb's Session is a per-process singleton; tests stub the shared-session
    accessor so ``setup()`` doesn't touch real chdb state. The SQLAlchemy stub
    likewise avoids touching the test DB (whose schema is owned by the
    fixture-run alembic migration).
    """

    class _FakeSession:
        def query(self, _sql):
            return None

    class _FakeEngine:
        def dispose(self):
            return None

    monkeypatch.setattr(setup, "get_shared_session", lambda _path: _FakeSession())
    monkeypatch.setattr(setup, "create_engine", lambda _url: _FakeEngine())
    monkeypatch.setattr(setup.SQLModel.metadata, "create_all", lambda _engine: None)
    monkeypatch.setattr(setup, "_seed_default_tenant", lambda _engine: None)


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
    assert result.ollama.status == OLLAMA_NOT_OLLAMA
    assert not any(s.name == "ollama" for s in result.steps)


def test_is_setup_done_false_without_marker(tmp_path, monkeypatch):
    monkeypatch.setenv("AAICLICK_LOCAL_ROOT", str(tmp_path))
    assert setup.is_setup_done() is False


def test_migrate_upgrade_invokes_alembic(monkeypatch):
    calls: list[tuple] = []
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, "upgrade", lambda config, revision: calls.append(("upgrade", revision)))
    monkeypatch.setattr(setup, "ch_upgrade_standalone", lambda: [])

    result = setup.migrate(MIGRATE_UPGRADE)

    assert result.action == MIGRATE_UPGRADE
    assert result.revision == "head"
    assert calls == [("upgrade", "head")]


def test_migrate_upgrade_runs_alembic_then_ch(monkeypatch):
    calls: list[tuple] = []
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, "upgrade", lambda config, revision: calls.append(("alembic", revision)))
    monkeypatch.setattr(setup, "ch_upgrade_standalone", lambda: calls.append(("ch",)) or ["0001"])

    result = setup.migrate(MIGRATE_UPGRADE)

    assert calls == [("alembic", "head"), ("ch",)]
    assert result.ch_versions_applied == ["0001"]


def test_migrate_current_reports_ch_versions(monkeypatch):
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, "current", lambda config, verbose: None)
    monkeypatch.setattr(
        setup,
        "ch_status_standalone",
        lambda: [ChVersionState(version="0001", applied=True), ChVersionState(version="0002", applied=False)],
    )

    result = setup.migrate(MIGRATE_CURRENT)

    assert result.ch_versions == [
        ChVersionStatus(version="0001", applied=True),
        ChVersionStatus(version="0002", applied=False),
    ]


def test_migrate_history_reports_ch_versions(monkeypatch):
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, "history", lambda config, verbose: None)
    monkeypatch.setattr(
        setup,
        "ch_status_standalone",
        lambda: [ChVersionState(version="0001", applied=True)],
    )

    result = setup.migrate(MIGRATE_HISTORY)

    assert result.ch_versions == [ChVersionStatus(version="0001", applied=True)]


def test_migrate_downgrade_stays_alembic_only(monkeypatch):
    calls: list[tuple] = []
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, "downgrade", lambda config, revision: calls.append(("alembic", revision)))
    monkeypatch.setattr(setup, "ch_upgrade_standalone", lambda: calls.append(("ch",)))

    result = setup.migrate(MIGRATE_DOWNGRADE, "-1")

    assert calls == [("alembic", "-1")]
    assert result.ch_versions_applied == []


def test_migrate_downgrade_requires_revision(monkeypatch):
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, "downgrade", lambda *a, **k: None)

    with pytest.raises(errors.Invalid):
        setup.migrate(MIGRATE_DOWNGRADE)


def test_migrate_show_requires_revision(monkeypatch):
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, "show", lambda *a, **k: None)

    with pytest.raises(errors.Invalid):
        setup.migrate(MIGRATE_SHOW)


def test_migrate_current_runs_without_revision(monkeypatch):
    calls: list[tuple] = []
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(
        setup.command,
        "current",
        lambda config, verbose=False: calls.append(("current", verbose)),
    )
    monkeypatch.setattr(setup, "ch_status_standalone", lambda: [])

    result = setup.migrate(MIGRATE_CURRENT)

    assert result.action == MIGRATE_CURRENT
    assert result.revision is None
    assert calls == [("current", True)]


def test_seed_default_tenant_inserts_once():
    engine = sa_create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)

    setup._seed_default_tenant(engine)
    setup._seed_default_tenant(engine)  # idempotent re-run

    with engine.connect() as conn:
        rows = conn.execute(sa_select(Tenant.id, Tenant.slug)).all()
    assert rows == [(DEFAULT_TENANT_ID, "aaiclick")]


def _older_sqlite_db(path):
    """Build a database shaped like one written before tenancy landed."""
    engine = sa_create_engine(f"sqlite:///{path}")
    SQLModel.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            sa_text(
                "INSERT INTO jobs (id, tenant_id, name, status, run_type, "
                "preservation_mode, runner_mode, created_at) "
                "VALUES (1, 1, 'old-job', 'pending', 'flat', 'NONE', 'subprocess', '2024-01-01')"
            )
        )
        conn.execute(sa_text("DROP INDEX ix_jobs_tenant_id"))
        conn.execute(sa_text("ALTER TABLE jobs DROP COLUMN tenant_id"))
        conn.execute(sa_text("DROP TABLE tenants"))
    engine.dispose()


def _stub_chdb(monkeypatch):
    class _FakeSession:
        def query(self, _sql):
            return None

    monkeypatch.setattr(setup, "get_shared_session", lambda _path: _FakeSession())


def test_stale_local_db_lists_columns_added_since(tmp_path, monkeypatch):
    """A database written by an older version reports the columns it lacks."""
    db = tmp_path / "local.db"
    monkeypatch.setenv("AAICLICK_SQL_URL", f"sqlite+aiosqlite:///{db}")
    _older_sqlite_db(db)

    assert "jobs.tenant_id" in setup.stale_local_db()


def test_stale_local_db_empty_for_current_schema(tmp_path, monkeypatch):
    """A database at the current schema is not reported stale."""
    db = tmp_path / "local.db"
    monkeypatch.setenv("AAICLICK_SQL_URL", f"sqlite+aiosqlite:///{db}")
    engine = sa_create_engine(f"sqlite:///{db}")
    SQLModel.metadata.create_all(engine)
    engine.dispose()

    assert setup.stale_local_db() == []


def test_stale_local_db_empty_when_absent(tmp_path, monkeypatch):
    """A first run has no database to compare against."""
    monkeypatch.setenv("AAICLICK_SQL_URL", f"sqlite+aiosqlite:///{tmp_path / 'local.db'}")

    assert setup.stale_local_db() == []


def test_setup_refuses_stale_local_db_without_force(tmp_path, monkeypatch):
    """Regression: ``setup`` used to run ``create_all`` over an older
    ``local.db``, which creates missing *tables* but never alters existing
    ones — so it reported ``ok`` while ``jobs`` silently kept its old shape
    and every tenant-scoped query failed with "no such column: tenant_id".

    It now refuses instead, naming the flag that recreates the database."""
    db = tmp_path / "local.db"
    monkeypatch.setenv("AAICLICK_LOCAL_ROOT", str(tmp_path))
    monkeypatch.setenv("AAICLICK_SQL_URL", f"sqlite+aiosqlite:///{db}")
    monkeypatch.delenv("AAICLICK_CH_URL", raising=False)
    _stub_chdb(monkeypatch)
    _older_sqlite_db(db)

    with pytest.raises(errors.Invalid, match="--force"):
        setup.setup()

    assert db.exists()


def test_setup_force_recreates_stale_local_db(tmp_path, monkeypatch):
    """``--force`` deletes the outdated database and rebuilds it current."""
    db = tmp_path / "local.db"
    monkeypatch.setenv("AAICLICK_LOCAL_ROOT", str(tmp_path))
    monkeypatch.setenv("AAICLICK_SQL_URL", f"sqlite+aiosqlite:///{db}")
    monkeypatch.delenv("AAICLICK_CH_URL", raising=False)
    _stub_chdb(monkeypatch)
    _older_sqlite_db(db)

    result = setup.setup(force=True)

    sqlite_step = next(s for s in result.steps if s.name == "sqlite")
    assert "recreated" in (sqlite_step.detail or "")
    assert setup.stale_local_db() == []
    engine = sa_create_engine(f"sqlite:///{db}")
    with engine.connect() as conn:
        assert conn.execute(sa_text("SELECT count(*) FROM jobs")).scalar() == 0
        assert conn.execute(sa_text("SELECT slug FROM tenants")).scalar() == "aaiclick"
    engine.dispose()


def test_setup_force_keeps_a_current_database(tmp_path, monkeypatch):
    """``--force`` only deletes on a schema collision — an up-to-date
    database keeps its rows so a routine re-run is never destructive."""
    db = tmp_path / "local.db"
    monkeypatch.setenv("AAICLICK_LOCAL_ROOT", str(tmp_path))
    monkeypatch.setenv("AAICLICK_SQL_URL", f"sqlite+aiosqlite:///{db}")
    monkeypatch.delenv("AAICLICK_CH_URL", raising=False)
    _stub_chdb(monkeypatch)
    engine = sa_create_engine(f"sqlite:///{db}")
    SQLModel.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            sa_text(
                "INSERT INTO jobs (id, tenant_id, name, status, run_type, "
                "preservation_mode, runner_mode, created_at) "
                "VALUES (1, 1, 'keep-me', 'pending', 'flat', 'NONE', 'subprocess', '2024-01-01')"
            )
        )

    setup.setup(force=True)

    with engine.connect() as conn:
        assert conn.execute(sa_text("SELECT name FROM jobs WHERE id = 1")).scalar() == "keep-me"
    engine.dispose()
