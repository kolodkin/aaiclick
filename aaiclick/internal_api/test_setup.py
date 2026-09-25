"""Tests for ``aaiclick.internal_api.setup``."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine as sa_create_engine
from sqlalchemy import text as sa_text
from sqlmodel import SQLModel

from aaiclick.oplog.migrate import ChVersionState
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


@pytest.fixture
def local_db(tmp_path, monkeypatch):
    """Point aaiclick at an empty local root and yield its ``local.db`` path.

    The chdb step is stubbed — chdb allows one session per process, so it isn't covered here.
    """
    db = tmp_path / "local.db"
    monkeypatch.setenv("AAICLICK_LOCAL_ROOT", str(tmp_path))
    monkeypatch.setenv("AAICLICK_SQL_URL", f"sqlite+aiosqlite:///{db}")
    monkeypatch.delenv("AAICLICK_CH_URL", raising=False)
    monkeypatch.setattr(setup, "get_shared_session", lambda _path: MagicMock())
    return db


def test_setup_local_writes_marker_and_returns_ok_steps(local_db, tmp_path):
    result = setup.setup()

    assert isinstance(result, SetupResult)
    assert result.mode == "local"
    assert (tmp_path / "setup_done").exists()
    step_names = [s.name for s in result.steps]
    assert "chdb" in step_names and "sqlite" in step_names
    assert all(s.status == "ok" for s in result.steps if s.name in {"chdb", "sqlite"})
    assert setup.is_setup_done() is True
    assert setup.stale_local_db() == []
    assert setup.missing_local_tables() == []


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


def test_setup_with_ai_non_ollama_populates_ollama_field(local_db, monkeypatch):
    monkeypatch.setenv("AAICLICK_AI_MODEL", "openai/gpt-4")

    result = setup.setup(ai=True)

    assert result.ollama is not None
    assert result.ollama.status == OLLAMA_NOT_OLLAMA
    assert not any(s.name == "ollama" for s in result.steps)


def test_is_setup_done_false_without_marker(tmp_path, monkeypatch):
    monkeypatch.setenv("AAICLICK_LOCAL_ROOT", str(tmp_path))
    assert setup.is_setup_done() is False


def test_migrate_upgrade_runs_alembic_then_ch(monkeypatch):
    calls: list[tuple] = []
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, "upgrade", lambda config, revision: calls.append(("alembic", revision)))
    monkeypatch.setattr(setup, "ch_upgrade_standalone", lambda: calls.append(("ch",)) or ["0001"])

    result = setup.migrate(MIGRATE_UPGRADE)

    assert result.action == MIGRATE_UPGRADE
    assert result.revision == "head"
    assert calls == [("alembic", "head"), ("ch",)]
    assert result.ch_versions_applied == ["0001"]


@pytest.mark.parametrize(
    "action, alembic_command, ch_states",
    [
        pytest.param(
            MIGRATE_CURRENT,
            "current",
            [ChVersionState(version="0001", applied=True), ChVersionState(version="0002", applied=False)],
            id="current",
        ),
        pytest.param(MIGRATE_HISTORY, "history", [ChVersionState(version="0001", applied=True)], id="history"),
    ],
)
def test_migrate_reports_ch_versions(monkeypatch, action, alembic_command, ch_states):
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, alembic_command, lambda config, verbose: None)
    monkeypatch.setattr(setup, "ch_status_standalone", lambda: ch_states)

    result = setup.migrate(action)

    expected = [ChVersionStatus(version=s.version, applied=s.applied) for s in ch_states]
    assert result.ch_versions == expected


def test_migrate_downgrade_stays_alembic_only(monkeypatch):
    calls: list[tuple] = []
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, "downgrade", lambda config, revision: calls.append(("alembic", revision)))
    monkeypatch.setattr(setup, "ch_upgrade_standalone", lambda: calls.append(("ch",)))

    result = setup.migrate(MIGRATE_DOWNGRADE, "-1")

    assert calls == [("alembic", "-1")]
    assert result.ch_versions_applied == []


@pytest.mark.parametrize(
    "action",
    [
        pytest.param(MIGRATE_DOWNGRADE, id="downgrade"),
        pytest.param(MIGRATE_SHOW, id="show"),
    ],
)
def test_migrate_requires_revision(monkeypatch, action):
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())

    with pytest.raises(errors.Invalid, match="requires a revision"):
        setup.migrate(action)


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


_INSERT_JOB = (
    "INSERT INTO jobs (id, name, status, run_type, preservation_mode, runner_mode, created_at) "
    "VALUES (1, :name, 'pending', 'flat', 'NONE', 'subprocess', '2024-01-01')"
)


def _current_sqlite_db(path, *, job_name: str | None = None):
    """Build a database at the current schema, optionally holding one job."""
    engine = sa_create_engine(f"sqlite:///{path}")
    SQLModel.metadata.create_all(engine)
    if job_name is not None:
        with engine.begin() as conn:
            conn.execute(sa_text(_INSERT_JOB), {"name": job_name})
    return engine


def _older_sqlite_db(path):
    """Build a database shaped like one written before ``jobs.error`` existed."""
    engine = _current_sqlite_db(path, job_name="old-job")
    with engine.begin() as conn:
        conn.execute(sa_text("ALTER TABLE jobs DROP COLUMN error"))
    engine.dispose()


def test_stale_local_db_lists_columns_added_since(local_db):
    """A database written by an older version reports the columns it lacks."""
    _older_sqlite_db(local_db)

    assert "jobs.error" in setup.stale_local_db()


def test_stale_local_db_empty_for_current_schema(local_db):
    """A database at the current schema is not reported stale."""
    _current_sqlite_db(local_db).dispose()

    assert setup.stale_local_db() == []


def test_stale_local_db_sees_a_dropped_index(local_db):
    """Drift the models express but a column comparison cannot see.

    The check builds a reference database and diffs against it, so indexes,
    unique constraints and foreign keys count too — a database whose columns
    all match is not necessarily the one ``setup`` would build.
    """
    engine = _current_sqlite_db(local_db)
    with engine.begin() as conn:
        conn.execute(sa_text("DROP INDEX ix_jobs_status"))
    engine.dispose()

    assert "jobs.ix_jobs_status (index)" in setup.stale_local_db()


def test_missing_local_tables_is_separate_from_shape_drift(local_db):
    """A dropped table is reported apart from a wrong-shaped one, because
    ``create_all`` can add it back without recreating the database."""
    engine = _current_sqlite_db(local_db)
    with engine.begin() as conn:
        conn.execute(sa_text("DROP TABLE groups"))
    engine.dispose()

    assert setup.missing_local_tables() == ["groups"]
    assert setup.stale_local_db() == []


def test_stale_local_db_empty_when_absent(local_db):
    """A first run has no database to compare against."""
    assert setup.stale_local_db() == []


def test_setup_refuses_stale_local_db_without_force(local_db):
    """Regression: ``setup`` used to run ``create_all`` over an older
    ``local.db``, which creates missing *tables* but never alters existing
    ones — so it reported ``ok`` while ``jobs`` silently kept its old shape
    and every query against it failed with "no such column".

    It now refuses instead, naming the flag that recreates the database."""
    _older_sqlite_db(local_db)

    with pytest.raises(errors.Invalid, match="--force"):
        setup.setup()

    assert local_db.exists()


def test_setup_force_recreates_stale_local_db(local_db):
    """``--force`` deletes the outdated database and rebuilds it current."""
    _older_sqlite_db(local_db)

    result = setup.setup(force=True)

    sqlite_step = next(s for s in result.steps if s.name == "sqlite")
    assert "recreated" in (sqlite_step.detail or "")
    assert setup.stale_local_db() == []
    engine = sa_create_engine(f"sqlite:///{local_db}")
    with engine.connect() as conn:
        assert conn.execute(sa_text("SELECT count(*) FROM jobs")).scalar() == 0
    engine.dispose()


def test_setup_force_keeps_a_current_database(local_db):
    """``--force`` only deletes on a schema collision — an up-to-date
    database keeps its rows so a routine re-run is never destructive."""
    engine = _current_sqlite_db(local_db, job_name="keep-me")

    setup.setup(force=True)

    with engine.connect() as conn:
        assert conn.execute(sa_text("SELECT name FROM jobs WHERE id = 1")).scalar() == "keep-me"
    engine.dispose()
