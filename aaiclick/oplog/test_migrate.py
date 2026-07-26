"""Tests for the ClickHouse migration runner's pure file/SQL handling."""

from __future__ import annotations

from pathlib import Path

import pytest

from aaiclick.data.data_context.ch_client import get_ch_client
from aaiclick.oplog.migrate import (
    MigrationFile,
    ch_applied_versions,
    ch_pending,
    ch_upgrade,
    list_migration_files,
    split_statements,
)


def _write(tmp_path: Path, name: str, content: str = "SELECT 1;") -> Path:
    path = tmp_path / name
    path.write_text(content)
    return path


def test_list_migration_files_sorted_by_version(tmp_path):
    _write(tmp_path, "0002_add_column.sql")
    _write(tmp_path, "0001_baseline.sql")

    files = list_migration_files(tmp_path)

    assert [f.version for f in files] == ["0001", "0002"]
    assert files[0] == MigrationFile(version="0001", path=tmp_path / "0001_baseline.sql")


def test_list_migration_files_empty_dir(tmp_path):
    assert list_migration_files(tmp_path) == []


def test_list_migration_files_rejects_duplicate_versions(tmp_path):
    _write(tmp_path, "0001_a.sql")
    _write(tmp_path, "0001_b.sql")

    with pytest.raises(ValueError, match="duplicate"):
        list_migration_files(tmp_path)


def test_list_migration_files_rejects_bad_names(tmp_path):
    _write(tmp_path, "baseline.sql")

    with pytest.raises(ValueError, match="baseline.sql"):
        list_migration_files(tmp_path)


def test_split_statements_on_trailing_semicolon():
    sql = "CREATE TABLE a (x UInt64) ENGINE = MergeTree ORDER BY x;\nCREATE TABLE b (y String) ENGINE = Memory;\n"

    assert split_statements(sql) == [
        "CREATE TABLE a (x UInt64) ENGINE = MergeTree ORDER BY x",
        "CREATE TABLE b (y String) ENGINE = Memory",
    ]


def test_split_statements_strips_comments_and_blanks():
    sql = "-- create the table\nCREATE TABLE a (\n    x UInt64  -- the key\n) ENGINE = Memory;\n\n"

    assert split_statements(sql) == ["CREATE TABLE a (\n    x UInt64\n) ENGINE = Memory"]


def test_split_statements_last_statement_without_semicolon():
    assert split_statements("SELECT 1") == ["SELECT 1"]


async def test_ch_upgrade_applies_pending_in_order(orch_ctx, tmp_path):
    _write(tmp_path, "0001_a.sql", "CREATE TABLE IF NOT EXISTS mig_a (x UInt64) ENGINE = Memory;")
    _write(
        tmp_path,
        "0002_b.sql",
        "CREATE TABLE IF NOT EXISTS mig_b (y UInt64) ENGINE = Memory;\nINSERT INTO mig_b VALUES (1);",
    )
    ch = get_ch_client()

    applied = await ch_upgrade(ch, migrations_dir=tmp_path)

    assert applied == ["0001", "0002"]
    assert await ch_pending(ch, migrations_dir=tmp_path) == []
    assert await ch_applied_versions(ch) == ["0001", "0002"]
    rows = (await ch.query("SELECT y FROM mig_b")).result_rows
    assert rows == [(1,)]


async def test_ch_upgrade_second_run_is_noop(orch_ctx, tmp_path):
    _write(tmp_path, "0001_a.sql", "CREATE TABLE IF NOT EXISTS mig_a (x UInt64) ENGINE = Memory;")
    ch = get_ch_client()
    await ch_upgrade(ch, migrations_dir=tmp_path)

    assert await ch_upgrade(ch, migrations_dir=tmp_path) == []


async def test_ch_upgrade_applies_only_new_versions(orch_ctx, tmp_path):
    _write(tmp_path, "0001_a.sql", "CREATE TABLE IF NOT EXISTS mig_a (x UInt64) ENGINE = Memory;")
    ch = get_ch_client()
    await ch_upgrade(ch, migrations_dir=tmp_path)
    _write(tmp_path, "0002_b.sql", "CREATE TABLE IF NOT EXISTS mig_b (y UInt64) ENGINE = Memory;")

    assert await ch_upgrade(ch, migrations_dir=tmp_path) == ["0002"]


async def test_ch_pending_rejects_gap(orch_ctx, tmp_path):
    """An unapplied version older than an applied one means history was rewritten."""
    _write(tmp_path, "0002_b.sql", "CREATE TABLE IF NOT EXISTS mig_b (y UInt64) ENGINE = Memory;")
    ch = get_ch_client()
    await ch_upgrade(ch, migrations_dir=tmp_path)
    _write(tmp_path, "0001_a.sql", "CREATE TABLE IF NOT EXISTS mig_a (x UInt64) ENGINE = Memory;")

    with pytest.raises(RuntimeError, match="0001"):
        await ch_pending(ch, migrations_dir=tmp_path)


async def test_ch_pending_rejects_unknown_applied_version(orch_ctx, tmp_path):
    _write(tmp_path, "0001_a.sql", "CREATE TABLE IF NOT EXISTS mig_a (x UInt64) ENGINE = Memory;")
    ch = get_ch_client()
    await ch_upgrade(ch, migrations_dir=tmp_path)
    (tmp_path / "0001_a.sql").unlink()

    with pytest.raises(RuntimeError, match="0001"):
        await ch_pending(ch, migrations_dir=tmp_path)


async def test_ch_upgrade_mid_script_failure_leaves_version_unrecorded(orch_ctx, tmp_path):
    _write(
        tmp_path,
        "0001_a.sql",
        "CREATE TABLE IF NOT EXISTS mig_a (x UInt64) ENGINE = Memory;\nSELECT broken syntax here;",
    )
    ch = get_ch_client()

    with pytest.raises(Exception):
        await ch_upgrade(ch, migrations_dir=tmp_path)

    assert await ch_applied_versions(ch) == []
    # Fix the script; the idempotent first statement re-runs harmlessly.
    _write(tmp_path, "0001_a.sql", "CREATE TABLE IF NOT EXISTS mig_a (x UInt64) ENGINE = Memory;")
    assert await ch_upgrade(ch, migrations_dir=tmp_path) == ["0001"]


async def test_ch_upgrade_dry_run_executes_nothing(orch_ctx, tmp_path):
    _write(tmp_path, "0001_a.sql", "CREATE TABLE IF NOT EXISTS mig_dry (x UInt64) ENGINE = Memory;")
    ch = get_ch_client()

    pending = await ch_upgrade(ch, migrations_dir=tmp_path, dry_run=True)

    assert pending == ["0001"]
    assert await ch_applied_versions(ch) == []
    exists = (await ch.query("EXISTS TABLE mig_dry")).result_rows
    assert exists[0][0] == 0


async def test_baseline_creates_oplog_tables(orch_ctx):
    """The shipped 0001 baseline creates operation_log and task_logs."""
    ch = get_ch_client()

    await ch_upgrade(ch)

    for table in ("operation_log", "task_logs"):
        exists = (await ch.query(f"EXISTS TABLE {table}")).result_rows
        assert exists[0][0] == 1, table
    assert "0001" in await ch_applied_versions(ch)


async def test_baseline_is_safe_on_existing_tables(orch_ctx):
    """Pre-existing tables (an install predating the framework) are untouched."""
    ch = get_ch_client()
    # Simulate an install that predates the framework: operation_log exists
    # (with a divergent shape) but schema_migrations does not.
    await ch.command("DROP TABLE IF EXISTS operation_log")
    await ch.command("DROP TABLE IF EXISTS schema_migrations")
    await ch.command("CREATE TABLE operation_log (id UInt64) ENGINE = Memory")
    await ch.command("INSERT INTO operation_log VALUES (7)")

    await ch_upgrade(ch)

    rows = (await ch.query("SELECT id FROM operation_log")).result_rows
    assert rows == [(7,)]
    assert "0001" in await ch_applied_versions(ch)
