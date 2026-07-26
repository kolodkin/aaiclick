"""Tests for the ClickHouse migration runner's pure file/SQL handling."""

from __future__ import annotations

from pathlib import Path

import pytest

from aaiclick.oplog.migrate import MigrationFile, list_migration_files, split_statements


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
