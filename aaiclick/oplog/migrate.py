"""
aaiclick.oplog.migrate - dbmate-inspired forward-only migration runner for
aaiclick's internal ClickHouse tables.

Migration scripts live in ``aaiclick/oplog/migrations/NNNN_description.sql``
and are applied in version order; applied versions are recorded in the
``schema_migrations`` ClickHouse table. See ``docs/designs/ch_migrations.md``.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import NamedTuple

MIGRATIONS_DIR = Path(__file__).parent / "migrations"

_FILENAME_RE = re.compile(r"^(\d{4})_[a-z0-9_]+\.sql$")
_COMMENT_RE = re.compile(r"--.*$", re.MULTILINE)


class MigrationFile(NamedTuple):
    version: str
    path: Path


def list_migration_files(migrations_dir: Path) -> list[MigrationFile]:
    """Return migration files sorted by version.

    Raises ``ValueError`` for files not named ``NNNN_description.sql`` and
    for duplicate ``NNNN`` versions.
    """
    files: list[MigrationFile] = []
    for path in sorted(migrations_dir.glob("*.sql")):
        match = _FILENAME_RE.match(path.name)
        if match is None:
            raise ValueError(f"Migration file {path.name} does not match NNNN_description.sql")
        files.append(MigrationFile(version=match.group(1), path=path))

    versions = [f.version for f in files]
    if len(versions) != len(set(versions)):
        raise ValueError(f"duplicate migration versions in {migrations_dir}: {versions}")
    return files


def split_statements(sql: str) -> list[str]:
    """Split a migration script into statements on ``;`` at end of line.

    ``--`` line comments are stripped first, so a semicolon inside a comment
    never splits. Semicolons inside string literals are not supported —
    internal DDL scripts don't need them.
    """
    without_comments = "\n".join(line.rstrip() for line in _COMMENT_RE.sub("", sql).splitlines())
    statements = re.split(r";\s*$", without_comments, flags=re.MULTILINE)
    return [stmt.strip().rstrip(";").strip() for stmt in statements if stmt.strip()]
