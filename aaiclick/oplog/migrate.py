"""
aaiclick.oplog.migrate - dbmate-inspired forward-only migration runner for
aaiclick's internal ClickHouse tables.

Migration scripts live in ``aaiclick/oplog/migrations/NNNN_description.sql``
and are applied in version order; applied versions are recorded in the
``schema_migrations`` ClickHouse table.
"""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path
from typing import NamedTuple

from aaiclick.data.data_context import ChClient
from aaiclick.data.data_context.ch_client import create_ch_client

logger = logging.getLogger(__name__)

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


SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    String,
    applied_at DateTime64(3)
) ENGINE = MergeTree ORDER BY version
"""


async def ch_applied_versions(ch_client: ChClient) -> list[str]:
    """Ensure ``schema_migrations`` exists and return applied versions, sorted."""
    await ch_client.command(SCHEMA_MIGRATIONS_DDL)
    result = await ch_client.query("SELECT version FROM schema_migrations ORDER BY version")
    return [row[0] for row in result.result_rows]


async def ch_pending(ch_client: ChClient, migrations_dir: Path = MIGRATIONS_DIR) -> list[str]:
    """Return versions present on disk but not yet applied, in apply order.

    Raises ``RuntimeError`` when the applied set is inconsistent with the
    files on disk: an applied version with no file, or an unapplied file
    older than an applied version (a gap — history was rewritten).
    """
    files = list_migration_files(migrations_dir)
    file_versions = [f.version for f in files]
    applied = await ch_applied_versions(ch_client)

    unknown = sorted(set(applied) - set(file_versions))
    if unknown:
        raise RuntimeError(
            f"schema_migrations contains versions with no migration file: {unknown}. "
            f"The installed code is older than the database schema."
        )

    pending = [v for v in file_versions if v not in set(applied)]
    if applied and pending:
        newest_applied = applied[-1]
        gaps = [v for v in pending if v < newest_applied]
        if gaps:
            raise RuntimeError(
                f"Migration gap: versions {gaps} are unapplied but older than "
                f"already-applied {newest_applied}. Renumber the new migration(s) past {newest_applied}."
            )
    return pending


async def ch_upgrade(
    ch_client: ChClient,
    migrations_dir: Path = MIGRATIONS_DIR,
    dry_run: bool = False,
) -> list[str]:
    """Apply pending migrations in order; return the versions applied.

    With ``dry_run=True``, log each pending script's statements and return
    the pending versions without executing anything.

    ClickHouse has no DDL transactions: a mid-script failure leaves the
    version unrecorded, and re-running the (idempotent) script after fixing
    it completes the migration.
    """
    pending = await ch_pending(ch_client, migrations_dir)
    files_by_version = {f.version: f for f in list_migration_files(migrations_dir)}

    for version in pending:
        migration = files_by_version[version]
        statements = split_statements(migration.path.read_text())
        if dry_run:
            logger.info("[dry-run] %s would execute %d statement(s):", migration.path.name, len(statements))
            for stmt in statements:
                logger.info("[dry-run] %s", stmt)
            continue
        logger.info("Applying CH migration %s", migration.path.name)
        for stmt in statements:
            await ch_client.command(stmt)
        await ch_client.command(
            "INSERT INTO schema_migrations (version, applied_at) VALUES ({version:String}, now64(3))",
            parameters={"version": version},
        )
    return pending


class ChVersionState(NamedTuple):
    version: str
    applied: bool


async def _ch_upgrade_with_client(dry_run: bool) -> list[str]:
    ch_client = await create_ch_client()
    try:
        return await ch_upgrade(ch_client, dry_run=dry_run)
    finally:
        await ch_client.close()


async def _ch_status_with_client() -> list[ChVersionState]:
    ch_client = await create_ch_client()
    try:
        applied = set(await ch_applied_versions(ch_client))
    finally:
        await ch_client.close()
    return [
        ChVersionState(version=f.version, applied=f.version in applied)
        for f in list_migration_files(MIGRATIONS_DIR)
    ]


def ch_upgrade_standalone(dry_run: bool = False) -> list[str]:
    """Sync entry for ``aaiclick migrate``: own client, own event loop."""
    return asyncio.run(_ch_upgrade_with_client(dry_run))


def ch_status_standalone() -> list[ChVersionState]:
    """Sync entry for ``aaiclick migrate current/history``."""
    return asyncio.run(_ch_status_with_client())
