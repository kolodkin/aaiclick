"""Internal API for environment bootstrap — setup / migrate / ollama.

Unlike other ``internal_api`` modules these functions operate on
infrastructure (filesystem, embedded databases, external services) and do
not require an active ``orch_context()`` or ``data_context()``. They are
safe to call before any orchestration state exists.

Every function returns a pydantic view model; the CLI renderer handles
human output and the ``--json`` flag. Alembic subcommands still write their
own status to stdout via their internal logger — that output belongs to
alembic, not to this module.

``bootstrap_ollama`` lives in ``aaiclick.ai.ollama`` beside its probe and is
re-exported here so the CLI / REST / MCP surfaces keep one import path.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import NamedTuple

from alembic import command
from sqlalchemy import create_engine, insert, inspect, select
from sqlalchemy.engine import Connection, Engine, make_url

from aaiclick.ai.ollama import bootstrap_ollama, get_configured_model
from aaiclick.auth.models import Tenant
from aaiclick.backend import (
    get_ch_url,
    get_root,
    get_sql_url,
    is_chdb,
    is_local,
    is_sqlite,
)
from aaiclick.data.data_context.chdb_client import get_chdb_data_path, get_shared_session
from aaiclick.datetime_utils import utc_now
from aaiclick.oplog.migrate import ch_status_standalone, ch_upgrade_standalone
from aaiclick.orchestration.env import get_db_url
from aaiclick.orchestration.migrate import get_alembic_config
from aaiclick.orchestration.models import SQLModel
from aaiclick.tenancy import DEFAULT_TENANT_ID, DEFAULT_TENANT_SLUG
from aaiclick.view_models import (
    MIGRATE_CURRENT,
    MIGRATE_DOWNGRADE,
    MIGRATE_HEADS,
    MIGRATE_HISTORY,
    MIGRATE_SHOW,
    MIGRATE_UPGRADE,
    ChVersionStatus,
    MigrationAction,
    MigrationResult,
    OllamaBootstrapResult,
    SetupResult,
    SetupStep,
)

from .errors import Invalid


def _seed_default_tenant(engine: Engine) -> None:
    """Insert the default tenant (fixed id) if missing — idempotent re-run."""
    with engine.begin() as conn:
        exists = conn.execute(select(Tenant.id).where(Tenant.id == DEFAULT_TENANT_ID)).first()
        if exists is None:
            conn.execute(
                insert(Tenant).values(
                    id=DEFAULT_TENANT_ID, slug=DEFAULT_TENANT_SLUG, name=DEFAULT_TENANT_SLUG, created_at=utc_now()
                )
            )


def is_setup_done() -> bool:
    """Return True if the ``setup_done`` marker file exists under the root dir."""
    return (get_root() / "setup_done").exists()


def _sync_db_url() -> str | None:
    """Sync SQLAlchemy URL for the local SQLite DB, or None in Postgres mode."""
    if not is_sqlite():
        return None
    return get_db_url().replace("sqlite+aiosqlite", "sqlite")


def _local_db_path() -> Path | None:
    """Filesystem path of the local SQLite database, or None in Postgres mode."""
    sync_url = _sync_db_url()
    if sync_url is None:
        return None
    database = make_url(sync_url).database
    return Path(database) if database else None


@contextmanager
def _local_db_connection() -> Iterator[Connection | None]:
    """A connection to the local SQLite database, or ``None`` when there is
    none to inspect (Postgres mode, or a first run). One place owns the engine,
    so a caller asking several questions pays for a single open."""
    sync_url, db_path = _sync_db_url(), _local_db_path()
    if sync_url is None or db_path is None or not db_path.exists():
        yield None
        return
    engine = create_engine(sync_url)
    try:
        # One connection for the whole pass: an Inspector bound to the engine
        # checks a connection out again for every table it reflects.
        with engine.connect() as conn:
            yield conn
    finally:
        engine.dispose()


class _Shape(NamedTuple):
    """One table as the database materialises it, reduced to comparable values."""

    columns: dict[str, tuple[str, bool]]
    indexes: dict[str, tuple[tuple[str, ...], bool]]
    unique: dict[str, tuple[str, ...]]
    foreign_keys: dict[tuple[str, ...], tuple[str, tuple[str, ...]]]


def _shape(conn: Connection) -> dict[str, _Shape]:
    """Reflect every table into the comparable form above."""
    inspector = inspect(conn)
    return {
        table: _Shape(
            columns={c["name"]: (str(c["type"]), bool(c["nullable"])) for c in inspector.get_columns(table)},
            indexes={
                i["name"]: (tuple(i["column_names"]), bool(i.get("unique")))
                for i in inspector.get_indexes(table)
                if i["name"]
            },
            unique={u["name"]: tuple(u["column_names"]) for u in inspector.get_unique_constraints(table) if u["name"]},
            foreign_keys={
                tuple(f["constrained_columns"]): (f["referred_table"], tuple(f["referred_columns"]))
                for f in inspector.get_foreign_keys(table)
            },
        )
        for table in inspector.get_table_names()
    }


def _reference_shape() -> dict[str, _Shape]:
    """The shape ``setup`` would produce, from a throwaway database built now.

    Compared against a *database* rather than against ``SQLModel.metadata``
    because only the database says what the models actually materialise —
    column types as SQLite renders them, indexes, unique constraints, foreign
    keys. Both sides come from the same ``create_all`` on the same dialect, so
    reflection quirks appear on both and cancel; a metadata-to-reflection
    comparison has to special-case each of them instead.

    In memory rather than in a temp directory: same dialect and same DDL, four
    times quicker, and nothing to clean up if the caller raises.
    """
    engine = create_engine("sqlite://")
    try:
        SQLModel.metadata.create_all(engine)
        with engine.connect() as conn:
            return _shape(conn)
    finally:
        engine.dispose()


class LocalDbDrift(NamedTuple):
    """How a local database differs from the one ``setup`` would build now.

    Split because the two halves have different remedies: ``create_all`` adds a
    missing table without touching anything else, while a table that exists in
    the wrong shape can only be fixed by recreating the database — SQLite
    cannot ``ALTER`` its way there, which is why the revision chain is
    PostgreSQL-only (see "One migration chain, not two" in
    ``docs/designs/orchestration.md``).
    """

    missing_tables: list[str]
    mismatched: list[str]


def _drift(conn: Connection) -> LocalDbDrift:
    reference, actual = _reference_shape(), _shape(conn)
    missing_tables = sorted(set(reference) - set(actual))
    mismatched: list[str] = []
    for table in sorted(set(reference) & set(actual)):
        want, got = reference[table], actual[table]
        mismatched.extend(f"{table}.{name}" for name in want.columns if name not in got.columns)
        mismatched.extend(f"{table}.{name} (index)" for name in want.indexes if name not in got.indexes)
        mismatched.extend(f"{table}.{name} (unique)" for name in want.unique if name not in got.unique)
        mismatched.extend(
            f"{table}.{'+'.join(cols)} (foreign key)" for cols in want.foreign_keys if cols not in got.foreign_keys
        )
    return LocalDbDrift(missing_tables, mismatched)


def _old_default_tenant(conn: Connection) -> int | None:
    """The id of a default-tenant row predating the current ``DEFAULT_TENANT_ID``.

    The id moved once (to ``1 << 62``), and ``_seed_default_tenant`` looks the
    row up by id — so against an older database it inserts a second row and
    trips the ``slug`` unique constraint. The id is not just a key: it is
    written into every ``tenant_id`` column and into ClickHouse table names
    (``p_<tenant_id>_<name>``), so the remedy is to recreate, not to re-point
    the row.
    """
    if "tenants" not in inspect(conn).get_table_names():
        return None
    found = conn.execute(select(Tenant.id).where(Tenant.slug == DEFAULT_TENANT_SLUG)).scalar()
    return found if found is not None and found != DEFAULT_TENANT_ID else None


def stale_local_db() -> list[str]:
    """Everything an existing local table lacks, against a reference build.

    ``SQLModel.metadata.create_all`` only creates missing *tables* — it never
    alters one that already exists. A ``local.db`` written by an older version
    therefore gains any newly added table while its existing tables silently
    keep their original columns.

    Returns ``table.column`` names, plus ``(index)`` / ``(unique)`` /
    ``(foreign key)`` entries for the rest; empty when the database is current
    or absent.
    """
    with _local_db_connection() as conn:
        return [] if conn is None else _drift(conn).mismatched


_RECREATE_TAIL = (
    "SQLite databases are not migrated in place, so it has to be recreated. "
    "Local job/task history is lost; data objects in chdb are untouched."
)


def stale_local_db_reason(*, limit: int = 5) -> str | None:
    """Why the local SQLite database cannot be reused, or ``None`` when it can.

    The one question every caller asks before continuing against the database
    it found — shape (missing columns) and seeded content (a default tenant
    from before the id moved) answered over a single connection.
    """
    with _local_db_connection() as conn:
        if conn is None:
            return None
        mismatched = _drift(conn).mismatched
        detail = None
        if mismatched:
            shown = ", ".join(mismatched[:limit])
            if len(mismatched) > limit:
                shown += f" and {len(mismatched) - limit} more"
            detail = f"is missing {len(mismatched)} item(s) added since: {shown}"
        elif (old_id := _old_default_tenant(conn)) is not None:
            detail = f"carries default tenant {old_id}, not {DEFAULT_TENANT_ID}"
    if detail is None:
        return None
    return f"{_local_db_path()} was created by an older version of aaiclick: it {detail}. {_RECREATE_TAIL}"


def missing_local_tables() -> list[str]:
    """Model tables absent from an existing local SQLite DB.

    Separate from :func:`stale_local_db` because the remedy differs: these are
    added by a plain ``create_all``, whereas a table in the wrong shape needs
    the database recreated. ``is_setup_done`` only checks for a marker file, so
    a marker left beside an empty or truncated ``local.db`` (an interrupted
    setup, a wiped data dir) looked set up and every query failed with "no such
    table".

    Returns table names, empty when the database is current or absent.
    """
    with _local_db_connection() as conn:
        return [] if conn is None else _drift(conn).missing_tables


STALE_DB_REMEDY = "Re-run `aaiclick setup --force` to recreate it."
"""Remedy appended wherever an outdated local database blocks a command."""


def _reset_stale_local_db(*, force: bool) -> bool:
    """Delete the local SQLite DB when its schema predates the current models.

    Stale means either shape or seeded content is behind the models — see
    ``stale_local_db_reason``. Returns True when the database was removed.
    Raises ``Invalid`` when it is stale and ``force`` is not set, so no caller
    continues against a half-upgraded database.
    """
    db_path = _local_db_path()
    if db_path is None:
        return False
    reason = stale_local_db_reason()
    if reason is None:
        return False
    if not force:
        raise Invalid(f"{reason} {STALE_DB_REMEDY}")

    # -wal / -shm carry committed pages; leaving them beside a deleted DB
    # resurrects the old schema on the next connection.
    for suffix in ("", "-wal", "-shm"):
        db_path.with_name(db_path.name + suffix).unlink(missing_ok=True)
    return True


def setup(*, ai: bool = False, force: bool = False) -> SetupResult:
    """Initialize the local dev environment.

    Creates the chdb data directory (when using embedded chdb), applies
    ``SQLModel.metadata.create_all`` (when using SQLite), optionally pulls
    the configured Ollama model, and writes the ``setup_done`` marker file.

    Args:
        ai: Also pull the configured Ollama model.
        force: Delete and recreate the local SQLite database when its schema
            predates the current models. Without it such a database raises
            ``Invalid`` rather than being left half-upgraded. A database
            already at the current schema is never deleted, so a routine
            re-run with ``force`` set is not destructive.

    Returns a ``SetupResult`` whose ``steps`` describe each action taken —
    CLI rendering is the caller's responsibility.
    """
    root = get_root()
    steps: list[SetupStep] = []

    if is_chdb():
        chdb_path = get_chdb_data_path()
        Path(chdb_path).mkdir(parents=True, exist_ok=True)
        # chdb's Session is a per-process singleton: opening it here also
        # populates the shared cache so subsequent callers reuse the same handle.
        get_shared_session(chdb_path).query("SELECT 1")
        steps.append(SetupStep(name="chdb", status="ok", detail=chdb_path))
    else:
        steps.append(
            SetupStep(
                name="clickhouse",
                status="skipped",
                detail="remote server — requires pip install aaiclick[distributed]",
            )
        )

    if is_sqlite():
        db_url = get_db_url()
        recreated = _reset_stale_local_db(force=force)
        engine = create_engine(_sync_db_url() or db_url)
        SQLModel.metadata.create_all(engine)
        _seed_default_tenant(engine)
        engine.dispose()
        detail = f"{db_url} (recreated — schema predated this version)" if recreated else db_url
        steps.append(SetupStep(name="sqlite", status="ok", detail=detail))
    else:
        steps.append(
            SetupStep(
                name="postgres",
                status="skipped",
                detail="requires pip install aaiclick[distributed]; run migrations separately",
            )
        )

    ollama: OllamaBootstrapResult | None = None
    if ai:
        ollama = bootstrap_ollama(get_configured_model())

    Path(root).mkdir(parents=True, exist_ok=True)
    (root / "setup_done").write_text("")

    return SetupResult(
        root=str(root),
        ch_url=get_ch_url(),
        sql_url=get_sql_url(),
        mode="local" if is_local() else "distributed",
        steps=steps,
        ollama=ollama,
    )


def _ch_versions() -> list[ChVersionStatus]:
    return [ChVersionStatus(version=s.version, applied=s.applied) for s in ch_status_standalone()]


def migrate(action: MigrationAction, revision: str | None = None) -> MigrationResult:
    """Run a migration subcommand against both orchestration databases.

    ``UPGRADE`` runs the alembic upgrade and then applies pending ClickHouse
    migrations; ``CURRENT`` / ``HISTORY`` report both sides. ``DOWNGRADE`` /
    ``HEADS`` / ``SHOW`` are alembic-only (ClickHouse migrations are
    forward-only). ``UPGRADE`` defaults ``revision`` to ``"head"`` when
    omitted; ``DOWNGRADE`` and ``SHOW`` require an explicit revision. Alembic
    emits its own log output while running — this function returns a
    structured ``MigrationResult`` describing the invocation.
    """
    config = get_alembic_config()

    if action == MIGRATE_UPGRADE:
        target = revision or "head"
        command.upgrade(config, target)
        ch_applied = ch_upgrade_standalone()
        return MigrationResult(action=action, revision=target, ch_versions_applied=ch_applied)

    if action == MIGRATE_DOWNGRADE:
        if revision is None:
            raise Invalid("migrate downgrade requires a revision argument (e.g. '-1')")
        command.downgrade(config, revision)
        return MigrationResult(action=action, revision=revision)

    if action == MIGRATE_CURRENT:
        command.current(config, verbose=True)
        return MigrationResult(action=action, ch_versions=_ch_versions())

    if action == MIGRATE_HISTORY:
        command.history(config, verbose=True)
        return MigrationResult(action=action, ch_versions=_ch_versions())

    if action == MIGRATE_HEADS:
        command.heads(config, verbose=True)
        return MigrationResult(action=action)

    if action == MIGRATE_SHOW:
        if revision is None:
            raise Invalid("migrate show requires a revision argument")
        command.show(config, revision)
        return MigrationResult(action=action, revision=revision)

    raise Invalid(f"Unknown migrate action: {action}")
