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


def _missing_columns(conn: Connection) -> list[str]:
    """``table.column`` for every model column absent from an existing table."""
    inspector = inspect(conn)
    existing = set(inspector.get_table_names())
    missing: list[str] = []
    for table in SQLModel.metadata.sorted_tables:
        if table.name not in existing:
            continue
        present = {col["name"] for col in inspector.get_columns(table.name)}
        missing.extend(f"{table.name}.{col.name}" for col in table.columns if col.name not in present)
    return missing


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
    """Model columns missing from the tables already in the local SQLite DB.

    ``SQLModel.metadata.create_all`` only creates missing *tables* — it never
    alters one that already exists. A ``local.db`` written by an older version
    therefore gains any newly added table while its existing tables silently
    keep their original columns. Alembic cannot migrate it either: the
    revision chain is authored for PostgreSQL (non-batch ``create_foreign_key``,
    ``ALTER`` of constraints) and raises ``NotImplementedError`` on SQLite.

    Returns ``table.column`` names, empty when the database is current or
    absent.
    """
    with _local_db_connection() as conn:
        return [] if conn is None else _missing_columns(conn)


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
        columns = _missing_columns(conn)
        detail = None
        if columns:
            shown = ", ".join(columns[:limit])
            if len(columns) > limit:
                shown += f" and {len(columns) - limit} more"
            detail = f"is missing {len(columns)} column(s) added since: {shown}"
        elif (old_id := _old_default_tenant(conn)) is not None:
            detail = f"carries default tenant {old_id}, not {DEFAULT_TENANT_ID}"
    if detail is None:
        return None
    return f"{_local_db_path()} was created by an older version of aaiclick: it {detail}. {_RECREATE_TAIL}"


def missing_local_tables() -> list[str]:
    """Model tables absent from an existing local SQLite DB.

    ``is_setup_done`` only checks for a marker file, and :func:`stale_local_db`
    compares columns of tables that *exist* — it skips missing ones, so it
    reports a database with no tables at all as current. A marker left beside
    an empty or truncated ``local.db`` (an interrupted setup, a wiped data dir)
    therefore looked set up, and every query failed with "no such table".

    Returns table names, empty when the database is current or absent.
    """
    sync_url = _sync_db_url()
    db_path = _local_db_path()
    if sync_url is None or db_path is None or not db_path.exists():
        return []
    engine = create_engine(sync_url)
    try:
        with engine.connect() as conn:
            existing = set(inspect(conn).get_table_names())
        return [t.name for t in SQLModel.metadata.sorted_tables if t.name not in existing]
    finally:
        engine.dispose()


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
