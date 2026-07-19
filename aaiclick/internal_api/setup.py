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

from pathlib import Path

from alembic import command
from sqlalchemy import create_engine

from aaiclick.ai.ollama import bootstrap_ollama, get_configured_model
from aaiclick.backend import (
    get_ch_url,
    get_root,
    get_sql_url,
    is_chdb,
    is_local,
    is_sqlite,
)
from aaiclick.data.data_context.chdb_client import get_chdb_data_path, get_shared_session
from aaiclick.orchestration.env import get_db_url
from aaiclick.orchestration.migrate import get_alembic_config
from aaiclick.orchestration.models import SQLModel
from aaiclick.view_models import (
    MIGRATE_CURRENT,
    MIGRATE_DOWNGRADE,
    MIGRATE_HEADS,
    MIGRATE_HISTORY,
    MIGRATE_SHOW,
    MIGRATE_UPGRADE,
    MigrationAction,
    MigrationResult,
    OllamaBootstrapResult,
    SetupResult,
    SetupStep,
)

from .errors import Invalid


def is_setup_done() -> bool:
    """Return True if the ``setup_done`` marker file exists under the root dir."""
    return (get_root() / "setup_done").exists()


def setup(*, ai: bool = False) -> SetupResult:
    """Initialize the local dev environment.

    Creates the chdb data directory (when using embedded chdb), applies
    ``SQLModel.metadata.create_all`` (when using SQLite), optionally pulls
    the configured Ollama model, and writes the ``setup_done`` marker file.

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
        sync_url = db_url.replace("sqlite+aiosqlite", "sqlite")
        engine = create_engine(sync_url)
        SQLModel.metadata.create_all(engine)
        engine.dispose()
        steps.append(SetupStep(name="sqlite", status="ok", detail=db_url))
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


def migrate(action: MigrationAction, revision: str | None = None) -> MigrationResult:
    """Run an alembic subcommand against the orchestration database.

    ``UPGRADE`` defaults ``revision`` to ``"head"`` when omitted;
    ``DOWNGRADE`` and ``SHOW`` require an explicit revision. Alembic emits
    its own log output while running — this function returns a structured
    ``MigrationResult`` describing the invocation.
    """
    config = get_alembic_config()

    if action == MIGRATE_UPGRADE:
        target = revision or "head"
        command.upgrade(config, target)
        return MigrationResult(action=action, revision=target)

    if action == MIGRATE_DOWNGRADE:
        if revision is None:
            raise Invalid("migrate downgrade requires a revision argument (e.g. '-1')")
        command.downgrade(config, revision)
        return MigrationResult(action=action, revision=revision)

    if action == MIGRATE_CURRENT:
        command.current(config, verbose=True)
        return MigrationResult(action=action)

    if action == MIGRATE_HISTORY:
        command.history(config, verbose=True)
        return MigrationResult(action=action)

    if action == MIGRATE_HEADS:
        command.heads(config, verbose=True)
        return MigrationResult(action=action)

    if action == MIGRATE_SHOW:
        if revision is None:
            raise Invalid("migrate show requires a revision argument")
        command.show(config, revision)
        return MigrationResult(action=action, revision=revision)

    raise Invalid(f"Unknown migrate action: {action}")
