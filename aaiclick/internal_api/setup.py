"""Internal API for environment bootstrap — setup / migrate / ollama.

Unlike other ``internal_api`` modules these functions operate on
infrastructure (filesystem, embedded databases, external services) and do
not require an active ``orch_context()`` or ``data_context()``. They are
safe to call before any orchestration state exists.

Every function returns a pydantic view model; the CLI renderer handles
human output and the ``--json`` flag. Alembic subcommands still write their
own status to stdout via their internal logger — that output belongs to
alembic, not to this module.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from pathlib import Path

from alembic import command
from chdb.session import Session
from sqlalchemy import create_engine

from aaiclick.backend import (
    get_ch_url,
    get_root,
    get_sql_url,
    is_chdb,
    is_local,
    is_sqlite,
)
from aaiclick.data.data_context.chdb_client import get_chdb_data_path
from aaiclick.orchestration.env import get_db_url
from aaiclick.orchestration.migrate import get_alembic_config
from aaiclick.orchestration.models import SQLModel
from aaiclick.view_models import (
    MigrationAction,
    MigrationResult,
    OllamaBootstrapResult,
    OllamaBootstrapStatus,
    SetupResult,
    SetupStep,
)

from .errors import Invalid

OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_PING_TIMEOUT_S = 2
OLLAMA_SHOW_TIMEOUT_S = 5
OLLAMA_PULL_TIMEOUT_S = 600
DEFAULT_OLLAMA_MODEL = "ollama/llama3.1:8b"


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
        sess = Session(chdb_path)
        sess.query("SELECT 1")
        sess.cleanup()
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
        model = os.environ.get("AAICLICK_AI_MODEL", DEFAULT_OLLAMA_MODEL)
        ollama = bootstrap_ollama(model)
        steps.append(SetupStep(name="ollama", status=_ollama_step_status(ollama), detail=ollama.detail or ollama.model))

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


def _ollama_step_status(result: OllamaBootstrapResult) -> str:
    """Translate an ``OllamaBootstrapStatus`` to a ``SetupStep`` status."""
    if result.status in (OllamaBootstrapStatus.ALREADY_PRESENT, OllamaBootstrapStatus.PULLED):
        return "ok"
    if result.status == OllamaBootstrapStatus.NOT_OLLAMA:
        return "skipped"
    return "failed"


def bootstrap_ollama(model: str, *, base_url: str = OLLAMA_BASE_URL) -> OllamaBootstrapResult:
    """Ensure the named Ollama model is available locally.

    Non-Ollama models (anything not prefixed ``ollama/``) short-circuit to
    ``NOT_OLLAMA``. If the server is unreachable, returns
    ``SERVER_UNREACHABLE``; if the model is already downloaded, returns
    ``ALREADY_PRESENT``; otherwise triggers a pull and returns ``PULLED``.
    """
    if not model.startswith("ollama/"):
        return OllamaBootstrapResult(
            model=model,
            server_url=base_url,
            status=OllamaBootstrapStatus.NOT_OLLAMA,
            detail="not an Ollama model — nothing to pull",
        )

    model_name = model.removeprefix("ollama/")

    try:
        urllib.request.urlopen(base_url, timeout=OLLAMA_PING_TIMEOUT_S)  # noqa: S310
    except (urllib.error.URLError, OSError) as exc:
        return OllamaBootstrapResult(
            model=model,
            server_url=base_url,
            status=OllamaBootstrapStatus.SERVER_UNREACHABLE,
            detail=f"ollama server not reachable: {exc}",
        )

    show_req = urllib.request.Request(  # noqa: S310
        f"{base_url}/api/show",
        data=json.dumps({"model": model_name}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(show_req, timeout=OLLAMA_SHOW_TIMEOUT_S)  # noqa: S310
        return OllamaBootstrapResult(
            model=model,
            server_url=base_url,
            status=OllamaBootstrapStatus.ALREADY_PRESENT,
            detail=f"model '{model_name}' already downloaded",
        )
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise

    pull_req = urllib.request.Request(  # noqa: S310
        f"{base_url}/api/pull",
        data=json.dumps({"model": model_name, "stream": False}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(pull_req, timeout=OLLAMA_PULL_TIMEOUT_S) as resp:  # noqa: S310
            payload = json.loads(resp.read())
    except (urllib.error.URLError, OSError) as exc:
        return OllamaBootstrapResult(
            model=model,
            server_url=base_url,
            status=OllamaBootstrapStatus.FAILED,
            detail=f"pull failed: {exc}",
        )

    if payload.get("status") == "success":
        return OllamaBootstrapResult(
            model=model,
            server_url=base_url,
            status=OllamaBootstrapStatus.PULLED,
            detail=f"model '{model_name}' pulled",
        )
    return OllamaBootstrapResult(
        model=model,
        server_url=base_url,
        status=OllamaBootstrapStatus.FAILED,
        detail=f"unexpected pull response: {payload}",
    )


def migrate(action: MigrationAction, revision: str | None = None) -> MigrationResult:
    """Run an alembic subcommand against the orchestration database.

    ``UPGRADE`` defaults ``revision`` to ``"head"`` when omitted;
    ``DOWNGRADE`` and ``SHOW`` require an explicit revision. Alembic emits
    its own log output while running — this function returns a structured
    ``MigrationResult`` describing the invocation.
    """
    config = get_alembic_config()

    if action == MigrationAction.UPGRADE:
        target = revision or "head"
        command.upgrade(config, target)
        return MigrationResult(action=action, revision=target)

    if action == MigrationAction.DOWNGRADE:
        if revision is None:
            raise Invalid("migrate downgrade requires a revision argument (e.g. '-1')")
        command.downgrade(config, revision)
        return MigrationResult(action=action, revision=revision)

    if action == MigrationAction.CURRENT:
        command.current(config, verbose=True)
        return MigrationResult(action=action)

    if action == MigrationAction.HISTORY:
        command.history(config, verbose=True)
        return MigrationResult(action=action)

    if action == MigrationAction.HEADS:
        command.heads(config, verbose=True)
        return MigrationResult(action=action)

    if action == MigrationAction.SHOW:
        if revision is None:
            raise Invalid("migrate show requires a revision argument")
        command.show(config, revision)
        return MigrationResult(action=action, revision=revision)

    raise Invalid(f"Unknown migrate action: {action}")
