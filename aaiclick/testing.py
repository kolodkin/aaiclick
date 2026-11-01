"""Shared test helpers and fixtures.

``worker_databases()`` gives each pytest process its own CH and SQL databases;
conftests wrap it in their own autouse session fixture. Per-subpackage
conftests import the pytest fixtures defined here (``orch_module_ctx``,
``orch_module_ctx_no_ch``, ``orch_ctx``, ``orch_ctx_no_ch``). pytest
recognises imported fixtures by identity, so the same fixture re-exported
from multiple conftests still runs once per scope. Keeping the
implementations here avoids copy-paste across
``aaiclick/data/conftest.py``, ``aaiclick/orchestration/conftest.py``,
and ``aaiclick/oplog/conftest.py``.
"""

from __future__ import annotations

import asyncio
import gc
import os
import shutil
import subprocess
import sys
import tempfile
from collections import Counter
from collections.abc import AsyncIterator, Iterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager, contextmanager, nullcontext
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple
from unittest.mock import patch

import pytest
from alembic import command
from sqlalchemy import create_engine, text
from sqlmodel import col, select

from aaiclick.__main__ import main
from aaiclick.backend import get_root, is_chdb, is_local, is_sqlite, parse_ch_url
from aaiclick.data.models import FIELDTYPE_ARRAY, ColumnInfo, Schema
from aaiclick.data.object_context import ChClient, get_ch_client
from aaiclick.oplog.migrate import ch_applied_versions, ch_upgrade, ch_upgrade_standalone
from aaiclick.oplog.models import clear_schema_cache
from aaiclick.orchestration.migrate import get_alembic_config
from aaiclick.orchestration.models import TASK_COMPLETED, TASK_RUNNING, JobStatus, SQLModel, Task, TaskRun, TaskStatus
from aaiclick.orchestration.orch_context import get_sql_session, orch_context, task_scope
from aaiclick.orchestration.view_models import JobStatsView, TaskStatsView
from aaiclick.snowflake import get_snowflake_id

from .datetime_utils import utc_now

_BASE_SQL_DB = os.environ.get("POSTGRES_DB", "aaiclick")

# Detection harness: when set, force a full GC at every test boundary so
# aiohttp connection leaks surface as deterministic failures (see
# gc_leak_check below and docs/designs/technical_debt.md). The nightly dist job sets
# it; the per-PR suite leaves it off because the per-test collect is slow.
_GC_LEAK_CHECK = bool(os.environ.get("AAICLICK_GC_LEAK_CHECK"))


@pytest.fixture(autouse=_GC_LEAK_CHECK)
def gc_leak_check():
    """Force ``gc.collect()`` in teardown to attribute connection leaks to a test.

    clickhouse-connect's async client can leave aiohttp ``Connection``
    objects in a refcycle whose ``__del__`` emits ``ResourceWarning:
    Unclosed connection`` only under cyclic GC, in non-deterministic
    ``__del__`` order. Natural GC fires that warning at an arbitrary later
    point, so under ``filterwarnings = ["error"]`` it lands on a random test
    or escapes the run entirely. Collecting in teardown pulls the collection
    into the current test's reporting scope: across a full suite that is
    hundreds of forced collections, driving the odds of never hitting the
    leak to near zero and pinning the failure to a specific test.

    Autouse is gated on ``AAICLICK_GC_LEAK_CHECK`` so the per-PR suite pays
    nothing; the nightly dist run enables it alongside ``PYTHONASYNCIODEBUG=1``
    (which adds the leaking response's source traceback).
    """
    yield
    gc.collect()


# ---------------------------------------------------------------------------
# Pure helpers — used inside fixture bodies and optionally by ad-hoc tests.
# ---------------------------------------------------------------------------


async def reset_sql_tables() -> None:
    """Delete rows from every user table in the active SQL database.

    Uses ``DELETE FROM`` on SQLite and ``TRUNCATE ... RESTART IDENTITY
    CASCADE`` on Postgres. Alembic's ``alembic_version`` is preserved so
    migrations don't have to re-run between tests.
    """
    async with get_sql_session() as session:
        if is_sqlite():
            result = await session.execute(
                text("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
            )
            present = {r[0] for r in result.all()} - {"alembic_version"}
            # Children before parents — SQLite checks FKs on every DELETE.
            ordered = [t.name for t in reversed(SQLModel.metadata.sorted_tables) if t.name in present]
            for name in [*present.difference(ordered), *ordered]:
                await session.execute(text(f'DELETE FROM "{name}"'))
        else:
            result = await session.execute(
                text("SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename != 'alembic_version'")
            )
            tables = [r[0] for r in result.all()]
            if tables:
                quoted = ", ".join(f'"{t}"' for t in tables)
                await session.execute(text(f"TRUNCATE {quoted} RESTART IDENTITY CASCADE"))
        await session.commit()


async def ch_table_ddl(ch: ChClient) -> dict[str, str]:
    """``create_table_query`` of each table in the active CH database, by name."""
    result = await ch.query("SELECT name, create_table_query FROM system.tables WHERE database = currentDatabase()")
    return {row[0]: row[1] for row in result.result_rows}


async def list_ch_tables(ch: ChClient) -> set[str]:
    """Names of the tables in the active CH database."""
    return set(await ch_table_ddl(ch))


async def create_ch_tables(ch: ChClient, *names: str) -> None:
    """Create a throwaway single-column ``Memory`` table for each name."""
    for name in names:
        await ch.command(f"CREATE TABLE {name} (x UInt8) ENGINE = Memory")


@dataclass
class _MigratedSchema:
    """The CH database as ``ch_upgrade`` leaves an empty one."""

    tables: dict[str, str] = field(default_factory=dict)  # name -> create_table_query
    versions: list[str] | None = None  # None until the first full rebuild


# Process-wide rather than a ContextVar: each test's reset runs in its own
# context yet must reuse the snapshot an earlier reset took.
_migrated_schema = _MigratedSchema()


async def _matches_migrated_schema(ch: ChClient, tables: dict[str, str]) -> bool:
    if _migrated_schema.versions is None:
        return False
    if any(tables.get(name) != ddl for name, ddl in _migrated_schema.tables.items()):
        return False
    # The DDL matched, so schema_migrations exists — skip ch_applied_versions' CREATE.
    result = await ch.query("SELECT version FROM schema_migrations ORDER BY version")
    return [row[0] for row in result.result_rows] == _migrated_schema.versions


async def drop_all_ch_tables() -> None:
    """Reset the active CH database to its freshly migrated, empty state.

    Truncates the migration-created tables while they match the last full
    rebuild; otherwise drops everything and re-applies migrations (see
    ``docs/designs/testing.md``).
    """
    ch = get_ch_client()
    tables = await ch_table_ddl(ch)
    keep = _migrated_schema.tables.keys() if await _matches_migrated_schema(ch, tables) else set()
    if dropped := tables.keys() - keep:
        await ch.command("DROP TABLE IF EXISTS " + ", ".join(f"`{name}`" for name in dropped))
    for table_name in keep - {"schema_migrations"}:
        await ch.command(f"TRUNCATE TABLE `{table_name}`")
    if not keep:
        await ch_upgrade(ch)
        _migrated_schema.tables = await ch_table_ddl(ch)
        _migrated_schema.versions = await ch_applied_versions(ch)
    # Column types cached from the dropped tables must not leak into the
    # next test (which may recreate them with a different shape).
    clear_schema_cache()


async def per_test_reset(*, reset_ch: bool = True, reset_sql: bool = True) -> None:
    """Wipe CH and/or SQL state between tests sharing a module orch scope.

    Must be called while the enclosing orch context is active.
    """
    if reset_ch:
        await drop_all_ch_tables()
    if reset_sql:
        await reset_sql_tables()


@asynccontextmanager
async def module_orch_scope(ctx: AbstractAsyncContextManager[None]) -> AsyncIterator[None]:
    """Enter an orch context once for the lifetime of a module-scoped fixture."""
    async with ctx:
        yield


@asynccontextmanager
async def reset_test_state(
    ctx: AbstractAsyncContextManager[None],
    *,
    reset_ch: bool = True,
    reset_sql: bool = False,
) -> AsyncIterator[None]:
    """Enter ``ctx``, reset the requested backend state, then yield.

    Function-scoped alternative to ``module_orch_scope`` + ``per_test_reset``
    — kept for tests/fixtures that genuinely need their own orch context
    per function (e.g. the ``data/conftest.py`` ``ctx`` fixture).
    """
    async with ctx:
        await per_test_reset(reset_ch=reset_ch, reset_sql=reset_sql)
        yield


async def seed_registry_row(table: str, *, fieldtype: str = FIELDTYPE_ARRAY) -> None:
    """Insert (or upsert) a minimal ``table_registry.schema_doc`` row.

    Lets tests point Object / View machinery at arbitrary table names
    without going through ``create_object``. The emitted ``schema_doc``
    declares a single ``value`` column inheriting ``fieldtype``.
    """
    schema_doc = Schema(
        fieldtype=fieldtype,
        columns={"value": ColumnInfo("Int64", fieldtype=fieldtype)},
        engine="MergeTree",
    ).model_dump_json()
    async with get_sql_session() as sess:
        await sess.execute(
            text(
                "INSERT INTO table_registry "
                "(table_name, created_at, schema_doc) "
                "VALUES (:t, :now, :sd) "
                "ON CONFLICT (table_name) DO NOTHING"
            ),
            {"t": table, "now": utc_now(), "sd": schema_doc},
        )
        await sess.commit()


async def set_task_image_source(job_id: int, image_source: dict) -> None:
    """Stamp ``image_source`` onto a single-task job's task, as a container
    submission would, so in-process executors can be exercised against it."""
    async with get_sql_session() as session:
        task = (await session.execute(select(Task).where(Task.job_id == job_id))).scalar_one()
        task.image_source = image_source
        session.add(task)
        await session.commit()


async def run_cli(*argv: str) -> None:
    """Run ``main()`` with ``argv`` against the test database.

    ``main()`` calls ``asyncio.run``, so it cannot share the test's running
    loop. ``run_in_executor`` gives it a thread with an empty context, so the
    CLI opens its own ``orch_context`` on the database ``orch_ctx`` just reset.
    """
    with patch("sys.argv", ["aaiclick", *argv]):
        await asyncio.get_running_loop().run_in_executor(None, main)


def make_job_stats(job_status: JobStatus, tasks: list[TaskStatsView]) -> JobStatsView:
    """A ``JobStatsView`` whose ``status_counts`` and ``total_tasks`` match ``tasks``."""
    return JobStatsView(
        job_id=1,
        job_name="j",
        job_status=job_status,
        total_tasks=len(tasks),
        status_counts=dict(Counter(t.status for t in tasks)),
        tasks=tasks,
    )


def _pg_connect(dbname: str):
    """Connect to PostgreSQL with environment-based credentials."""
    # Distributed extra only — keep inline so local-only installs can import this module.
    import psycopg2  # noqa: PLC0415

    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ.get("POSTGRES_USER", "aaiclick"),
        password=os.environ.get("POSTGRES_PASSWORD", "secret"),
        dbname=dbname,
    )


# ---------------------------------------------------------------------------
# Shared pytest fixtures — imported into per-subpackage conftests.
# ---------------------------------------------------------------------------


@contextmanager
def worker_ch_database() -> Iterator[None]:
    """Per-process CH isolation — ``<root>/chdb_data`` for chdb, a
    ``default_<worker>`` database for a real server.

    Without this, the per-test ``DROP TABLE`` sweep would cross worker
    boundaries in real-CH CI jobs. Both are migrated before yielding, for
    processes that need the schema at startup (the web e2e server and
    workers). chdb is migrated in a throwaway subprocess so the parent holds
    no session until a test needs one — except in local mode, where
    ``init_oplog_tables`` migrates on first use.
    """
    if is_chdb():
        with pytest.MonkeyPatch.context() as mp:
            mp.setenv("AAICLICK_CH_URL", f"chdb://{get_root() / 'chdb_data'}")
            if not is_local():
                _ch_upgrade_in_subprocess()
            yield
        return

    # Distributed extra only — keep inline so local-only installs can import this module.
    import clickhouse_connect  # noqa: PLC0415

    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    if not worker:
        ch_upgrade_standalone()
        yield
        return

    params = parse_ch_url()
    base_db = params["database"]
    db_name = f"{base_db}_{worker}"

    def _admin():
        return clickhouse_connect.get_client(
            host=params["host"],
            port=params["port"],
            username=params["username"],
            password=params["password"],
            database=base_db,
        )

    admin = _admin()
    admin.command(f"DROP DATABASE IF EXISTS `{db_name}`")
    admin.command(f"CREATE DATABASE `{db_name}`")
    admin.close()

    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("AAICLICK_CH_URL", os.environ["AAICLICK_CH_URL"].rsplit("/", 1)[0] + f"/{db_name}")
        ch_upgrade_standalone()
        try:
            yield
        finally:
            admin = _admin()
            admin.command(f"DROP DATABASE IF EXISTS `{db_name}`")
            admin.close()


@contextmanager
def worker_sql_database() -> Iterator[None]:
    """Per-process SQL isolation — ``<root>/local.db`` for SQLite, a
    ``<base>_<worker>`` database for Postgres.

    SQLite: schema created once via ``SQLModel.metadata.create_all``;
    per-test cleanup is a ``DELETE FROM`` sweep in ``reset_sql_tables``.
    Postgres: migrated once via Alembic; per-test cleanup is
    ``TRUNCATE ... RESTART IDENTITY CASCADE``.
    """
    if is_sqlite():
        db_path = get_root() / "local.db"
        _create_sqlite_schema(db_path)
        with pytest.MonkeyPatch.context() as mp:
            mp.setenv("AAICLICK_SQL_URL", f"sqlite+aiosqlite:///{db_path}")
            yield
        return

    # Distributed extra only — keep inline so local-only installs can import this module.
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT  # noqa: PLC0415

    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    if not worker:
        config = get_alembic_config()
        command.upgrade(config, "head")
        yield
        return

    db_name = f"{_BASE_SQL_DB}_{worker}"

    # Postgres forbids CREATE/DROP DATABASE inside a transaction; psycopg2
    # wraps every statement in one by default. Autocommit disables the
    # wrapping for this connection, which is used only for that DDL.
    conn = _pg_connect("postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
    cur.execute(f'CREATE DATABASE "{db_name}"')
    cur.close()
    conn.close()

    with pytest.MonkeyPatch.context() as mp:
        sql_url = os.environ.get("AAICLICK_SQL_URL")
        if sql_url and "postgresql" in sql_url:
            mp.setenv("AAICLICK_SQL_URL", f"{sql_url.rsplit('/', 1)[0]}/{db_name}")
        config = get_alembic_config()
        command.upgrade(config, "head")
        try:
            yield
        finally:
            conn = _pg_connect("postgres")
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
            cur.close()
            conn.close()


def _worker_tempdir(kind: str) -> tempfile.TemporaryDirectory[str]:
    """A throwaway directory named after this pytest process. Removal errors
    are ignored: chdb's process-wide Session may still hold it at teardown."""
    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    return tempfile.TemporaryDirectory(prefix=f"aaiclick_{kind}_{worker or 'main'}_", ignore_cleanup_errors=True)


def _create_sqlite_schema(db_path: Path) -> None:
    """Create the orchestration schema once; per-test cleanup is a ``DELETE FROM`` sweep."""
    engine = create_engine(f"sqlite:///{db_path}")
    SQLModel.metadata.create_all(engine)
    engine.dispose()


def _ch_upgrade_in_subprocess() -> None:
    """Migrate the chdb directory ``AAICLICK_CH_URL`` points at in a throwaway
    process: chdb allows one data path per process, so doing it here would pin
    the parent's Session to that directory."""
    script = "from aaiclick.oplog.migrate import ch_upgrade_standalone; ch_upgrade_standalone()"
    subprocess.run([sys.executable, "-c", script], check=True)


@contextmanager
def worker_databases() -> Iterator[None]:
    """This pytest process's own CH and SQL databases until exit.

    ``AAICLICK_LOCAL_ROOT`` is pinned to a fresh tempdir, so file-backed
    backends (SQLite, chdb) live under it in ``local start``'s layout and a
    run never opens ``~/.aaiclick`` — which a running ``local start`` holds
    the chdb lock on — with or without xdist. Server-backed backends
    (Postgres, ClickHouse) get a per-xdist-worker database.
    """
    with _worker_tempdir("root") as root, pytest.MonkeyPatch.context() as mp:
        mp.setenv("AAICLICK_LOCAL_ROOT", root)
        with worker_ch_database(), worker_sql_database():
            yield


@pytest.fixture(scope="module")
async def orch_module_ctx():
    """Module-scoped ``orch_context()`` with chdb — entered once per module.

    chdb's Session is a true per-process singleton (see
    ``docs/designs/testing.md``); orch_context never closes it, so this
    fixture's scope only affects orchestration state, not chdb lifecycle.
    """
    async with module_orch_scope(orch_context()):
        yield


@contextmanager
def _mp_chdb_dir() -> Iterator[None]:
    """A migrated per-module chdb tempdir that ``AAICLICK_CH_URL`` points at.

    Migrated up front: with a non-SQLite SQL backend the child's
    ``init_oplog_tables`` only checks for pending migrations.
    """
    with _worker_tempdir("chdb_mp") as tmp_dir, pytest.MonkeyPatch.context() as mp:
        mp.setenv("AAICLICK_CH_URL", f"chdb://{tmp_dir}")
        _ch_upgrade_in_subprocess()
        yield


@pytest.fixture(scope="module")
async def orch_module_ctx_no_ch():
    """Module-scoped ``orch_context()`` — entered once per module.

    Used by test modules whose tests spawn multiprocessing workers: the
    parent never opens chdb, so the child can acquire the chdb file lock.
    Must live in a dedicated module (no ``orch_ctx`` alongside) so the
    module-scoped chdb session doesn't collide.

    Under chdb, redirects ``AAICLICK_CH_URL`` to a per-module tempdir
    (``_mp_chdb_dir``) so mp-worker children open a fresh chdb file that no
    other module's session holds a lock on.
    """
    with _mp_chdb_dir() if is_chdb() else nullcontext():
        async with module_orch_scope(orch_context()):
            yield


@pytest.fixture
async def orch_ctx(orch_module_ctx):
    """Per-test state reset + synthetic task_scope on top of the module-scoped
    chdb orch context.

    The task_scope activates OrchLifecycleHandler so ``create_object`` can
    write to ``table_registry.schema_doc`` — required by Phase 2's
    registry-backed ``_get_table_schema`` read path.
    """
    await per_test_reset(reset_ch=True, reset_sql=True)
    synthetic_id = get_snowflake_id()
    async with task_scope(task_id=synthetic_id, job_id=synthetic_id, run_id=synthetic_id):
        yield


@pytest.fixture(scope="module")
async def orch_shared_ctx(orch_module_ctx):
    """Module-scoped orch context, reset once before the module's first test.

    For modules whose tests only assert on state built once by a
    module-scoped setup fixture (a job run, seeded objects). Must not be
    mixed with ``orch_ctx`` in one module: its per-test reset would wipe
    the shared state.
    """
    await per_test_reset(reset_ch=True, reset_sql=True)
    yield


@pytest.fixture(scope="module")
async def orch_shared_ctx_no_ch(orch_module_ctx_no_ch):
    """``orch_shared_ctx`` for mp-worker modules: the child owns chdb, so only SQL is reset."""
    await per_test_reset(reset_ch=False, reset_sql=True)
    yield


@pytest.fixture
async def orch_ctx_no_ch(orch_module_ctx_no_ch):
    """Per-test state reset on top of the module-scoped no-ch orch context.

    For tests where the child process owns chdb (e.g. multiprocessing
    worker). Must be used only in modules that never also use
    ``orch_ctx`` — mixing the two within one module would require two
    conflicting module-scoped orch contexts.
    """
    await per_test_reset(reset_ch=False, reset_sql=True)
    yield


class GitDaemon(NamedTuple):
    """The CI ``git daemon`` the runner e2es publish repos into."""

    base: Path
    port: str

    def url(self, name: str) -> str:
        return f"git://127.0.0.1:{self.port}/{name}"


def _git_daemon() -> GitDaemon:
    """The daemon from ``AAICLICK_E2E_GIT_DAEMON_BASE`` / ``_PORT``; skips the
    test when unset — these e2es are workflow-driven."""
    base = os.environ.get("AAICLICK_E2E_GIT_DAEMON_BASE")
    port = os.environ.get("AAICLICK_E2E_GIT_DAEMON_PORT")
    if not base or not port:
        pytest.skip("git daemon not configured; this runner e2e is workflow-driven")
    return GitDaemon(Path(base), port)


def init_bare_repo(path: Path, branch: str = "main") -> Path:
    """An empty bare repo at ``path`` whose default branch is ``branch``."""
    subprocess.run(["git", "init", "-q", "--bare", "-b", branch, str(path)], check=True)
    return path


def _allow_any_sha(bare: Path) -> None:
    # The build fetches a raw SHA over the smart transport; upload-pack rejects
    # that unless the serving repo opts in.
    subprocess.run(["git", "-C", str(bare), "config", "uploadpack.allowAnySHA1InWant", "true"], check=True)


class UserRepo(NamedTuple):
    """A fixture repo published into the CI git daemon (see ``publish_user_repo``)."""

    remote: str
    sha: str
    worktree: Path


def publish_user_repo(tmp_path_factory: pytest.TempPathFactory, fixture_dir: Path) -> UserRepo:
    """Publish ``fixture_dir`` as a bare git repo into the CI git daemon.

    Shared by the docker and kubernetes runner suites. ``worktree`` is the
    user-repo checkout the host CLI runs from; ``remote`` is what the build
    clones at ``sha``."""
    daemon = _git_daemon()
    worktree = tmp_path_factory.mktemp("user_repo")
    shutil.copytree(fixture_dir, worktree, dirs_exist_ok=True)

    def git(cwd: Path, *args: str) -> str:
        result = subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True)
        return result.stdout.strip()

    git(worktree, "init", "-q", "-b", "main")
    git(worktree, "add", "-A")
    # ``-c …`` overrides keep the commit independent of the runner's global git
    # config (identity + any ambient commit.gpgsign needing a key CI lacks).
    git(
        worktree,
        "-c",
        "user.email=e2e@example.com",
        "-c",
        "user.name=e2e",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "-qm",
        "fixture",
    )
    sha = git(worktree, "rev-parse", "HEAD")

    name = f"{fixture_dir.name}.git"
    bare = daemon.base / name
    git(worktree, "clone", "-q", "--bare", str(worktree), str(bare))
    _allow_any_sha(bare)
    return UserRepo(remote=daemon.url(name), sha=sha, worktree=worktree)


@pytest.fixture(scope="session")
def sandbox_remote() -> str:
    """An empty bare repo in the CI git daemon for the sandbox e2e to push
    into (the daemon runs with ``receive-pack`` enabled)."""
    daemon = _git_daemon()
    bare = init_bare_repo(daemon.base / "sandbox.git")
    _allow_any_sha(bare)
    return daemon.url("sandbox.git")


async def set_task_runs(task_id: int, run_ids: list[int], run_statuses: list[TaskStatus] | None = None) -> None:
    """Record ``run_ids`` as the task's attempts (each ``COMPLETED`` unless
    ``run_statuses`` says otherwise) — for log tests that flush ``task_logs``
    rows by hand instead of running the task."""
    statuses: list[TaskStatus] = run_statuses if run_statuses is not None else [TASK_COMPLETED] * len(run_ids)
    async with get_sql_session() as session:
        for run_id, status in zip(run_ids, statuses, strict=True):
            completed_at = None if status == TASK_RUNNING else utc_now()
            session.add(TaskRun(run_id=run_id, task_id=task_id, status=status, completed_at=completed_at))
        await session.commit()


async def get_task_runs(task_id: int) -> list[TaskRun]:
    """The task's attempts, oldest first."""
    async with get_sql_session() as session:
        rows = await session.execute(
            select(TaskRun).where(col(TaskRun.task_id) == task_id).order_by(col(TaskRun.run_id))
        )
        return list(rows.scalars().all())
