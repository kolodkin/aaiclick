"""
Orchestration test configuration.

Each test starts with empty SQL and CH state via per-test reset:
- SQL (SQLite or Postgres): per-xdist-worker DB (schema set up once),
  per-test TRUNCATE of all user tables.
- CH (chdb or real ClickHouse): per-xdist-worker DB (embedded chdb dir
  or dedicated real-CH database), per-test DROP of all tables.
"""

import os
import shutil
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import create_engine, text

from aaiclick.backend import is_chdb, is_sqlite, parse_ch_url
from aaiclick.data.data_context import get_ch_client
from aaiclick.orchestration.models import SQLModel
from aaiclick.orchestration.orch_context import get_sql_session, orch_context

_BASE_DB = os.environ.get("POSTGRES_DB", "aaiclick")


@pytest.fixture(autouse=True)
def _tmp_log_dir(tmp_path, monkeypatch):
    """Direct task logs to a temporary directory in all orchestration tests."""
    monkeypatch.setenv("AAICLICK_LOG_DIR", str(tmp_path))


@pytest.fixture
def fast_poll(monkeypatch):
    """Reduce polling and retry delays for worker-loop tests."""
    monkeypatch.setattr(
        "aaiclick.orchestration.execution.worker.POLL_INTERVAL",
        0.5,
    )
    monkeypatch.setattr(
        "aaiclick.orchestration.background.background_worker.RETRY_BASE_DELAY",
        0.01,
    )
    monkeypatch.setattr(
        "aaiclick.orchestration.execution.mp_worker.CHILD_POLL_INTERVAL",
        0.1,
    )


@pytest.fixture(autouse=True, scope="session")
def _shared_chdb_dir():
    """Session-scoped chdb data directory (autouse for all orchestration tests).

    chdb's embedded server can only be initialized once per process with a
    single data path. If pytest_configure (root conftest) already set
    AAICLICK_CH_URL for an xdist worker, reuse that path instead of
    creating a second one — chdb forbids multiple paths in one process.
    """
    if not is_sqlite():
        yield ""
        return
    existing_url = os.environ.get("AAICLICK_CH_URL", "")
    if existing_url.startswith("chdb://"):
        yield existing_url.removeprefix("chdb://")
        return
    tmp_dir = tempfile.mkdtemp(prefix="aaiclick_orch_chdb_")
    os.environ["AAICLICK_CH_URL"] = f"chdb://{tmp_dir}"
    yield tmp_dir
    os.environ.pop("AAICLICK_CH_URL", None)
    shutil.rmtree(tmp_dir, ignore_errors=True)


# -- Real ClickHouse per-worker isolation --


@pytest.fixture(autouse=True, scope="session")
def _ch_worker_db():
    """Create and drop an isolated ClickHouse database per xdist worker.

    chdb mode already has per-worker isolation via the shared data
    directory (each worker's `default` DB is in its own tempdir) so this
    is a no-op there. Real ClickHouse is a single shared server across
    all xdist workers; without a per-worker database the per-test DROP
    would obliterate other workers' tables.
    """
    if is_chdb():
        yield
        return

    import clickhouse_connect

    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    if not worker:
        yield
        return

    params = parse_ch_url()
    base_db = params["database"]
    db_name = f"{base_db}_{worker}"

    admin = clickhouse_connect.get_client(
        host=params["host"],
        port=params["port"],
        username=params["username"],
        password=params["password"],
        database=base_db,
    )
    admin.command(f"DROP DATABASE IF EXISTS `{db_name}`")
    admin.command(f"CREATE DATABASE `{db_name}`")
    admin.close()

    prior_url = os.environ["AAICLICK_CH_URL"]
    os.environ["AAICLICK_CH_URL"] = prior_url.rsplit("/", 1)[0] + f"/{db_name}"

    yield

    os.environ["AAICLICK_CH_URL"] = prior_url
    admin = clickhouse_connect.get_client(
        host=params["host"],
        port=params["port"],
        username=params["username"],
        password=params["password"],
        database=base_db,
    )
    admin.command(f"DROP DATABASE IF EXISTS `{db_name}`")
    admin.close()


# -- PostgreSQL per-worker isolation --


def _pg_connect(dbname: str):
    """Connect to PostgreSQL with environment-based credentials."""
    import psycopg2

    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ.get("POSTGRES_USER", "aaiclick"),
        password=os.environ.get("POSTGRES_PASSWORD", "secret"),
        dbname=dbname,
    )


@pytest.fixture(scope="session")
def _pg_worker_db():
    """Create and drop an isolated PostgreSQL database per xdist worker."""
    if is_sqlite():
        yield
        return

    from alembic import command
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    from aaiclick.orchestration.migrate import get_alembic_config

    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    if not worker:
        config = get_alembic_config()
        command.upgrade(config, "head")
        yield
        return

    db_name = f"{_BASE_DB}_{worker}"

    conn = _pg_connect("postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
    cur.execute(f'CREATE DATABASE "{db_name}"')
    cur.close()
    conn.close()

    os.environ["POSTGRES_DB"] = db_name
    sql_url = os.environ.get("AAICLICK_SQL_URL")
    if sql_url and "postgresql" in sql_url:
        base = sql_url.rsplit("/", 1)[0]
        os.environ["AAICLICK_SQL_URL"] = f"{base}/{db_name}"

    config = get_alembic_config()
    command.upgrade(config, "head")

    yield

    conn = _pg_connect("postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
    cur.close()
    conn.close()


# -- Per-test reset helpers --


async def _reset_sql_tables() -> None:
    """Truncate all user tables in the active SQL database (Postgres path).

    SQLite is handled via tempdir-per-test in _orch_test_env, so the SQLite
    DB is already empty at test start — nothing to do here.
    """
    if is_sqlite():
        return
    async with get_sql_session() as session:
        result = await session.execute(
            text("SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename != 'alembic_version'")
        )
        tables = [r[0] for r in result.all()]
        if tables:
            quoted = ", ".join(f'"{t}"' for t in tables)
            await session.execute(text(f"TRUNCATE {quoted} RESTART IDENTITY CASCADE"))
        await session.commit()


async def _drop_all_ch_tables() -> None:
    """Drop every table in the active CH database.

    Singletons (operation_log) are recreated lazily by init_oplog_tables
    on next task_scope entry. Safe against real CH because _ch_worker_db
    gives each xdist worker its own database, so this never touches
    another worker's tables.
    """
    ch = get_ch_client()
    result = await ch.query("SELECT name FROM system.tables WHERE database = currentDatabase()")
    for row in result.result_rows:
        await ch.command(f"DROP TABLE IF EXISTS `{row[0]}`")


# -- Shared test environment --


@asynccontextmanager
async def _orch_test_env(
    monkeypatch: pytest.MonkeyPatch,
    chdb_path: str,
    *,
    with_ch: bool = True,
) -> AsyncIterator[None]:
    """Shared setup for all orch_ctx variants.

    Single per-test reset methodology: every test starts with empty SQL +
    empty chdb state, so test ordering and crash recovery are irrelevant.

    SQLite: dedicated temp DB per test (effectively a drop+create).
    PostgreSQL: TRUNCATE all user tables in the per-worker DB.
    chdb: DROP all tables in the default database.
    """
    if is_sqlite():
        tmp_dir = tempfile.mkdtemp(prefix="aaiclick_orch_sql_")
        db_path = os.path.join(tmp_dir, "test.db")

        monkeypatch.setenv("AAICLICK_SQL_URL", f"sqlite+aiosqlite:///{db_path}")
        monkeypatch.setenv("AAICLICK_CH_URL", f"chdb://{chdb_path}")

        engine = create_engine(f"sqlite:///{db_path}")
        SQLModel.metadata.create_all(engine)
        engine.dispose()

        try:
            async with orch_context(with_ch=with_ch):
                if with_ch:
                    await _drop_all_ch_tables()
                yield
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
    else:
        async with orch_context(with_ch=with_ch):
            await _reset_sql_tables()
            if with_ch:
                await _drop_all_ch_tables()
            yield


@pytest.fixture
async def orch_ctx(monkeypatch, _shared_chdb_dir, _pg_worker_db):
    """Function-scoped orch context with full chdb + SQL."""
    async with _orch_test_env(monkeypatch, _shared_chdb_dir):
        yield


@pytest.fixture
async def orch_ctx_no_ch(monkeypatch, _shared_chdb_dir, _pg_worker_db):
    """Function-scoped orch context without chdb (with_ch=False).

    For tests where the child process owns chdb (e.g. multiprocessing worker).
    Uses the shared chdb dir — the parent releases its lock before spawning
    the child (see mp_worker._run_task_in_child).
    """
    async with _orch_test_env(monkeypatch, _shared_chdb_dir, with_ch=False):
        yield
