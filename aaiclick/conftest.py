"""
Pytest configuration for aaiclick tests.

Session-scoped autouse fixtures set up per-xdist-worker CH and SQL
databases. Per-test reset (drop CH tables, truncate SQL) happens inside
each ``orch_ctx`` / ``ctx`` fixture via ``reset_test_state`` from
``aaiclick.test_utils``.
"""

import asyncio
import importlib
import os
import shutil
import tempfile

import pytest
from alembic import command
from sqlalchemy import create_engine

from aaiclick.backend import is_chdb, is_local, parse_ch_url
from aaiclick.data.data_context import chdb_client as _chdb_client
from aaiclick.data.data_context.chdb_client import close_session as _real_close_session
from aaiclick.data.data_context.chdb_client import get_chdb_data_path as _real_chdb_path
from aaiclick.oplog.lineage import OplogNode
from aaiclick.orchestration.migrate import get_alembic_config
from aaiclick.orchestration.models import SQLModel
from aaiclick.orchestration.orch_context import orch_context
from aaiclick.test_utils import reset_test_state

# ``aaiclick.orchestration`` re-exports the ``orch_context`` function from the
# same-named submodule, which shadows the submodule attribute on the package.
# Fetch the submodule by fully-qualified name so we can patch its bindings.
_orch_context_module = importlib.import_module("aaiclick.orchestration.orch_context")

_BASE_SQL_DB = os.environ.get("POSTGRES_DB", "aaiclick")


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True, scope="session")
def _pin_chdb_session():
    """Keep the chdb Session alive for the entire pytest run.

    chdb's embedded ClickHouse carries one Poco Application singleton + one
    native ThreadPool per process. Tearing that down and rebuilding it
    repeatedly (which ``orch_context()`` does on every exit via
    ``close_session()``) races lingering ThreadPool workers against
    reinitialization and trips a glibc ``pthread_mutex_lock`` assertion
    inside ``_chdb.abi3.so``. The crash is intermittent (~25–40% of full
    test-suite runs) and fully inside chdb's native code — see
    ``docs/technical_debt.md`` and chdb-io/chdb#229.

    Test-only mitigation: no-op ``close_session`` for the life of the
    pytest session. The chdb Session becomes a true per-process
    singleton, which is what chdb actually supports.
    """
    if not is_chdb():
        yield
        return
    noop = lambda _path: None  # noqa: E731 — intentional trivial no-op stub
    # ``orch_context`` imported ``close_session`` by name, so patch the
    # binding in both the source module and the importer. pyright balks at
    # assigning to ``ModuleType`` attributes — intentional monkey-patch.
    original_src = _chdb_client.close_session
    original_orch = _orch_context_module.close_session
    _chdb_client.close_session = noop  # pyright: ignore[reportAttributeAccessIssue]
    _orch_context_module.close_session = noop  # pyright: ignore[reportAttributeAccessIssue]
    try:
        yield
    finally:
        _chdb_client.close_session = original_src  # pyright: ignore[reportAttributeAccessIssue]
        _orch_context_module.close_session = original_orch  # pyright: ignore[reportAttributeAccessIssue]


@pytest.fixture(autouse=True, scope="session")
def _ch_worker_setup():
    """Per-worker CH isolation — tempdir for chdb, database for real CH.

    Every xdist worker gets its own ``default`` CH database:

    - **chdb**: a per-worker tempdir (chdb forbids multiple data paths
      per process, and separate processes can't share one directory).
    - **real CH**: a ``default_<worker>`` database in the shared server.

    Without this, the per-test ``DROP TABLE`` sweep would cross worker
    boundaries in real-CH CI jobs.
    """
    worker = os.environ.get("PYTEST_XDIST_WORKER", "")

    if is_chdb():
        if not worker:
            yield
            return
        tmp_dir = tempfile.mkdtemp(prefix=f"aaiclick_chdb_{worker}_")
        prior_url = os.environ.get("AAICLICK_CH_URL")
        os.environ["AAICLICK_CH_URL"] = f"chdb://{tmp_dir}"
        try:
            yield
        finally:
            if prior_url is None:
                os.environ.pop("AAICLICK_CH_URL", None)
            else:
                os.environ["AAICLICK_CH_URL"] = prior_url
            shutil.rmtree(tmp_dir, ignore_errors=True)
        return

    # Distributed extra only — keep inline so local-only installs can import this conftest.
    import clickhouse_connect

    if not worker:
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

    prior_url = os.environ["AAICLICK_CH_URL"]
    os.environ["AAICLICK_CH_URL"] = prior_url.rsplit("/", 1)[0] + f"/{db_name}"
    try:
        yield
    finally:
        os.environ["AAICLICK_CH_URL"] = prior_url
        admin = _admin()
        admin.command(f"DROP DATABASE IF EXISTS `{db_name}`")
        admin.close()


def _pg_connect(dbname: str):
    """Connect to PostgreSQL with environment-based credentials."""
    # Distributed extra only — keep inline so local-only installs can import this conftest.
    import psycopg2

    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ.get("POSTGRES_USER", "aaiclick"),
        password=os.environ.get("POSTGRES_PASSWORD", "secret"),
        dbname=dbname,
    )


@pytest.fixture(autouse=True, scope="session")
def _sql_worker_setup():
    """Per-worker SQL isolation — SQLite file for local, database for Postgres.

    Local mode: one SQLite file per worker. Schema is created once via
    ``SQLModel.metadata.create_all``; per-test cleanup is a ``DELETE FROM``
    sweep in ``reset_sql_tables`` — no tempdir-per-test needed.

    Distributed mode: one Postgres database per worker, migrated once via
    Alembic; per-test cleanup is ``TRUNCATE ... RESTART IDENTITY CASCADE``.
    """
    worker = os.environ.get("PYTEST_XDIST_WORKER", "")

    if is_local():
        tmp_dir = tempfile.mkdtemp(prefix=f"aaiclick_sql_{worker or 'main'}_")
        db_path = os.path.join(tmp_dir, "test.db")
        prior_url = os.environ.get("AAICLICK_SQL_URL")
        os.environ["AAICLICK_SQL_URL"] = f"sqlite+aiosqlite:///{db_path}"

        engine = create_engine(f"sqlite:///{db_path}")
        SQLModel.metadata.create_all(engine)
        engine.dispose()
        try:
            yield
        finally:
            if prior_url is None:
                os.environ.pop("AAICLICK_SQL_URL", None)
            else:
                os.environ["AAICLICK_SQL_URL"] = prior_url
            shutil.rmtree(tmp_dir, ignore_errors=True)
        return

    # Distributed extra only — keep inline so local-only installs can import this conftest.
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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


def make_oplog_node(
    table: str,
    operation: str,
    kwargs: dict[str, str] | None = None,
) -> OplogNode:
    """Create an OplogNode with sensible defaults for tests."""
    return OplogNode(
        table=table,
        operation=operation,
        kwargs=kwargs or {},
        sql_template=None,
        task_id=None,
        job_id=None,
    )


@pytest.fixture
async def orch_ctx():
    """Function-scoped orch context with per-test CH + SQL reset.

    Single definition — visible to every test via pytest's conftest
    hierarchy, so oplog and orchestration tests share one fixture.
    """
    async with reset_test_state(orch_context(), reset_sql=True):
        yield


@pytest.fixture
async def orch_ctx_no_ch():
    """Function-scoped orch context without CH (``with_ch=False``).

    For tests where the child process owns chdb (e.g. multiprocessing
    worker); the parent releases its lock before spawning the child
    (see ``mp_worker._run_task_in_child``).

    ``_pin_chdb_session`` no-ops ``close_session`` for the pytest run to
    dodge a chdb teardown race, but mp-worker tests rely on the parent's
    chdb file lock being released so the child can open it. Release the
    pinned session here using the real ``close_session`` before entering
    the no-ch orch context.
    """
    if is_chdb():
        _real_close_session(_real_chdb_path())
    async with reset_test_state(orch_context(with_ch=False), reset_ch=False, reset_sql=True):
        yield
