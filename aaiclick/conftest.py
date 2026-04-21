"""
Pytest configuration for aaiclick tests.

Session-scoped autouse fixtures set up per-xdist-worker CH and SQL
databases. Per-module ``orch_context`` fixtures keep chdb alive across
every test in the module (avoiding the per-test chdb Session teardown
that triggers a native ThreadPool race). Per-test state reset happens
inside each ``orch_ctx`` / ``orch_ctx_no_ch`` fixture via
``per_test_reset`` from ``aaiclick.test_utils``.
"""

import importlib
import os
import shutil
import tempfile

import pytest
from alembic import command
from sqlalchemy import create_engine

from aaiclick.backend import is_chdb, is_local, parse_ch_url
from aaiclick.data.data_context import chdb_client as _chdb_client
from aaiclick.oplog.lineage import OplogNode
from aaiclick.orchestration.migrate import get_alembic_config
from aaiclick.orchestration.models import SQLModel
from aaiclick.orchestration.orch_context import orch_context
from aaiclick.test_utils import module_orch_scope, per_test_reset

# ``aaiclick.orchestration`` re-exports ``orch_context`` as a function, which
# shadows the submodule attribute on the package. Resolve the submodule by
# fully-qualified name so ``_pin_chdb_session`` can patch its ``close_session``.
_orch_context_module = importlib.import_module("aaiclick.orchestration.orch_context")

_BASE_SQL_DB = os.environ.get("POSTGRES_DB", "aaiclick")


@pytest.fixture(autouse=True, scope="session")
def _pin_chdb_session():
    """Keep the chdb Session alive for the entire pytest run.

    Module-scoped orch fixtures already reduce chdb Session teardown
    calls from per-test (~500) to per-module (~20), but even those
    remaining cycles intermittently race chdb's native ThreadPool on
    teardown (glibc ``pthread_mutex_lock`` assertion inside
    ``_chdb.abi3.so``, see ``docs/technical_debt.md``). No-op
    ``close_session`` for the lifetime of the pytest run so the chdb
    Session is a true per-process singleton — what chdb actually
    supports.
    """
    if not is_chdb():
        yield
        return
    noop = lambda _path: None  # noqa: E731 — intentional trivial no-op stub
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


@pytest.fixture(scope="module")
async def _orch_module_ctx():
    """Module-scoped ``orch_context()`` with chdb — entered once per module.

    Keeps the chdb Session alive across every test in the module so the
    per-test Session teardown (which races chdb's native ThreadPool, see
    ``docs/technical_debt.md``) fires at most once per module, not once
    per test.
    """
    async with module_orch_scope(orch_context()):
        yield


@pytest.fixture(scope="module")
async def _orch_module_ctx_no_ch():
    """Module-scoped ``orch_context(with_ch=False)`` — entered once per module.

    Used by test modules whose tests spawn multiprocessing workers: the
    parent never opens chdb, so the child can acquire the chdb file lock.
    Must live in a dedicated module (no ``orch_ctx`` alongside) so the
    module-scoped chdb session doesn't collide.

    Under chdb, redirects ``AAICLICK_CH_URL`` to a per-module tempdir so
    mp-worker children open a fresh chdb file that no other module's
    session holds a lock on.
    """
    prior_url = None
    tmp_dir = None
    if is_chdb():
        tmp_dir = tempfile.mkdtemp(prefix="aaiclick_chdb_mp_")
        prior_url = os.environ.get("AAICLICK_CH_URL")
        os.environ["AAICLICK_CH_URL"] = f"chdb://{tmp_dir}"
    try:
        async with module_orch_scope(orch_context(with_ch=False)):
            yield
    finally:
        if is_chdb():
            if prior_url is None:
                os.environ.pop("AAICLICK_CH_URL", None)
            else:
                os.environ["AAICLICK_CH_URL"] = prior_url
            if tmp_dir is not None:
                shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.fixture
async def orch_ctx(_orch_module_ctx):
    """Per-test state reset on top of the module-scoped chdb orch context."""
    await per_test_reset(reset_ch=True, reset_sql=True)
    yield


@pytest.fixture
async def orch_ctx_no_ch(_orch_module_ctx_no_ch):
    """Per-test state reset on top of the module-scoped no-ch orch context.

    For tests where the child process owns chdb (e.g. multiprocessing
    worker). Must be used only in modules that never also use
    ``orch_ctx`` — mixing the two within one module would require two
    conflicting module-scoped orch contexts.
    """
    await per_test_reset(reset_ch=False, reset_sql=True)
    yield
