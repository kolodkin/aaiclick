"""Shared test helpers and fixtures.

Per-subpackage conftests import the pytest fixtures defined here
(``ch_worker_setup``, ``sql_worker_setup``, ``pin_chdb_session``,
``orch_module_ctx``, ``orch_module_ctx_no_ch``, ``orch_ctx``,
``orch_ctx_no_ch``). pytest recognises imported fixtures by identity, so
the same fixture re-exported from multiple conftests still runs once per
scope. Keeping the implementations here avoids copy-paste across
``aaiclick/data/conftest.py``, ``aaiclick/orchestration/conftest.py``,
``aaiclick/oplog/conftest.py``, and ``aaiclick/ai/conftest.py``.
"""

from __future__ import annotations

import importlib
import os
import shutil
import tempfile
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import pytest
from alembic import command
from sqlalchemy import create_engine, text

from aaiclick.backend import is_chdb, is_local, parse_ch_url
from aaiclick.data.data_context import chdb_client as _chdb_client
from aaiclick.data.data_context import get_ch_client
from aaiclick.oplog.lineage import OplogNode
from aaiclick.orchestration.migrate import get_alembic_config
from aaiclick.orchestration.models import SQLModel
from aaiclick.orchestration.orch_context import get_sql_session, orch_context

# ``aaiclick.orchestration`` re-exports ``orch_context`` as a function, which
# shadows the submodule attribute on the package. Resolve the submodule by
# fully-qualified name so ``pin_chdb_session`` can patch its ``close_session``.
_orch_context_module = importlib.import_module("aaiclick.orchestration.orch_context")

_BASE_SQL_DB = os.environ.get("POSTGRES_DB", "aaiclick")


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
        if is_local():
            result = await session.execute(
                text("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
            )
            tables = [r[0] for r in result.all() if r[0] != "alembic_version"]
            for name in tables:
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


async def drop_all_ch_tables() -> None:
    """Drop every table in the active CH database.

    Singletons (operation_log) are recreated lazily by init_oplog_tables
    on next task_scope entry. Safe against real CH because
    ``ch_worker_setup`` gives each xdist worker its own database, so
    this never touches another worker's tables.
    """
    ch = get_ch_client()
    result = await ch.query("SELECT name FROM system.tables WHERE database = currentDatabase()")
    for row in result.result_rows:
        await ch.command(f"DROP TABLE IF EXISTS `{row[0]}`")


async def per_test_reset(*, reset_ch: bool = True, reset_sql: bool = True) -> None:
    """Wipe CH and/or SQL state between tests sharing a module orch scope.

    Must be called while the enclosing orch context is active. When
    ``reset_ch`` is true, requires ``with_ch=True`` in the enclosing
    ``orch_context``.
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


def with_value_order(obj):
    """Wrap a cross-table array Object with ``.view(order_by="value")``.

    Used by operator tests to satisfy the Phase 4 cross-table contract
    (``Object._apply_operator`` rejects array+array ops between different
    tables without explicit ``order_by`` on both sides). Scalars pass
    through unchanged.
    """
    from aaiclick.data.models import FIELDTYPE_ARRAY

    if obj._schema.fieldtype == FIELDTYPE_ARRAY:
        return obj.view(order_by="value")
    return obj


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


def _pg_connect(dbname: str):
    """Connect to PostgreSQL with environment-based credentials."""
    # Distributed extra only — keep inline so local-only installs can import this module.
    import psycopg2

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


@pytest.fixture(autouse=True, scope="session")
def pin_chdb_session():
    """Keep the chdb Session alive for the entire pytest run.

    Module-scoped orch fixtures already reduce chdb Session teardown
    calls from per-test to per-module, but even those remaining cycles
    intermittently race chdb's native ThreadPool on teardown (glibc
    ``pthread_mutex_lock`` assertion inside ``_chdb.abi3.so``, see
    ``docs/technical_debt.md``). No-op ``close_session`` for the
    lifetime of the pytest run so the chdb Session is a true per-process
    singleton — what chdb actually supports.
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
def ch_worker_setup():
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

    # Distributed extra only — keep inline so local-only installs can import this module.
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


@pytest.fixture(autouse=True, scope="session")
def sql_worker_setup():
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

    # Distributed extra only — keep inline so local-only installs can import this module.
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


@pytest.fixture(scope="module")
async def orch_module_ctx():
    """Module-scoped ``orch_context()`` with chdb — entered once per module.

    Keeps the chdb Session alive across every test in the module so the
    per-test Session teardown (which races chdb's native ThreadPool, see
    ``docs/technical_debt.md``) fires at most once per module, not once
    per test.
    """
    async with module_orch_scope(orch_context()):
        yield


@pytest.fixture(scope="module")
async def orch_module_ctx_no_ch():
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
async def orch_ctx(orch_module_ctx):
    """Per-test state reset + synthetic task_scope on top of the module-scoped
    chdb orch context.

    The task_scope activates OrchLifecycleHandler so create_object can write
    to table_registry.schema_doc — required by Phase 2's registry-backed
    _get_table_schema read path. Tests that need a real task/job/run_id
    should use the job-factory fixtures instead.
    """
    from aaiclick.orchestration.orch_context import task_scope
    from aaiclick.snowflake import get_snowflake_id

    await per_test_reset(reset_ch=True, reset_sql=True)
    synthetic_id = get_snowflake_id()
    async with task_scope(task_id=synthetic_id, job_id=synthetic_id, run_id=synthetic_id):
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
