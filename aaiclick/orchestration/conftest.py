"""
Orchestration test configuration.

Supports two backends:
- distributed: Each xdist worker gets its own PostgreSQL database for full isolation
- local: Each xdist worker gets its own SQLite file in a temp directory
"""

import os
import tempfile

import pytest

from aaiclick.backend import is_sqlite
from aaiclick.orchestration.orch_context import orch_context

# Capture the original database name before any fixture modifies it
_BASE_DB = os.environ.get("POSTGRES_DB", "aaiclick")


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
def _isolated_db():
    """Create and drop an isolated database for this xdist worker."""
    if is_sqlite():
        yield from _setup_sqlite_db()
    else:
        yield from _setup_pg_db()


def _setup_sqlite_db():
    """Set up an isolated SQLite database for tests."""
    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    suffix = f"_{worker}" if worker else ""
    tmp_dir = tempfile.mkdtemp(prefix="aaiclick_test_")
    db_path = os.path.join(tmp_dir, f"test{suffix}.db")
    os.environ["AAICLICK_SQL_URL"] = f"sqlite+aiosqlite:///{db_path}"

    # Create tables via SQLModel metadata
    from sqlalchemy import create_engine

    from aaiclick.orchestration.models import SQLModel

    engine = create_engine(f"sqlite:///{db_path}")
    SQLModel.metadata.create_all(engine)
    engine.dispose()

    yield

    # Cleanup
    import shutil

    shutil.rmtree(tmp_dir, ignore_errors=True)


def _setup_pg_db():
    """Set up an isolated PostgreSQL database for tests."""
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    from aaiclick.orchestration.migrate import get_alembic_config

    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    if not worker:
        # Not running under xdist — use the default database as-is
        from alembic import command

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

    # Point all subsequent get_db_url() calls to the worker database
    os.environ["POSTGRES_DB"] = db_name
    # Also update AAICLICK_SQL_URL if set, so get_sql_url() uses the worker DB
    sql_url = os.environ.get("AAICLICK_SQL_URL")
    if sql_url and "postgresql" in sql_url:
        # Replace database name in URL: ...host:port/old_db -> ...host:port/new_db
        base = sql_url.rsplit("/", 1)[0]
        os.environ["AAICLICK_SQL_URL"] = f"{base}/{db_name}"

    # Run migrations
    from alembic import command

    config = get_alembic_config()
    command.upgrade(config, "head")

    yield

    # Teardown: drop the worker database
    conn = _pg_connect("postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
    cur.close()
    conn.close()


@pytest.fixture(autouse=True)
def _tmp_log_dir(tmp_path, monkeypatch):
    """Direct task logs to a temporary directory in all orchestration tests."""
    monkeypatch.setenv("AAICLICK_LOG_DIR", str(tmp_path))


@pytest.fixture
async def orch_ctx(_isolated_db):
    """
    Function-scoped orch context for orchestration tests.

    Depends on _isolated_db to ensure the worker has its own database.
    """
    async with orch_context():
        yield
