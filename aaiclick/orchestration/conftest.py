"""
Orchestration test configuration.

Each xdist worker gets its own PostgreSQL database for full isolation,
allowing parallel test execution without row-locking conflicts.
"""

import os

import psycopg2
import pytest
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from aaiclick.orchestration.context import orch_context
from aaiclick.orchestration.migrate import run_migrations

# Capture the original database name before any fixture modifies it
_BASE_DB = os.environ.get("POSTGRES_DB", "aaiclick")


@pytest.fixture(scope="session")
def _isolated_pg_db():
    """Create and drop an isolated PostgreSQL database for this xdist worker."""
    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    if not worker:
        # Not running under xdist — use the default database as-is
        yield
        return

    db_name = f"{_BASE_DB}_{worker}"
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    user = os.environ.get("POSTGRES_USER", "aaiclick")
    password = os.environ.get("POSTGRES_PASSWORD", "secret")

    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname="postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
    cur.execute(f'CREATE DATABASE "{db_name}"')
    cur.close()
    conn.close()

    # Point all subsequent get_pg_url() calls to the worker database
    os.environ["POSTGRES_DB"] = db_name

    # Run migrations on the new database
    run_migrations(["upgrade", "head"])

    yield

    # Teardown: drop the worker database
    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname="postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
    cur.close()
    conn.close()


@pytest.fixture
async def orch_ctx(_isolated_pg_db):
    """
    Function-scoped orch context for orchestration tests.

    Depends on _isolated_pg_db to ensure the worker has its own database.
    """
    async with orch_context():
        yield
