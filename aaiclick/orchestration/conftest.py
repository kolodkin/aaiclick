"""
Orchestration test configuration.

Each xdist worker gets its own PostgreSQL database for full isolation,
allowing parallel test execution without row-locking conflicts.
"""

import os

import psycopg2
import pytest
from alembic import command
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from aaiclick.orchestration.context import orch_context
from aaiclick.orchestration.migrate import get_alembic_config

# Capture the original database name before any fixture modifies it
_BASE_DB = os.environ.get("POSTGRES_DB", "aaiclick")


def _pg_connect(dbname: str):
    """Connect to PostgreSQL with environment-based credentials."""
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ.get("POSTGRES_USER", "aaiclick"),
        password=os.environ.get("POSTGRES_PASSWORD", "secret"),
        dbname=dbname,
    )


@pytest.fixture(scope="session")
def _isolated_pg_db():
    """Create and drop an isolated PostgreSQL database for this xdist worker."""
    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    if not worker:
        # Not running under xdist — use the default database as-is
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

    # Point all subsequent get_pg_url() calls to the worker database
    os.environ["POSTGRES_DB"] = db_name

    # Run migrations directly (avoid run_migrations which calls sys.exit on error)
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


@pytest.fixture
async def orch_ctx(_isolated_pg_db):
    """
    Function-scoped orch context for orchestration tests.

    Depends on _isolated_pg_db to ensure the worker has its own database.
    """
    async with orch_context():
        yield
