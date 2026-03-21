"""
Pytest configuration for aaiclick tests.

This module provides test fixtures. ClickHouse setup is handled by
scripts/setup_and_test.py or manually via docker-compose.
"""

import asyncio
import os
import shutil
import tempfile

import pytest

from sqlalchemy import create_engine

from aaiclick.backend import is_chdb, is_sqlite
from aaiclick.data.data_context import data_context
from aaiclick.orchestration.orch_context import orch_context
from aaiclick.orchestration.models import SQLModel


def pytest_configure(config):
    """Give each xdist worker its own chdb data directory.

    chdb (embedded ClickHouse) is single-process — multiple workers cannot
    share the same data directory. When running under pytest-xdist, each
    worker gets a unique temp directory via AAICLICK_CHDB_PATH.
    """
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    if worker_id is not None and is_chdb():
        chdb_dir = tempfile.mkdtemp(prefix=f"aaiclick_chdb_{worker_id}_")
        os.environ["AAICLICK_CH_URL"] = f"chdb://{chdb_dir}"


@pytest.fixture(scope="session")
def event_loop():
    """
    Create an event loop for async tests.
    """
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def ctx():
    """
    Session-scoped data context shared across all tests in a worker.

    Objects are cleaned up via refcounting when they go out of scope,
    so table accumulation is not a concern.
    """
    async with data_context():
        yield


@pytest.fixture
async def orch_ctx():
    """
    Function-scoped orch context for tests that require orchestration infrastructure.

    SQLite: creates a temporary database and initialises schema via SQLModel.
    PostgreSQL: assumes migrations have already been applied (by CI or aaiclick setup).
    """
    if is_sqlite():
        tmpdir = tempfile.mkdtemp(prefix="aaiclick_test_")
        db_path = os.path.join(tmpdir, "test.db")
        old_url = os.environ.get("AAICLICK_SQL_URL")
        os.environ["AAICLICK_SQL_URL"] = f"sqlite+aiosqlite:///{db_path}"

        engine = create_engine(f"sqlite:///{db_path}")
        SQLModel.metadata.create_all(engine)
        engine.dispose()

        try:
            async with orch_context():
                yield
        finally:
            if old_url is not None:
                os.environ["AAICLICK_SQL_URL"] = old_url
            elif "AAICLICK_SQL_URL" in os.environ:
                del os.environ["AAICLICK_SQL_URL"]
            shutil.rmtree(tmpdir, ignore_errors=True)
    else:
        async with orch_context():
            yield
