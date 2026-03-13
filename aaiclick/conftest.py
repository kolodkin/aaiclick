"""
Pytest configuration for aaiclick tests.

This module provides test fixtures. ClickHouse setup is handled by
scripts/setup_and_test.py or manually via docker-compose.
"""

import asyncio
import os
import tempfile

import pytest

from aaiclick.data.data_context import data_context


def pytest_configure(config):
    """Give each xdist worker its own chdb data directory.

    chdb (embedded ClickHouse) is single-process — multiple workers cannot
    share the same data directory. When running under pytest-xdist, each
    worker gets a unique temp directory via AAICLICK_CHDB_PATH.
    """
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    if worker_id is not None and os.environ.get("AAICLICK_BACKEND", "local") == "local":
        chdb_dir = tempfile.mkdtemp(prefix=f"aaiclick_chdb_{worker_id}_")
        os.environ["AAICLICK_CHDB_PATH"] = chdb_dir


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
