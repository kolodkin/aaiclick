"""
Pytest configuration for aaiclick tests.

This module provides:
- pytest_configure: xdist worker isolation for chdb
- event_loop: session-scoped event loop for async tests
- orch_ctx: fallback for tests outside the orchestration package (e.g. oplog)
"""

import asyncio
import os
import shutil
import tempfile

import pytest

from aaiclick.backend import is_chdb


def pytest_configure(config):
    """Give each xdist worker its own chdb data directory.

    chdb (embedded ClickHouse) is single-process — multiple workers cannot
    share the same data directory. When running under pytest-xdist, each
    worker gets a unique temp directory via AAICLICK_CH_URL.
    """
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    if worker_id is not None and is_chdb():
        chdb_dir = tempfile.mkdtemp(prefix=f"aaiclick_chdb_{worker_id}_")
        os.environ["AAICLICK_CH_URL"] = f"chdb://{chdb_dir}"


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def _shared_chdb_dir():
    """Session-scoped chdb data directory for non-orchestration tests."""
    tmp_dir = tempfile.mkdtemp(prefix="aaiclick_orch_chdb_")
    yield tmp_dir
    shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.fixture
async def orch_ctx(monkeypatch, _shared_chdb_dir):
    """Function-scoped orch context with dedicated sql_url.

    Fallback for tests outside aaiclick/orchestration/ (e.g. oplog tests).
    The orchestration package has its own conftest with the same fixture.
    """
    from aaiclick.orchestration.conftest import _orch_test_env

    async with _orch_test_env(monkeypatch, _shared_chdb_dir):
        yield
