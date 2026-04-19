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
from aaiclick.oplog.lineage import OplogNode


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


@pytest.fixture(scope="module")
def _orch_chdb_dir():
    """Provide a module-scoped chdb path for tests outside the orchestration package.

    chdb's embedded server is a process-wide singleton bound to one data
    path until cleanup() is called. Per-module isolation bounds the number
    of t_* tables that accumulate on disk per chdb session. On teardown
    the chdb session is closed so the next module can re-init with a fresh
    path; the prior AAICLICK_CH_URL is restored.

    If another chdb Session is already active in this process (e.g. from
    a session-scoped data-test fixture sharing the same xdist worker),
    skip the override and reuse the existing path — chdb forbids two
    paths per process.
    """
    from aaiclick.data.data_context.chdb_client import _sessions, close_session

    if _sessions:
        existing_url = os.environ.get("AAICLICK_CH_URL", "")
        if existing_url.startswith("chdb://"):
            yield existing_url.removeprefix("chdb://")
            return
    prior_url = os.environ.get("AAICLICK_CH_URL")
    tmp_dir = tempfile.mkdtemp(prefix="aaiclick_orch_chdb_")
    os.environ["AAICLICK_CH_URL"] = f"chdb://{tmp_dir}"
    try:
        yield tmp_dir
    finally:
        close_session(tmp_dir)
        if prior_url is None:
            os.environ.pop("AAICLICK_CH_URL", None)
        else:
            os.environ["AAICLICK_CH_URL"] = prior_url
        shutil.rmtree(tmp_dir, ignore_errors=True)


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
async def orch_ctx(monkeypatch, _orch_chdb_dir):
    """Function-scoped orch context with dedicated sql_url.

    Fallback for tests outside aaiclick/orchestration/ (e.g. oplog tests).
    The orchestration package has its own conftest with the same fixture.
    """
    from aaiclick.orchestration.conftest import _orch_test_env

    async with _orch_test_env(monkeypatch, _orch_chdb_dir):
        yield
