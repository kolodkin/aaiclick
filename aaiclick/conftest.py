"""
Pytest configuration for aaiclick tests.

This module provides:
- _ch_worker_setup: session-scoped per-worker CH isolation (autouse)
- event_loop: session-scoped event loop for async tests
- orch_ctx: fallback for tests outside the orchestration package (e.g. oplog)
"""

import asyncio
import os
import shutil
import tempfile

import pytest

from aaiclick.backend import is_chdb, parse_ch_url
from aaiclick.oplog.lineage import OplogNode


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


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
async def orch_ctx(monkeypatch):
    """Function-scoped orch context for tests outside aaiclick/orchestration/.

    (E.g. oplog tests.) Delegates to the orchestration package's
    ``_orch_test_env`` which reads the per-worker AAICLICK_CH_URL set by
    ``_ch_worker_setup`` (defined in this file, visible to all tests
    via pytest's conftest hierarchy).
    """
    from aaiclick.orchestration.conftest import _orch_test_env

    async with _orch_test_env(monkeypatch):
        yield
