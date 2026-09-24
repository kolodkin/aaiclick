"""Pytest fixtures for aaiclick.orchestration tests.

Shared fixtures (``ch_worker_setup``, ``sql_worker_setup``, ``orch_ctx``
family) register globally via the ``aaiclick.testing`` plugin (see
``aaiclick/conftest.py``). This conftest holds orchestration-local
helpers: the polling-speed monkeypatches and the ``bg_db`` SQLite engine
shared by the ``background/`` and ``lifecycle/`` tests.
"""

import importlib.util
import os
import shutil
import tempfile

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

from aaiclick.orchestration.models import SQLModel

# The Postgres transport imports asyncpg at module level (``distributed``
# extra); skip its test module when the driver is not installed, the same
# way the root conftest skips the ``server`` and ``ai`` suites.
collect_ignore = [] if importlib.util.find_spec("asyncpg") else ["events/test_events_postgres.py"]


@pytest.fixture
def fast_poll(monkeypatch):
    """Reduce polling and retry delays for worker-loop tests."""
    monkeypatch.setattr(
        "aaiclick.orchestration.execution.execution_worker.POLL_INTERVAL",
        0.5,
    )
    monkeypatch.setattr(
        "aaiclick.orchestration.background.background_worker.RETRY_BASE_DELAY",
        0.01,
    )
    monkeypatch.setattr(
        "aaiclick.orchestration.execution.mp_worker.CHILD_POLL_INTERVAL",
        0.1,
    )


@pytest.fixture
async def bg_db():
    """Create a temp SQLite DB with schema, yield (async_engine, tmpdir), then cleanup."""
    tmpdir = tempfile.mkdtemp(prefix="aaiclick_bgtest_")
    db_path = os.path.join(tmpdir, "test.db")
    sync_engine = create_engine(f"sqlite:///{db_path}")
    SQLModel.metadata.create_all(sync_engine)
    sync_engine.dispose()
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    yield engine
    await engine.dispose()
    shutil.rmtree(tmpdir, ignore_errors=True)
