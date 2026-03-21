"""
Oplog test configuration.

Provides orch_ctx fixture for oplog tests that require orchestration infrastructure
(task_scope, _OrchLifecycleView) via a temporary SQLite database.
"""

import os
import shutil
import tempfile

import pytest

from aaiclick.orchestration.context import orch_context


@pytest.fixture
async def orch_ctx():
    """
    Function-scoped orch context for oplog tests.

    Creates a temporary SQLite database for each test to ensure isolation.
    """
    tmpdir = tempfile.mkdtemp(prefix="aaiclick_oplog_test_")
    db_path = os.path.join(tmpdir, "test.db")
    old_url = os.environ.get("AAICLICK_SQL_URL")
    os.environ["AAICLICK_SQL_URL"] = f"sqlite+aiosqlite:///{db_path}"

    from sqlalchemy import create_engine

    from aaiclick.orchestration.models import SQLModel

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
