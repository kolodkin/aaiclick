"""
Orchestration test configuration.

Each test gets:
- Dedicated SQLite database (per-test temp file for isolation)
- Shared chdb data directory (per-session, chdb only allows one path per process)
"""

import os
import shutil
import tempfile
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

import pytest

from sqlalchemy import create_engine, text
from sqlmodel import select

from aaiclick.backend import is_sqlite
from aaiclick.orchestration.execution.claiming import cancel_job
from aaiclick.orchestration.models import Job, JobStatus, SQLModel, TaskStatus
from aaiclick.orchestration.orch_context import get_sql_session, orch_context

_BASE_DB = os.environ.get("POSTGRES_DB", "aaiclick")


@pytest.fixture(autouse=True)
def _tmp_log_dir(tmp_path, monkeypatch):
    """Direct task logs to a temporary directory in all orchestration tests."""
    monkeypatch.setenv("AAICLICK_LOG_DIR", str(tmp_path))


_ORCH_CHDB_DIR: Optional[str] = None


@pytest.fixture(autouse=True, scope="session")
def _shared_chdb_dir():
    """Session-scoped chdb data directory (autouse for all orchestration tests).

    chdb's embedded server can only be initialized once per process with a
    single data path. Sets AAICLICK_CH_URL at the session level so that any
    code reading the env (e.g. snowflake_id) uses the correct path even
    outside per-test fixtures.
    """
    global _ORCH_CHDB_DIR
    tmp_dir = tempfile.mkdtemp(prefix="aaiclick_orch_chdb_")
    _ORCH_CHDB_DIR = tmp_dir
    old_url = os.environ.get("AAICLICK_CH_URL")
    os.environ["AAICLICK_CH_URL"] = f"chdb://{tmp_dir}"
    yield tmp_dir
    if old_url is not None:
        os.environ["AAICLICK_CH_URL"] = old_url
    else:
        os.environ.pop("AAICLICK_CH_URL", None)
    shutil.rmtree(tmp_dir, ignore_errors=True)


@asynccontextmanager
async def _orch_test_env(
    monkeypatch: pytest.MonkeyPatch,
    chdb_path: str,
    *,
    with_ch: bool = True,
) -> AsyncIterator[None]:
    """Shared setup/teardown for all orch_ctx variants.

    Creates an isolated SQLite DB per test, reuses the session-scoped chdb
    dir, enters orch_context, and tears down on exit. For PostgreSQL, uses
    the CI-provided URL as-is.
    """
    if is_sqlite():
        tmp_dir = tempfile.mkdtemp(prefix="aaiclick_orch_sql_")
        db_path = os.path.join(tmp_dir, "test.db")

        monkeypatch.setenv("AAICLICK_SQL_URL", f"sqlite+aiosqlite:///{db_path}")
        monkeypatch.setenv("AAICLICK_CH_URL", f"chdb://{chdb_path}")

        engine = create_engine(f"sqlite:///{db_path}")
        SQLModel.metadata.create_all(engine)
        engine.dispose()

        try:
            async with orch_context(with_ch=with_ch):
                yield
                await _teardown_jobs()
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
    else:
        async with orch_context(with_ch=with_ch):
            yield
            await _teardown_jobs()


@pytest.fixture
async def orch_ctx(monkeypatch, _shared_chdb_dir):
    """Function-scoped orch context with full chdb + SQL."""
    async with _orch_test_env(monkeypatch, _shared_chdb_dir):
        yield


@pytest.fixture
async def orch_ctx_no_ch(monkeypatch):
    """Function-scoped orch context without chdb (with_ch=False).

    For tests where the child process owns chdb (e.g. multiprocessing worker).
    Uses a per-test chdb dir so the child process can lock it exclusively.
    """
    chdb_dir = tempfile.mkdtemp(prefix="aaiclick_orch_mp_chdb_")
    try:
        async with _orch_test_env(monkeypatch, chdb_dir, with_ch=False):
            yield
    finally:
        shutil.rmtree(chdb_dir, ignore_errors=True)


async def _teardown_jobs() -> None:
    """Cancel non-terminal jobs and orphan tasks."""
    async with get_sql_session() as session:
        result = await session.execute(
            select(Job.id).where(Job.status.notin_([
                JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED,
            ]))
        )
        for (job_id,) in result.all():
            await cancel_job(job_id)

        await session.execute(
            text(
                "UPDATE tasks SET status = :cancelled "
                "WHERE status IN ('PENDING', 'CLAIMED', 'RUNNING')"
            ),
            {"cancelled": TaskStatus.CANCELLED.value},
        )
        await session.commit()
