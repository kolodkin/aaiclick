"""
Orchestration test configuration.

Each test gets:
- SQLite: dedicated temp database per test
- PostgreSQL: per-xdist-worker database with Alembic migrations
- chdb: session-scoped shared directory (chdb allows one path per process)
"""

import os
import shutil
import tempfile
from contextlib import asynccontextmanager
from typing import AsyncIterator

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


@pytest.fixture(autouse=True, scope="session")
def _shared_chdb_dir():
    """Session-scoped chdb data directory (autouse for all orchestration tests).

    chdb's embedded server can only be initialized once per process with a
    single data path. Sets AAICLICK_CH_URL at the session level so that any
    code reading the env (e.g. snowflake_id) uses the correct path even
    outside per-test fixtures.
    """
    if not is_sqlite():
        yield ""
        return
    tmp_dir = tempfile.mkdtemp(prefix="aaiclick_orch_chdb_")
    old_url = os.environ.get("AAICLICK_CH_URL")
    os.environ["AAICLICK_CH_URL"] = f"chdb://{tmp_dir}"
    yield tmp_dir
    if old_url is not None:
        os.environ["AAICLICK_CH_URL"] = old_url
    else:
        os.environ.pop("AAICLICK_CH_URL", None)
    shutil.rmtree(tmp_dir, ignore_errors=True)


# -- PostgreSQL per-worker isolation --


def _pg_connect(dbname: str):
    """Connect to PostgreSQL with environment-based credentials."""
    import psycopg2

    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ.get("POSTGRES_USER", "aaiclick"),
        password=os.environ.get("POSTGRES_PASSWORD", "secret"),
        dbname=dbname,
    )


@pytest.fixture(scope="session")
def _pg_worker_db():
    """Create and drop an isolated PostgreSQL database per xdist worker."""
    if is_sqlite():
        yield
        return

    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    from aaiclick.orchestration.migrate import get_alembic_config
    from alembic import command

    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    if not worker:
        config = get_alembic_config()
        command.upgrade(config, "head")
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

    os.environ["POSTGRES_DB"] = db_name
    sql_url = os.environ.get("AAICLICK_SQL_URL")
    if sql_url and "postgresql" in sql_url:
        base = sql_url.rsplit("/", 1)[0]
        os.environ["AAICLICK_SQL_URL"] = f"{base}/{db_name}"

    config = get_alembic_config()
    command.upgrade(config, "head")

    yield

    conn = _pg_connect("postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
    cur.close()
    conn.close()


# -- Shared test environment --


@asynccontextmanager
async def _orch_test_env(
    monkeypatch: pytest.MonkeyPatch,
    chdb_path: str,
    *,
    with_ch: bool = True,
) -> AsyncIterator[None]:
    """Shared setup/teardown for all orch_ctx variants.

    SQLite: creates an isolated DB per test.
    PostgreSQL: uses the session-scoped worker DB (via _pg_worker_db).
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
async def orch_ctx(monkeypatch, _shared_chdb_dir, _pg_worker_db):
    """Function-scoped orch context with full chdb + SQL."""
    async with _orch_test_env(monkeypatch, _shared_chdb_dir):
        yield


@pytest.fixture
async def orch_ctx_no_ch(monkeypatch, _shared_chdb_dir, _pg_worker_db):
    """Function-scoped orch context without chdb (with_ch=False).

    For tests where the child process owns chdb (e.g. multiprocessing worker).
    Uses the shared chdb dir — the parent releases its lock before spawning
    the child (see mp_worker._run_task_in_child).
    """
    async with _orch_test_env(monkeypatch, _shared_chdb_dir, with_ch=False):
        yield


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
