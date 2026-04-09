"""End-to-end test for Object lifecycle with per-consumer pins.

Hooks into SQLAlchemy's engine events to capture all pin/unpin and
incref/decref activity on the junction tables, then verifies the
audit trail matches expectations.
"""

from typing import NamedTuple
from unittest.mock import AsyncMock

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.object import Object
from aaiclick.orchestration import tasks_list
from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.background.sqlite_handler import SqliteBackgroundHandler
from aaiclick.orchestration.execution.debug import run_job_tasks
from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.models import Job, JobStatus
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.sql_context import _sql_engine_var


# --- Task fixtures (module-level for entrypoint resolution) ---


@task
async def produce() -> Object:
    return await create_object_from_value([10, 20, 30])


@task
async def double(data: Object) -> Object:
    return await (data * 2)


@task
async def add_ten(data: Object) -> Object:
    return await (data + 10)


@task
async def add_objects(a: Object, b: Object) -> Object:
    return await (a + b)


@task
async def read_sum(data: Object) -> dict:
    result = await data.sum()
    return {"total": await result.data()}


# --- Job pipelines (module-level for entrypoint resolution) ---


@job("lifecycle_single")
def single_consumer_pipeline():
    data = produce()
    return read_sum(data=data)


@job("lifecycle_fan_out")
def fan_out_pipeline():
    data = produce()
    left = double(data=data)
    right = add_ten(data=data)
    merged = add_objects(a=left, b=right)
    return read_sum(data=merged)


@job("lifecycle_chain")
def chain_pipeline():
    data = produce()
    doubled = double(data=data)
    return read_sum(data=doubled)


@job("lifecycle_diamond")
def diamond_pipeline():
    data = produce()
    left = double(data=data)
    right = add_ten(data=data)
    merged = add_objects(a=left, b=right)
    return read_sum(data=merged)


# --- Audit trail ---

TRACKED_TABLES = {"table_pin_refs", "table_run_refs", "table_context_refs"}


class RefEvent(NamedTuple):
    action: str  # INSERT or DELETE
    table: str   # table_pin_refs, table_run_refs, table_context_refs


def _install_hook(async_engine):
    """Install SQLAlchemy event hook to capture junction table activity."""
    events = []
    sync_engine = async_engine.sync_engine

    @event.listens_for(sync_engine, "before_cursor_execute")
    def capture(conn, cursor, statement, parameters, context, executemany):
        stmt_upper = statement.upper()
        for tracked in TRACKED_TABLES:
            if tracked.upper() in stmt_upper:
                if "INSERT" in stmt_upper:
                    action = "INSERT"
                elif "DELETE" in stmt_upper:
                    action = "DELETE"
                else:
                    continue
                events.append(RefEvent(action, tracked))

    return events, capture


def _remove_hook(async_engine, listener):
    """Remove the event hook."""
    event.remove(async_engine.sync_engine, "before_cursor_execute", listener)


# --- Helpers ---


async def _get_pin_refs(session):
    result = await session.execute(text("SELECT table_name, task_id FROM table_pin_refs"))
    return {(r[0], r[1]) for r in result.fetchall()}


async def _get_run_refs(session):
    result = await session.execute(text("SELECT table_name, run_id FROM table_run_refs"))
    return {(r[0], r[1]) for r in result.fetchall()}


async def _get_context_tables(session):
    result = await session.execute(text("SELECT DISTINCT table_name FROM table_context_refs"))
    return {r[0] for r in result.fetchall()}


async def _run_cleanup():
    engine = _sql_engine_var.get()
    worker = BackgroundWorker()
    worker._engine = engine
    worker._handler = SqliteBackgroundHandler()
    worker._ch_client = AsyncMock()
    await worker._cleanup_unreferenced_tables()


async def _run_and_verify(pipeline_fn):
    """Run job, verify junction table state and audit trail, then cleanup."""
    engine = _sql_engine_var.get()
    events, listener = _install_hook(engine)

    try:
        job_obj = await pipeline_fn()
        await run_job_tasks(job_obj)

        async with get_sql_session() as session:
            db_job = (await session.execute(select(Job).where(Job.id == job_obj.id))).scalar_one()
            assert db_job.status == JobStatus.COMPLETED, f"Job failed: {db_job.error}"

            # No active run_refs after job completes
            run_refs = await _get_run_refs(session)
            assert len(run_refs) == 0, f"Stale run_refs: {run_refs}"

            # Context refs exist (tables not yet cleaned up)
            context_tables = await _get_context_tables(session)
            temp_tables = {t for t in context_tables if t.startswith("t_")}
            assert len(temp_tables) > 0, "Expected context_refs before cleanup"

        # Verify incref/decref pairs
        run_inserts = [e for e in events if e.table == "table_run_refs" and e.action == "INSERT"]
        run_deletes = [e for e in events if e.table == "table_run_refs" and e.action == "DELETE"]
        assert len(run_inserts) > 0, "No INCREFs recorded"
        assert len(run_deletes) > 0, "No DECREFs recorded"
        assert len(run_inserts) == len(run_deletes), (
            f"INCREF/DECREF mismatch: {len(run_inserts)} inserts vs {len(run_deletes)} deletes"
        )

        # Run background cleanup
        await _run_cleanup()

        async with get_sql_session() as session:
            context_tables = await _get_context_tables(session)
            temp_tables = {t for t in context_tables if t.startswith("t_")}
            assert len(temp_tables) == 0, f"Tables not cleaned up: {temp_tables}"

            pin_refs = await _get_pin_refs(session)
            assert len(pin_refs) == 0, f"Stale pin_refs after cleanup: {pin_refs}"

    finally:
        _remove_hook(engine, listener)

    return events


# --- Tests ---


async def test_single_consumer(orch_ctx):
    """A → B: Object survives for single consumer."""
    events = await _run_and_verify(single_consumer_pipeline)
    # Verify incref/decref balance
    runs = [e for e in events if e.table == "table_run_refs"]
    assert len(runs) > 0, "No run_ref activity"


async def test_fan_out(orch_ctx):
    """A → (B, C) → D: Object survives for both consumers."""
    events = await _run_and_verify(fan_out_pipeline)
    runs = [e for e in events if e.table == "table_run_refs"]
    assert len(runs) > 0, "No run_ref activity"


async def test_chain(orch_ctx):
    """A → B → C: each intermediate Object survives."""
    events = await _run_and_verify(chain_pipeline)
    runs = [e for e in events if e.table == "table_run_refs"]
    assert len(runs) > 0, "No run_ref activity"


async def test_diamond(orch_ctx):
    """A → (B, C) → D: diamond with shared Object."""
    events = await _run_and_verify(diamond_pipeline)
    runs = [e for e in events if e.table == "table_run_refs"]
    assert len(runs) > 0, "No run_ref activity"
