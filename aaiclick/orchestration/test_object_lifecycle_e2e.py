"""End-to-end test for Object lifecycle with per-consumer pins.

Verifies that Objects passed between tasks survive cleanup and that
junction tables (table_pin_refs, table_run_refs, table_context_refs)
reflect correct state at each stage:

1. After job completes: all pins drained, no run_refs remain
2. After background cleanup: tables dropped, context_refs cleaned
"""

from unittest.mock import AsyncMock

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlmodel import select

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.object import Object
from aaiclick.orchestration import tasks_list
from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.background.sqlite_handler import SqliteBackgroundHandler
from aaiclick.orchestration.execution.debug import ajob_test
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


# --- Helpers ---


async def _get_pin_refs(session):
    """Return all (table_name, task_id) from table_pin_refs."""
    result = await session.execute(text("SELECT table_name, task_id FROM table_pin_refs"))
    return {(r[0], r[1]) for r in result.fetchall()}


async def _get_run_refs(session):
    """Return all (table_name, run_id) from table_run_refs."""
    result = await session.execute(text("SELECT table_name, run_id FROM table_run_refs"))
    return {(r[0], r[1]) for r in result.fetchall()}


async def _get_context_tables(session):
    """Return all distinct table_names from table_context_refs."""
    result = await session.execute(text("SELECT DISTINCT table_name FROM table_context_refs"))
    return {r[0] for r in result.fetchall()}


async def _run_cleanup():
    """Run background worker cleanup once using the active SQL engine."""
    engine = _sql_engine_var.get()
    worker = BackgroundWorker()
    worker._engine = engine
    worker._handler = SqliteBackgroundHandler()
    worker._ch_client = AsyncMock()
    await worker._cleanup_unreferenced_tables()


async def _run_and_verify(pipeline_fn):
    """Run job, verify junction table state, then run cleanup."""
    job_obj = await pipeline_fn()
    await ajob_test(job_obj)

    async with get_sql_session() as session:
        db_job = (await session.execute(select(Job).where(Job.id == job_obj.id))).scalar_one()
        assert db_job.status == JobStatus.COMPLETED, f"Job failed: {db_job.error}"

        # After job completes: all pins should be drained (each consumer unpinned)
        pin_refs = await _get_pin_refs(session)
        assert len(pin_refs) == 0, f"Stale pin_refs after job completion: {pin_refs}"

        # After job completes: no active run_refs should remain
        run_refs = await _get_run_refs(session)
        assert len(run_refs) == 0, f"Stale run_refs after job completion: {run_refs}"

        # Context refs should still exist (tables not yet cleaned up)
        context_tables = await _get_context_tables(session)
        temp_tables = {t for t in context_tables if t.startswith("t_")}
        assert len(temp_tables) > 0, "Expected context_refs for temp tables before cleanup"

    # Run background cleanup — should drop all unreferenced tables
    await _run_cleanup()

    async with get_sql_session() as session:
        context_tables = await _get_context_tables(session)
        temp_tables = {t for t in context_tables if t.startswith("t_")}
        assert len(temp_tables) == 0, f"Tables not cleaned up: {temp_tables}"


# --- Tests ---


async def test_single_consumer(orch_ctx):
    """A → B: Object survives for single consumer."""
    await _run_and_verify(single_consumer_pipeline)


async def test_fan_out(orch_ctx):
    """A → (B, C) → D: Object survives for both consumers."""
    await _run_and_verify(fan_out_pipeline)


async def test_chain(orch_ctx):
    """A → B → C: Each intermediate Object survives."""
    await _run_and_verify(chain_pipeline)


async def test_diamond(orch_ctx):
    """A → (B, C) → D: Diamond dependency with shared Object."""
    await _run_and_verify(diamond_pipeline)
