"""End-to-end test for Object lifecycle with per-consumer pins.

Hooks into SQLAlchemy's engine events to capture all pin/unpin and
incref/decref activity on the junction tables, then verifies the
audit trail matches expectations.
"""

from typing import NamedTuple
from unittest.mock import AsyncMock

from sqlalchemy import event, text
from sqlmodel import select

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.object import Object
from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.background.sqlite_handler import SqliteBackgroundHandler
from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.execution.debug import run_job_tasks
from aaiclick.orchestration.models import Job, JobStatus
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.sql_context import _sql_engine_var
from aaiclick.testing import with_value_order

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
    # Cross-table array+array requires explicit order on both sides (Phase 4 contract).
    return await (with_value_order(a) + with_value_order(b))


@task
async def read_sum(data: Object) -> dict:
    result = await data.sum()
    return {"total": await result.data()}


@task
async def paginate_and_concat() -> Object:
    """Simulate load_shodan_kev_cves: create Views mid-loop, then concat.

    Regression test for the View lifecycle bug where page["field"].count()
    would decref the underlying table, allowing it to be prematurely dropped
    before the next concat iteration could reference it.
    """
    page_size = 3
    result = None
    for batch in [[1, 2, 3], [4, 5, 6], [7, 8]]:
        page = await create_object_from_value(batch)
        # This view creation+count was the bug trigger: View called incref (no-op
        # due to ON CONFLICT DO NOTHING), then decref on GC, leaving 0 run_refs.
        count = await (await page["value"].count()).data()
        result = page if result is None else await result.concat(page)
        if count < page_size:
            break
    assert result is not None
    return result


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


@job("lifecycle_view_concat")
def view_concat_pipeline():
    data = paginate_and_concat()
    return read_sum(data=data)


# --- Audit trail ---

TRACKED_TABLES = {"table_pin_refs", "table_run_refs", "table_context_refs"}


class RefEvent(NamedTuple):
    action: str  # INSERT or DELETE
    table: str  # table_pin_refs, table_run_refs, table_context_refs


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
    assert engine is not None
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

        # Verify pin/unpin pairs (per-consumer: producer fans out, consumer unpins)
        pin_inserts = [e for e in events if e.table == "table_pin_refs" and e.action == "INSERT"]
        pin_deletes = [e for e in events if e.table == "table_pin_refs" and e.action == "DELETE"]
        assert len(pin_inserts) > 0, "No PINs recorded"
        assert len(pin_deletes) > 0, "No UNPINs recorded"
        assert len(pin_inserts) == len(pin_deletes), (
            f"PIN/UNPIN mismatch: {len(pin_inserts)} pins vs {len(pin_deletes)} unpins"
        )

        # Verify incref/decref pairs
        run_inserts = [e for e in events if e.table == "table_run_refs" and e.action == "INSERT"]
        run_deletes = [e for e in events if e.table == "table_run_refs" and e.action == "DELETE"]
        assert len(run_inserts) > 0, "No INCREFs recorded"
        assert len(run_deletes) > 0, "No DECREFs recorded"
        assert len(run_inserts) == len(run_deletes), (
            f"INCREF/DECREF mismatch: {len(run_inserts)} inserts vs {len(run_deletes)} deletes"
        )

        # After job completes: all pins drained, no run_refs
        async with get_sql_session() as session:
            pin_refs = await _get_pin_refs(session)
            assert len(pin_refs) == 0, f"Stale pin_refs after job: {pin_refs}"

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
    """A → B: one pin for the single consumer, one unpin."""
    events = await _run_and_verify(single_consumer_pipeline)
    pins = [e for e in events if e.table == "table_pin_refs" and e.action == "INSERT"]
    # produce() result → read_sum(): 1 consumer pin
    assert len(pins) >= 1


async def test_fan_out(orch_ctx):
    """A → (B, C) → D: produce() result pinned for both double() and add_ten()."""
    events = await _run_and_verify(fan_out_pipeline)
    pins = [e for e in events if e.table == "table_pin_refs" and e.action == "INSERT"]
    # produce() fans out to double + add_ten = 2 pins for that table
    assert len(pins) >= 2


async def test_chain(orch_ctx):
    """A → B → C: each link gets its own pin."""
    events = await _run_and_verify(chain_pipeline)
    pins = [e for e in events if e.table == "table_pin_refs" and e.action == "INSERT"]
    # produce→double: 1 pin, double→read_sum: 1 pin
    assert len(pins) >= 2


async def test_diamond(orch_ctx):
    """A → (B, C) → D: diamond with per-consumer pins."""
    events = await _run_and_verify(diamond_pipeline)
    pins = [e for e in events if e.table == "table_pin_refs" and e.action == "INSERT"]
    # produce→(double, add_ten): 2 pins
    # double→add_objects: 1 pin
    # add_ten→add_objects: 1 pin
    # add_objects→read_sum: 1 pin
    assert len(pins) >= 5


async def test_view_concat_lifecycle(orch_ctx):
    """View created mid-loop does not prematurely decref the underlying table.

    Regression: page["value"].count() used to create a View that decreffed
    page.table on GC, leaving 0 run_refs and making the table eligible for
    background cleanup before the next concat iteration could use it.

    INCREF/DECREF balance is verified inside _run_and_verify (before cleanup).
    The returned events also include cleanup-phase DELETEs from _delete_table_refs,
    so we only assert that the job completed and tables were cleaned up — both
    are checked by _run_and_verify itself.
    """
    await _run_and_verify(view_concat_pipeline)
