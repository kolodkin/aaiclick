"""Integration tests for oplog provenance wiring and table lifecycle cleanup."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlmodel import select

from aaiclick.data.ch_client import create_ch_client
from aaiclick.orchestration.context import get_orch_session
from aaiclick.orchestration.env import get_db_url
from aaiclick.orchestration.execution import run_job_tasks
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.models import JobStatus, Task
from aaiclick.orchestration.pg_cleanup import PgCleanupWorker


async def test_execute_task_populates_oplog(orch_ctx):
    """Tasks executed via run_job_tasks automatically populate operation_log."""
    async with get_orch_session() as session:
        job = await create_job(
            "test_oplog_provenance",
            "aaiclick.orchestration.fixtures.sample_tasks.data_task",
        )
        job_id = job.id

    await run_job_tasks(job)

    assert job.status == JobStatus.COMPLETED

    ch = await create_ch_client()
    result = await ch.query(
        f"SELECT count() FROM operation_log WHERE job_id = {job_id}"
    )
    count = result.result_rows[0][0]
    assert count > 0, "operation_log should have entries for the job"


async def test_execute_task_oplog_has_task_id(orch_ctx):
    """operation_log entries include the correct task_id and job_id."""
    async with get_orch_session() as session:
        job = await create_job(
            "test_oplog_task_id",
            "aaiclick.orchestration.fixtures.sample_tasks.data_task",
        )
        job_id = job.id
        task_result = await session.execute(select(Task).where(Task.job_id == job_id))
        task_id = task_result.scalar_one().id

    await run_job_tasks(job)

    ch = await create_ch_client()
    result = await ch.query(
        f"SELECT task_id, job_id FROM operation_log "
        f"WHERE job_id = {job_id} LIMIT 1"
    )
    assert result.result_rows, "Should have at least one oplog entry"
    row_task_id, row_job_id = result.result_rows[0]
    assert row_task_id == task_id
    assert row_job_id == job_id


async def test_execute_task_registers_tables_in_registry(orch_ctx):
    """Tables created during task execution are registered in table_registry."""
    async with get_orch_session() as session:
        job = await create_job(
            "test_table_registry",
            "aaiclick.orchestration.fixtures.sample_tasks.data_task",
        )
        job_id = job.id

    await run_job_tasks(job)

    ch = await create_ch_client()
    result = await ch.query(
        f"SELECT count() FROM table_registry WHERE job_id = {job_id}"
    )
    count = result.result_rows[0][0]
    assert count > 0, "table_registry should have entries for the job"


async def test_sample_completed_job_tables_replaces_with_sample(orch_ctx):
    """Completed job tables are replaced with 10-row samples after cleanup."""
    async with get_orch_session() as session:
        job = await create_job(
            "test_lifecycle_sample",
            "aaiclick.orchestration.fixtures.sample_tasks.data_task",
        )
        job_id = job.id

    await run_job_tasks(job)
    assert job.status == JobStatus.COMPLETED

    ch = await create_ch_client()

    # Find the table created by the task
    result = await ch.query(
        f"SELECT table_name FROM table_registry WHERE job_id = {job_id} LIMIT 1"
    )
    assert result.result_rows, "table_registry should have entries"
    table_name = result.result_rows[0][0]

    # Verify table has more than 10 rows before sampling
    row_result = await ch.query(f"SELECT count() FROM {table_name}")
    original_count = row_result.result_rows[0][0]
    assert original_count > 10, "Task should have created 12 rows"

    # Run the cleanup worker's sampling step
    worker = PgCleanupWorker()
    await worker.start()
    try:
        await worker._sample_completed_job_tables()
    finally:
        await worker.stop()

    # Table still exists but with at most 10 rows
    row_result = await ch.query(f"SELECT count() FROM {table_name}")
    sampled_count = row_result.result_rows[0][0]
    assert sampled_count <= 10, f"Sampled table should have ≤10 rows, got {sampled_count}"

    # Table is removed from table_registry
    reg_result = await ch.query(
        f"SELECT count() FROM table_registry WHERE job_id = {job_id}"
    )
    assert reg_result.result_rows[0][0] == 0, "table_registry entries should be removed"


async def test_sample_completed_job_tables_persists_table(orch_ctx):
    """Sampled tables are excluded from lifecycle cleanup and persist."""
    async with get_orch_session() as session:
        job = await create_job(
            "test_lifecycle_persist",
            "aaiclick.orchestration.fixtures.sample_tasks.data_task",
        )
        job_id = job.id

    await run_job_tasks(job)

    ch = await create_ch_client()

    # Find the table created by the task
    result = await ch.query(
        f"SELECT table_name FROM table_registry WHERE job_id = {job_id} LIMIT 1"
    )
    table_name = result.result_rows[0][0]

    # Run sampling
    worker = PgCleanupWorker()
    await worker.start()
    try:
        await worker._sample_completed_job_tables()
    finally:
        await worker.stop()

    # Sampled table should no longer be in table_context_refs
    engine = create_async_engine(get_db_url(), echo=False)
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT count(*) FROM table_context_refs WHERE table_name = :t"),
            {"t": table_name},
        )
        count = result.scalar()
    await engine.dispose()

    assert count == 0, "Sampled table should be removed from table_context_refs"

    # Table still exists in ClickHouse
    exists = await ch.command(f"EXISTS TABLE {table_name}")
    assert exists, "Sampled table should still exist in ClickHouse"
