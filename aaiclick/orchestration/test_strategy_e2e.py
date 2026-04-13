"""
End-to-end integration test for STRATEGY preservation mode.

Submits a real job via ``create_job()`` + ``run_job_tasks()`` (exercising
``execute_task`` and the full orch-context plumbing) and verifies that
``operation_log.kwargs_aai_ids`` / ``result_aai_ids`` carry the
strategy-matched ids after the job completes.

This closes the gap that unit tests on ``task_scope`` alone can't cover:
the Job → sampling_strategy round trip through SQL + the runner.
"""

from __future__ import annotations

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.data_context.ch_client import create_ch_client
from aaiclick.data.object import Object
from aaiclick.orchestration.decorators import task
from aaiclick.orchestration.execution.runner import run_job_tasks
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.models import Job, JobStatus, PreservationMode
from aaiclick.orchestration.orch_context import get_sql_session
from sqlmodel import select


@task
async def _make_left() -> Object:
    return await create_object_from_value([10, 20, 30], name="strat_e2e_left")


@task
async def _make_right() -> Object:
    return await create_object_from_value([1, 2, 3], name="strat_e2e_right")


@task
async def _add(left: Object, right: Object) -> Object:
    return await (left + right)


@task
async def _strat_e2e_pipeline() -> Object:
    left = _make_left()
    right = _make_right()
    return _add(left=left, right=right)


async def test_strategy_mode_e2e_populates_lineage(orch_ctx):
    """Full job execution under STRATEGY mode populates kwargs_aai_ids / result_aai_ids."""
    job = await create_job(
        "strat_e2e",
        _strat_e2e_pipeline,
        preservation_mode=PreservationMode.STRATEGY,
        sampling_strategy={"p_strat_e2e_left": "value = 10"},
    )

    await run_job_tasks(job)

    async with get_sql_session() as session:
        db_job = (
            await session.execute(select(Job).where(Job.id == job.id))
        ).scalar_one()
    assert db_job.status == JobStatus.COMPLETED, f"Job failed: {db_job.error}"
    assert db_job.preservation_mode is PreservationMode.STRATEGY
    assert db_job.sampling_strategy == {"p_strat_e2e_left": "value = 10"}

    ch = await create_ch_client()
    rows = (
        await ch.query(
            "SELECT result_table, operation, kwargs_aai_ids, result_aai_ids "
            f"FROM operation_log WHERE job_id = {job.id}"
        )
    ).result_rows
    assert rows, f"No oplog rows recorded for job {job.id}"

    # At least one row (the binary + operation) should carry strategy-matched ids.
    # The left source table has the strategy applied, so the + op's
    # kwargs_aai_ids['left'] should contain exactly one id.
    add_rows = [
        (op, k, r) for _, op, k, r in rows if op == "+"
    ]
    assert add_rows, f"No '+' oplog row; got operations: {[r[1] for r in rows]}"
    _, kwargs_aai_ids_raw, result_aai_ids_raw = add_rows[0]
    kwargs_aai_ids = (
        dict(kwargs_aai_ids_raw)
        if not isinstance(kwargs_aai_ids_raw, dict)
        else kwargs_aai_ids_raw
    )
    result_aai_ids = list(result_aai_ids_raw)

    assert "left" in kwargs_aai_ids
    assert "right" in kwargs_aai_ids
    assert len(result_aai_ids) == 1
    assert len(kwargs_aai_ids["left"]) == 1
    assert len(kwargs_aai_ids["right"]) == 1

    # Drop the persistent inputs so the next test run starts clean.
    await ch.command("DROP TABLE IF EXISTS p_strat_e2e_left")
    await ch.command("DROP TABLE IF EXISTS p_strat_e2e_right")


async def test_none_mode_e2e_leaves_lineage_empty(orch_ctx):
    """Full job execution under NONE mode leaves lineage id arrays empty."""
    job = await create_job(
        "none_e2e",
        _strat_e2e_pipeline,
        preservation_mode=PreservationMode.NONE,
    )

    await run_job_tasks(job)

    async with get_sql_session() as session:
        db_job = (
            await session.execute(select(Job).where(Job.id == job.id))
        ).scalar_one()
    assert db_job.status == JobStatus.COMPLETED, f"Job failed: {db_job.error}"

    ch = await create_ch_client()
    rows = (
        await ch.query(
            "SELECT kwargs_aai_ids, result_aai_ids FROM operation_log "
            f"WHERE job_id = {job.id}"
        )
    ).result_rows
    assert rows, f"No oplog rows for job {job.id}"
    for kwargs_aai_ids_raw, result_aai_ids in rows:
        kwargs_aai_ids = (
            dict(kwargs_aai_ids_raw)
            if not isinstance(kwargs_aai_ids_raw, dict)
            else kwargs_aai_ids_raw
        )
        assert kwargs_aai_ids == {}
        assert list(result_aai_ids) == []

    await ch.command("DROP TABLE IF EXISTS p_strat_e2e_left")
    await ch.command("DROP TABLE IF EXISTS p_strat_e2e_right")
