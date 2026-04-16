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

from typing import Any

from sqlmodel import select

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.data_context.ch_client import create_ch_client
from aaiclick.data.object import Object
from aaiclick.orchestration.decorators import task
from aaiclick.orchestration.execution.runner import run_job_tasks
from aaiclick.orchestration.factories import create_job, create_task
from aaiclick.orchestration.models import Job, JobStatus, PreservationMode
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.snowflake_id import get_snowflake_id

# Task fixtures accept the persistent table suffix so each test gets unique
# p_* tables. That lets parallel xdist workers share a real ClickHouse
# server without colliding on table names.


@task
async def _make_left(suffix: str) -> Object:
    return await create_object_from_value([10, 20, 30], name=f"strat_e2e_left_{suffix}")


@task
async def _make_right(suffix: str) -> Object:
    return await create_object_from_value([1, 2, 3], name=f"strat_e2e_right_{suffix}")


@task
async def _add(left: Object, right: Object) -> Object:
    return await (left + right)


@task
async def _strat_e2e_pipeline(suffix: str) -> Any:
    left = _make_left(suffix=suffix)
    right = _make_right(suffix=suffix)
    return _add(left=left, right=right)


def _entry_task(suffix: str):
    return create_task(_strat_e2e_pipeline, {"suffix": suffix})


async def _assert_completed(job: Job) -> None:
    async with get_sql_session() as session:
        db_job = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
    assert db_job.status == JobStatus.COMPLETED, f"Job failed: {db_job.error}"


async def test_strategy_mode_e2e_populates_lineage(orch_ctx):
    """Full job execution under STRATEGY mode populates kwargs_aai_ids / result_aai_ids."""
    suffix = str(get_snowflake_id())
    left_table = f"p_strat_e2e_left_{suffix}"
    right_table = f"p_strat_e2e_right_{suffix}"

    job = await create_job(
        f"strat_e2e_{suffix}",
        _entry_task(suffix),
        preservation_mode=PreservationMode.STRATEGY,
        sampling_strategy={left_table: "value = 10"},
    )

    ch = await create_ch_client()
    try:
        await run_job_tasks(job)
        await _assert_completed(job)

        rows = (
            await ch.query(
                f"SELECT operation, kwargs_aai_ids, result_aai_ids FROM operation_log WHERE job_id = {job.id}"
            )
        ).result_rows
        assert rows, f"No oplog rows for job {job.id}"

        add_rows = [(op, k, r) for op, k, r in rows if op == "+"]
        assert add_rows, f"No '+' oplog row; got operations: {[r[0] for r in rows]}"
        _, kwargs_aai_ids_raw, result_aai_ids_raw = add_rows[0]
        kwargs_aai_ids = dict(kwargs_aai_ids_raw) if not isinstance(kwargs_aai_ids_raw, dict) else kwargs_aai_ids_raw
        result_aai_ids = list(result_aai_ids_raw)

        assert set(kwargs_aai_ids.keys()) == {"left", "right"}
        assert len(result_aai_ids) == 1
        assert len(kwargs_aai_ids["left"]) == 1
        assert len(kwargs_aai_ids["right"]) == 1
    finally:
        await ch.command(f"DROP TABLE IF EXISTS {left_table}")
        await ch.command(f"DROP TABLE IF EXISTS {right_table}")


async def test_none_mode_e2e_leaves_lineage_empty(orch_ctx):
    """Full job execution under NONE mode leaves lineage id arrays empty."""
    suffix = str(get_snowflake_id())
    left_table = f"p_strat_e2e_left_{suffix}"
    right_table = f"p_strat_e2e_right_{suffix}"

    job = await create_job(
        f"none_e2e_{suffix}",
        _entry_task(suffix),
        preservation_mode=PreservationMode.NONE,
    )

    ch = await create_ch_client()
    try:
        await run_job_tasks(job)
        await _assert_completed(job)

        rows = (
            await ch.query(f"SELECT kwargs_aai_ids, result_aai_ids FROM operation_log WHERE job_id = {job.id}")
        ).result_rows
        assert rows, f"No oplog rows for job {job.id}"
        for kwargs_aai_ids_raw, result_aai_ids in rows:
            kwargs_aai_ids = (
                dict(kwargs_aai_ids_raw) if not isinstance(kwargs_aai_ids_raw, dict) else kwargs_aai_ids_raw
            )
            assert kwargs_aai_ids == {}
            assert list(result_aai_ids) == []
    finally:
        await ch.command(f"DROP TABLE IF EXISTS {left_table}")
        await ch.command(f"DROP TABLE IF EXISTS {right_table}")
