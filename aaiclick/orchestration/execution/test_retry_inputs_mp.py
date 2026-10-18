"""One mp-worker run of a consumer that fails its first attempt; each test
asserts one feature of that run.

Between attempts the run sweeps unreferenced tables, so only the failed
attempt's pins keep the producer's table alive for the retry. Own module:
the run lives in a module fixture on ``orch_module_ctx_no_ch``, which a
per-test reset would wipe.
"""

import pytest
from sqlalchemy import text
from sqlmodel import select

from aaiclick.data.object.refs import native_ref

from ..background.background_worker import BackgroundWorker
from ..background.test_failure_cleanup import run_cleanup_pass, run_failure_cleanup
from ..fixtures.retry_pipelines import retry_pipeline
from ..models import JOB_COMPLETED, TASK_COMPLETED, TASK_FAILED, TERMINAL_JOB_STATUSES, Job, Task
from ..orch_context import get_sql_session
from .mp_worker import mp_worker_main_loop


async def _job(job_id: int) -> Job:
    async with get_sql_session() as session:
        return (await session.execute(select(Job).where(Job.id == job_id))).scalar_one()


@pytest.fixture(scope="module")
async def retried_run(orch_module_ctx_no_ch, tmp_path_factory) -> tuple[Job, Task]:
    """The finished job and its consumer task."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("aaiclick.orchestration.background.background_worker.RETRY_BASE_DELAY", 0.01)
        mp.setattr("aaiclick.orchestration.execution.execution_worker.POLL_INTERVAL", 0.5)
        mp.setattr("aaiclick.orchestration.execution.mp_worker.CHILD_POLL_INTERVAL", 0.1)
        job = await retry_pipeline(marker_file=str(tmp_path_factory.mktemp("retry") / "failed_once"))
        for _ in range(20):
            if (await _job(job.id)).status in TERMINAL_JOB_STATUSES:
                break
            await mp_worker_main_loop(max_tasks=1, install_signal_handlers=False, max_empty_polls=1)
            await run_failure_cleanup()
            await run_cleanup_pass(BackgroundWorker._cleanup_unreferenced_tables)
    async with get_sql_session() as session:
        consumer = (
            await session.execute(select(Task).where(Task.job_id == job.id, Task.name == "total_after_one_failure"))
        ).scalar_one()
    return await _job(job.id), consumer


async def test_retry_reads_its_input(retried_run):
    """The retry still finds the producer's table and computes from it."""
    job, consumer = retried_run
    assert job.status == JOB_COMPLETED, job.error
    assert consumer.result == native_ref(6)


async def test_retry_records_both_attempts(retried_run):
    _, consumer = retried_run
    assert consumer.attempt == 1
    assert consumer.run_statuses == [TASK_FAILED, TASK_COMPLETED]


async def test_success_releases_input_pins(retried_run):
    _, consumer = retried_run
    async with get_sql_session() as session:
        rows = await session.execute(
            text("SELECT table_name FROM table_pin_refs WHERE task_id = :tid"), {"tid": consumer.id}
        )
        assert rows.fetchall() == []
