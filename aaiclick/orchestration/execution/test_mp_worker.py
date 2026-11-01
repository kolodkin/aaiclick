"""Tests for multiprocessing worker."""

import asyncio

import pytest
from sqlmodel import select

from ...testing import get_task_runs
from ..background.test_cancelled_cleanup import run_cancelled_cleanup
from ..factories import create_job, create_task
from ..models import TASK_CANCELLED, TASK_COMPLETED, TASK_PENDING_FAILURE_CLEANUP, TASK_RUNNING, Task
from ..orch_context import get_ch_client, get_sql_session
from . import mp_worker
from .claiming import cancel_job
from .mp_worker import mp_worker_main_loop

pytestmark = pytest.mark.usefixtures("fast_poll")


async def test_mp_worker_executes_task(orch_ctx_no_ch):
    """Test that mp worker executes a task in a child process."""
    job = await create_job(
        "test_mp_job",
        "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
    )

    tasks_executed = await mp_worker_main_loop(
        max_tasks=1,
        install_signal_handlers=False,
        max_empty_polls=1,
    )

    assert tasks_executed == 1

    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        task = result.scalar_one()
        assert task.status == TASK_COMPLETED


async def test_mp_worker_handles_failure(orch_ctx_no_ch):
    """Test that mp worker handles task failures from child process.

    Asserts the recorded error matches the message raised by ``failing_task``
    so a child crash on chdb setup, an exit-code-only failure, or a timeout
    can't masquerade as a passing test.
    """
    job = await create_job(
        "test_mp_failing_job",
        "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
    )

    tasks_executed = await mp_worker_main_loop(
        max_tasks=1,
        install_signal_handlers=False,
        max_empty_polls=1,
    )

    assert tasks_executed == 0

    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        task = result.scalar_one()
        assert task.status == TASK_PENDING_FAILURE_CLEANUP
        assert task.error == "This task failed intentionally"


async def test_mp_worker_no_tasks(orch_ctx_no_ch):
    """Test that mp worker exits after max_empty_polls with no tasks."""
    tasks_executed = await mp_worker_main_loop(
        install_signal_handlers=False,
        max_empty_polls=1,
    )

    assert tasks_executed == 0


async def test_mp_worker_main_loop_rejects_bad_runner(monkeypatch):
    """The loop that dispatches container tasks validates AAICLICK_RUNNER
    before claiming anything, whoever starts it (CLI or e2e harness)."""
    monkeypatch.setenv("AAICLICK_RUNNER", "podman")
    with pytest.raises(ValueError, match="AAICLICK_RUNNER"):
        await mp_worker_main_loop(max_tasks=1, install_signal_handlers=False, max_empty_polls=1)


async def test_mp_worker_main_loop_rejects_chdb_with_runner(monkeypatch, tmp_path):
    """Container task logs are written from the parent, where a chdb session
    would lock every later task's child out of the database."""
    monkeypatch.setenv("AAICLICK_CH_URL", f"chdb://{tmp_path}")
    monkeypatch.setenv("AAICLICK_RUNNER", "docker")
    with pytest.raises(ValueError, match="cannot combine chdb"):
        await mp_worker_main_loop(max_tasks=1, install_signal_handlers=False, max_empty_polls=1)


async def test_mp_worker_main_loop_refuses_clickhouse(orch_ctx_no_ch, monkeypatch):
    """The parent claims and updates tasks over SQL only; a ClickHouse call
    there would take the chdb lock its children need."""

    async def loop_body(**_kwargs) -> int:
        with pytest.raises(RuntimeError, match="must not query ClickHouse"):
            await get_ch_client().query("SELECT 1")
        return 0

    monkeypatch.setattr(mp_worker, "_execution_worker_loop", loop_body)
    assert await mp_worker_main_loop(install_signal_handlers=False) == 0


async def _task_row(job_id: int) -> Task:
    async with get_sql_session() as session:
        return (await session.execute(select(Task).where(Task.job_id == job_id))).scalar_one()


async def test_mp_worker_kills_cancelled_module_task(orch_ctx_no_ch):
    """Cancelling a running module task kills its child instead of waiting for
    it to finish; the task then settles to CANCELLED."""
    job = await create_job(
        "test_mp_cancel",
        create_task("aaiclick.orchestration.fixtures.sample_tasks.slow_task", {"seconds": 60.0, "steps": 600}),
    )
    loop = asyncio.create_task(mp_worker_main_loop(max_tasks=1, install_signal_handlers=False, max_empty_polls=1))
    while [r.status for r in await get_task_runs((await _task_row(job.id)).id)] != [TASK_RUNNING]:
        await asyncio.sleep(0.05)

    await cancel_job(job.id)
    await asyncio.wait_for(loop, timeout=15)

    assert (await _task_row(job.id)).execution_worker_id is None
    await run_cancelled_cleanup()
    assert (await _task_row(job.id)).status == TASK_CANCELLED
