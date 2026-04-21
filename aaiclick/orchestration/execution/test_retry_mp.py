"""Retry-logic tests that spawn multiprocessing workers.

Kept in a dedicated module so ``orch_ctx_no_ch`` can be module-scoped
(the parent process never opens chdb, leaving the file lock free for
each spawned child). Shares ``_cancel_all_pending_tasks`` and
``_run_until_terminal`` with ``test_retry``.
"""

from sqlmodel import select

from ..background.test_pending_cleanup import run_pending_cleanup  # noqa: E402  (test-sibling import)
from ..factories import create_job, create_task
from ..models import Job, JobStatus, Task, TaskStatus
from ..orch_context import get_sql_session
from .mp_worker import mp_worker_main_loop
from .test_retry import _cancel_all_pending_tasks, _run_until_terminal


async def test_worker_retries_and_exhausts(orch_ctx_no_ch, fast_poll):
    """Worker retries a failing task until max_retries exhausted, then marks FAILED."""
    await _cancel_all_pending_tasks()

    job = await create_job(
        "test_retry_worker",
        create_task(
            "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
            max_retries=2,
        ),
    )

    await _run_until_terminal(job.id)

    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        t = result.scalar_one()
        assert t.status == TaskStatus.FAILED
        assert t.attempt == 2
        assert t.max_retries == 2
        assert t.error is not None

    async with get_sql_session() as session:
        result = await session.execute(select(Job).where(Job.id == job.id))
        j = result.scalar_one()
        assert j.status == JobStatus.FAILED


async def test_worker_no_retries_immediate_fail(orch_ctx_no_ch, fast_poll):
    """Task with max_retries=0 fails immediately after background cleanup."""
    await _cancel_all_pending_tasks()

    job = await create_job(
        "test_no_retry",
        create_task(
            "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
            max_retries=0,
        ),
    )

    await mp_worker_main_loop(
        max_tasks=1,
        install_signal_handlers=False,
        max_empty_polls=1,
    )

    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        t = result.scalar_one()
        assert t.status == TaskStatus.PENDING_CLEANUP

    await run_pending_cleanup()

    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        t = result.scalar_one()
        assert t.status == TaskStatus.FAILED
        assert t.attempt == 0
        assert t.error is not None

    async with get_sql_session() as session:
        result = await session.execute(select(Job).where(Job.id == job.id))
        j = result.scalar_one()
        assert j.status == JobStatus.FAILED


async def test_worker_retry_succeeds_on_third_attempt(orch_ctx_no_ch, tmp_path, fast_poll):
    """Flaky task fails twice, succeeds on the third attempt via retry."""
    await _cancel_all_pending_tasks()

    counter_file = str(tmp_path / "counter.txt")

    t = create_task(
        "aaiclick.orchestration.fixtures.sample_tasks.flaky_task",
        {"counter_file": counter_file},
        max_retries=2,
    )
    job = await create_job("test_retry_succeeds", t)

    await _run_until_terminal(job.id)

    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        t = result.scalar_one()
        assert t.status == TaskStatus.COMPLETED
        assert t.attempt == 2

    async with get_sql_session() as session:
        result = await session.execute(select(Job).where(Job.id == job.id))
        j = result.scalar_one()
        assert j.status == JobStatus.COMPLETED
