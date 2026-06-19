"""Tests for the Pod-side _pod_main entrypoint.

Pod-side tests live in their own module because they exercise the same code
path the Pod would run — including ``orch_context()`` booting chdb. We use the
default ``orch_ctx`` (chdb available), exactly like a real Pod; mixing these
into a module that uses ``orch_ctx_no_ch`` would force two chdb-mode contexts
in one module (see the chdb single-session constraint in ``docs/testing.md``).
"""

from __future__ import annotations

from sqlmodel import select

from ..factories import create_job
from ..models import TASK_RUNNING, Task, RemoteTaskResult
from ..orch_context import get_sql_session
from . import kubernetes_worker as kw


async def test_pod_main_writes_success_row(orch_ctx):
    """The Pod entrypoint runs a real task and writes a success RemoteTaskResult
    row the host reads back."""
    job = await create_job(
        "test_pod_main_success",
        "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
    )
    async with get_sql_session() as session:
        task = (await session.execute(select(Task).where(Task.job_id == job.id))).scalar_one()

    exit_code = await kw._pod_main(task.id, run_epoch=task.run_epoch)
    assert exit_code == 0

    async with get_sql_session() as session:
        row = (
            await session.execute(
                select(RemoteTaskResult).where(RemoteTaskResult.task_id == task.id, RemoteTaskResult.run_epoch == task.run_epoch)
            )
        ).scalar_one()
    assert row.success is True
    assert row.error is None
    assert row.log_path  # path to the captured log file


async def test_pod_main_writes_failure_row_on_exception(orch_ctx):
    job = await create_job(
        "test_pod_main_failure",
        "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
    )
    async with get_sql_session() as session:
        task = (await session.execute(select(Task).where(Task.job_id == job.id))).scalar_one()

    exit_code = await kw._pod_main(task.id, run_epoch=task.run_epoch)
    assert exit_code == 1

    async with get_sql_session() as session:
        row = (
            await session.execute(
                select(RemoteTaskResult).where(RemoteTaskResult.task_id == task.id, RemoteTaskResult.run_epoch == task.run_epoch)
            )
        ).scalar_one()
    assert row.success is False
    assert "intentionally" in (row.error or "")


async def test_pod_main_does_not_write_terminal_task_status(orch_ctx):
    """Reaper invariant: the Pod never flips the Task to a terminal status —
    only run_ids / run_statuses are appended (TASK_RUNNING for the attempt)."""
    job = await create_job(
        "test_pod_main_invariant",
        "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
    )
    async with get_sql_session() as session:
        task = (await session.execute(select(Task).where(Task.job_id == job.id))).scalar_one()
        original_status = task.status

    await kw._pod_main(task.id, run_epoch=task.run_epoch)

    async with get_sql_session() as session:
        task_after = (await session.execute(select(Task).where(Task.id == task.id))).scalar_one()
    assert task_after.status == original_status
    assert task_after.run_statuses == [TASK_RUNNING]
