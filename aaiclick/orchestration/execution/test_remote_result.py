"""Tests for the shared container-side entrypoint and result transport.

Entrypoint tests live in their own module because they exercise the same
code path the container/Pod would run — including ``orch_context()``
booting chdb; ``orch_ctx`` (not ``orch_ctx_no_ch``) matches a real
container. See the chdb single-session constraint in
``docs/designs/testing.md``.
"""

from __future__ import annotations

from sqlmodel import select

from ..factories import create_job
from ..models import TASK_RUNNING, RemoteTaskResult, Task
from ..orch_context import get_sql_session
from .execution_worker import RunnerResult
from .remote_result import collect_remote_result, remote_entry_main


async def _single_task(job_id: int) -> Task:
    async with get_sql_session() as session:
        return (await session.execute(select(Task).where(Task.job_id == job_id))).scalar_one()


async def _result_row(task: Task) -> RemoteTaskResult:
    async with get_sql_session() as session:
        return (
            await session.execute(
                select(RemoteTaskResult).where(
                    RemoteTaskResult.task_id == task.id, RemoteTaskResult.run_epoch == task.run_epoch
                )
            )
        ).scalar_one()


async def test_remote_entry_main_writes_success_row(orch_ctx):
    """The entrypoint runs a real task and writes a success RemoteTaskResult
    row the host reads back."""
    job = await create_job(
        "test_remote_entry_success",
        "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
    )
    task = await _single_task(job.id)

    exit_code = await remote_entry_main(task.id, run_epoch=task.run_epoch)
    assert exit_code == 0

    row = await _result_row(task)
    assert row.success is True
    assert row.error is None
    # `simple_task` returns None; result_ref should reflect that
    assert row.result_ref is None


async def test_remote_entry_main_writes_failure_row_on_exception(orch_ctx):
    """Failed user code must produce a failure row and exit non-zero — the
    host depends on the exit code as a fast failure signal too."""
    job = await create_job(
        "test_remote_entry_failure",
        "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
    )
    task = await _single_task(job.id)

    exit_code = await remote_entry_main(task.id, run_epoch=task.run_epoch)
    assert exit_code == 1

    row = await _result_row(task)
    assert row.success is False
    assert row.result_ref is None
    assert "intentionally" in (row.error or "")


async def test_remote_entry_main_does_not_write_terminal_task_status(orch_ctx):
    """Reaper invariant: the entrypoint never flips the Task to a terminal
    status — only run_ids / run_statuses are appended (TASK_RUNNING for the
    attempt)."""
    job = await create_job(
        "test_remote_entry_invariant",
        "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
    )
    task = await _single_task(job.id)
    original_status = task.status

    await remote_entry_main(task.id, run_epoch=task.run_epoch)

    async with get_sql_session() as session:
        task_after = (await session.execute(select(Task).where(Task.id == task.id))).scalar_one()
    assert task_after.status == original_status
    assert task_after.error is None
    assert len(task_after.run_ids) == 1
    assert task_after.run_statuses == [TASK_RUNNING]


def test_collect_remote_result_returns_payload():
    payload = RunnerResult(True, {"foo": 1}, None)
    result = collect_remote_result(0, None, False, payload, "container")
    assert result == payload


def test_collect_remote_result_synthesizes_failure_when_row_missing():
    result = collect_remote_result(137, None, False, None, "container")
    assert result.success is False
    assert result.error and "exited with code 137" in result.error


def test_collect_remote_result_cancellation_overrides_payload():
    """Even if the container managed to write a success row before being
    killed, a cancellation flag must override it — the host's explicit
    kill is the source of truth."""
    payload = RunnerResult(True, {}, None)
    result = collect_remote_result(137, None, True, payload, "pod")
    assert result.success is False
    assert result.error == "cancelled"


def test_collect_remote_result_vehicle_error_overrides_payload():
    """Timeout error from the vehicle wait takes precedence over the row —
    same reasoning as cancellation."""
    result = collect_remote_result(-1, "Task timed out after 60.0s", False, None, "container")
    assert result.success is False
    assert "timed out" in (result.error or "")
