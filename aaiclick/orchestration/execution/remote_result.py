"""Cross-boundary task-result transport for container runners.

Both container runners (docker / kubernetes) run their module tasks in a
remote process that cannot hand a Python object back to the host worker.
The contract lives here: the container-side entrypoint executes the task
and writes a ``RemoteTaskResult`` row keyed by ``(task_id, run_epoch)``;
the host worker reads the row back after the container exits and folds it
into a ``RunnerResult`` via :func:`collect_remote_result`.

Reaper invariant: the entrypoint never writes terminal task status. It
writes only its own ``remote_task_results`` row; the host worker writes
the terminal ``Task`` status via ``_handle_task_result``.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from sqlmodel import select

from ..models import RemoteTaskResult, Task
from ..orch_context import get_sql_session, orch_context
from .execution_worker import RunnerResult
from .runner import execute_task, serialize_task_result

REMOTE_ENTRYPOINT = ["python", "-m", "aaiclick.orchestration.execution.remote_result"]


async def write_task_run_result(
    task_id: int, run_epoch: int, success: bool, result_ref: dict | None, error: str | None
) -> None:
    """Upsert the per-attempt result row the host reads back. Keyed by
    ``(task_id, run_epoch)`` so a re-run under a new epoch never collides."""
    async with get_sql_session() as session:
        existing = (
            await session.execute(
                select(RemoteTaskResult).where(
                    RemoteTaskResult.task_id == task_id, RemoteTaskResult.run_epoch == run_epoch
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            existing = RemoteTaskResult(task_id=task_id, run_epoch=run_epoch, success=success)
            session.add(existing)
        existing.success = success
        existing.result_ref = result_ref
        existing.error = error
        await session.commit()


async def read_task_run_result(task_id: int, run_epoch: int) -> RunnerResult | None:
    async with get_sql_session() as session:
        row = (
            await session.execute(
                select(RemoteTaskResult).where(
                    RemoteTaskResult.task_id == task_id, RemoteTaskResult.run_epoch == run_epoch
                )
            )
        ).scalar_one_or_none()
    if row is None:
        return None
    return RunnerResult(row.success, row.result_ref, row.error)


def collect_remote_result(
    exit_code: int, error: str | None, was_cancelled: bool, payload: RunnerResult | None, vehicle_name: str
) -> RunnerResult:
    """Fold an exited vehicle plus its result row into a ``RunnerResult``.

    A fired cancellation or vehicle error overrides whatever the container
    wrote — the host's explicit kill is the source of truth."""
    if was_cancelled:
        return RunnerResult(False, None, "cancelled")
    if error is not None:
        return RunnerResult(False, None, error)
    if payload is None:
        return RunnerResult(False, None, f"{vehicle_name} exited with code {exit_code} but wrote no result row")
    return payload


async def remote_entry_main(task_id: int, run_epoch: int) -> int:
    """Entry point invoked inside the container/Pod. Runs the task via the
    shared ``execute_task`` path and writes a ``RemoteTaskResult`` row for
    the host."""
    success, result_ref, error = False, None, None
    exit_code = 0
    # orch_context wraps both execution and the result write — the latter
    # needs an active SQL session.
    async with orch_context():
        try:
            async with get_sql_session() as session:
                task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()
            data_result = await execute_task(task)
            result_ref = serialize_task_result(data_result, task.job_id)
            success = True
        except BaseException as e:
            success, error, exit_code = False, f"{type(e).__name__}: {e}", 1

        await write_task_run_result(task_id, run_epoch, success, result_ref, error)
    return exit_code


def _remote_entry_cli() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m aaiclick.orchestration.execution.remote_result",
        description="Container-side entrypoint for the docker and kubernetes runners.",
    )
    parser.add_argument("--task-id", type=int, required=True)
    parser.add_argument("--run-epoch", type=int, required=True)
    args = parser.parse_args()
    sys.exit(asyncio.run(remote_entry_main(args.task_id, args.run_epoch)))


if __name__ == "__main__":
    _remote_entry_cli()
