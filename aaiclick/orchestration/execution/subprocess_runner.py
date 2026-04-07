"""Subprocess entry point for task execution.

Accepts a task_id, sets up a full orch_context (SQL + ClickHouse),
loads the task from the database, and runs the complete execution flow:
import → deserialize → execute → serialize → update status → complete job.

Invoked by the worker via asyncio.create_subprocess_exec::

    python -m aaiclick.orchestration.execution.subprocess_runner <task_id>

Exit codes:
    0 — task completed (status written to DB)
    1 — task failed (status written to DB)
    2 — unexpected crash before DB status could be updated
"""

from __future__ import annotations

import asyncio
import signal
import sys
import traceback

from sqlmodel import select

from .claiming import update_task_status
from .runner import execute_task, register_returned_tasks, serialize_task_result
from .worker_helpers import increment_worker_stat, schedule_retry, try_complete_job
from ..models import Task, TaskStatus
from ..orch_context import get_sql_session, orch_context


async def run_task(task_id: int) -> int:
    """Load and execute a single task inside a full orch_context.

    Returns exit code: 0 for success, 1 for handled failure.
    """
    async with orch_context():
        async with get_sql_session() as session:
            result = await session.execute(select(Task).where(Task.id == task_id))
            task = result.scalar_one()

        await update_task_status(task.id, TaskStatus.RUNNING)

        try:
            data_result, log_path = await execute_task(task)
            data_result = await register_returned_tasks(data_result, task.id, task.job_id)
            result_ref = serialize_task_result(data_result, task.job_id)

            await update_task_status(
                task.id,
                TaskStatus.COMPLETED,
                result=result_ref,
                log_path=log_path,
            )
            if task.worker_id is not None:
                await increment_worker_stat(task.worker_id, "tasks_completed")
            await try_complete_job(task.job_id)
            return 0

        except asyncio.CancelledError:
            return 1

        except Exception as e:
            async with get_sql_session() as session:
                row = await session.execute(
                    select(Task.max_retries, Task.attempt).where(Task.id == task.id)
                )
                max_retries, attempt = row.one()

            if attempt < max_retries:
                await schedule_retry(task.id, attempt, str(e))
            else:
                await update_task_status(task.id, TaskStatus.FAILED, error=str(e))
                if task.worker_id is not None:
                    await increment_worker_stat(task.worker_id, "tasks_failed")
                await try_complete_job(task.job_id)
            return 1


def main() -> None:
    """CLI entry: ``python -m aaiclick.orchestration.execution.subprocess_runner <task_id>``."""
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <task_id>", file=sys.stderr)
        sys.exit(2)

    task_id = int(sys.argv[1])

    def _sigterm_handler(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _sigterm_handler)

    try:
        exit_code = asyncio.run(run_task(task_id))
    except (KeyboardInterrupt, SystemExit):
        exit_code = 1
    except Exception:
        traceback.print_exc()
        exit_code = 2

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
