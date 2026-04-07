"""Subprocess entry point for task execution.

Accepts a task_id, sets up a full orch_context (SQL + ClickHouse),
loads the task from the database, and runs the complete execution flow:
import → deserialize → execute → serialize → update status → complete job.

Invoked by the worker via multiprocessing.Process(target=run_task_process, args=(task_id,)).

Exit codes (set on multiprocessing.Process.exitcode):
    0 — task completed (status written to DB)
    1 — task failed (status written to DB)
    2 — unexpected crash before DB status could be updated
"""

from __future__ import annotations

import asyncio
import signal

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


def run_task_process(task_id: int) -> None:
    """Multiprocessing entry point. Runs asyncio.run(run_task(...)) and exits.

    Called as: multiprocessing.Process(target=run_task_process, args=(task_id,))

    Installs a SIGTERM handler for graceful cancellation from the worker.
    With fork, clears the inherited chdb session singleton so the subprocess
    creates its own fresh session from AAICLICK_CH_URL.
    """
    import traceback

    from aaiclick.data.data_context.chdb_client import _sessions
    _sessions.clear()

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

    raise SystemExit(exit_code)
