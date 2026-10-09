"""Shared job waiter for the runner e2e suites.

Queries the ORM directly rather than going through ``internal_api``: these
suites assert on the ``Job`` row itself, and a run measured in minutes gains
nothing from the change signals ``cli_wait.wait_for_job`` uses.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from sqlmodel import col, select

from aaiclick.datetime_utils import utc_now
from aaiclick.orchestration.env import job_wait_timeout
from aaiclick.orchestration.execution.mp_worker import mp_worker_main_loop
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import TERMINAL_JOB_STATUSES, Job
from aaiclick.orchestration.orch_context import get_sql_session


async def wait_for_job_by_name(job_name: str, timeout: float | None = None) -> Job:
    """Poll the most recent Job with this name until it reaches a terminal
    status, or fail. On timeout, dump per-task states so a stuck or failing
    task is diagnosable from the CI log (the worker writes its own output to
    per-task log files, not stdout).
    """
    if timeout is None:
        timeout = job_wait_timeout()
    deadline = utc_now() + timedelta(seconds=timeout)
    job = None
    while utc_now() < deadline:
        async with get_sql_session() as session:
            result = await session.execute(
                select(Job).where(Job.name == job_name).order_by(col(Job.id).desc()).limit(1)
            )
            job = result.scalar_one_or_none()
        if job is not None and job.status in TERMINAL_JOB_STATUSES:
            return job
        await asyncio.sleep(1.0)
    lines = [f"Job {job_name!r} did not complete within {timeout}s; job_status={getattr(job, 'status', None)}"]
    if job is not None:
        for t in await get_tasks_for_job(job.id):
            lines.append(f"  task entrypoint={t.entrypoint!r} status={t.status} attempt={t.attempt} error={t.error!r}")
    raise TimeoutError("\n".join(lines))


async def run_worker_until_done(job_name: str) -> Job:
    """Drive an mp worker loop in the background until the job named
    ``job_name`` reaches a terminal status, then stop the loop."""
    worker_task = asyncio.create_task(
        mp_worker_main_loop(max_tasks=10, install_signal_handlers=False, max_empty_polls=10)
    )
    try:
        return await wait_for_job_by_name(job_name)
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
