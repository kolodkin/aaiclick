"""Debug execution utilities for orchestration jobs.

This module provides functions for running jobs in debug/test mode,
separate from execution.py to avoid circular imports with models.py.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import Job


def test_job(job: Job) -> None:
    """
    Execute job synchronously in current process (test mode).

    Invokes the worker execute flow for testing/debugging.
    Similar to Airflow's test execution mode.

    Args:
        job: Job to execute

    Example:
        job = await create_job("my_job", "mymodule.task1")
        test_job(job)  # Blocks until job completes
    """
    asyncio.run(test_job_async(job))


async def test_job_async(job: Job) -> None:
    """
    Async implementation of test execution.

    Runs all tasks for this job within an OrchContext.

    Args:
        job: Job to execute
    """
    from .context import OrchContext
    from .execution import run_job_tasks

    async with OrchContext():
        await run_job_tasks(job)
