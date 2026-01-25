"""Debug execution utilities for orchestration jobs.

This module provides functions for running jobs in debug/test mode.
"""

import asyncio

from .context import OrchContext
from .execution import run_job_tasks
from .models import Job


def job_test(job: Job) -> None:
    """
    Execute job synchronously in current process (test mode).

    Invokes the worker execute flow for testing/debugging.
    Similar to Airflow's test execution mode.

    Args:
        job: Job to execute

    Example:
        job = await create_job("my_job", "mymodule.task1")
        job_test(job)  # Blocks until job completes
    """
    asyncio.run(job_test_async(job))


async def job_test_async(job: Job) -> None:
    """
    Async implementation of test execution.

    Runs all tasks for this job within an OrchContext.

    Args:
        job: Job to execute
    """
    async with OrchContext():
        await run_job_tasks(job)
