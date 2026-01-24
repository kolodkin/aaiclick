"""Debug execution utilities for orchestration jobs.

This module provides functions for running jobs in debug/test mode,
separate from execution.py to avoid circular imports with models.py.
"""

import asyncio


def run_job_test(job) -> None:
    """
    Execute job synchronously in current process (test mode).

    Invokes the worker execute flow for testing/debugging.
    Similar to Airflow's test execution mode.

    Args:
        job: Job to execute

    Example:
        job = await create_job("my_job", "mymodule.task1")
        run_job_test(job)  # Blocks until job completes
    """
    asyncio.run(run_job_test_async(job))


async def run_job_test_async(job) -> None:
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
