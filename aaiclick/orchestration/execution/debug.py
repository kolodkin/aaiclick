"""Debug execution utilities for orchestration jobs.

This module provides functions for running jobs in debug/test mode.
"""

import asyncio

from ..decorators import JobFactory
from ..models import Job
from ..orch_context import orch_context
from .runner import run_job_tasks


def job_test(job: Job | JobFactory, **kwargs) -> Job:
    """
    Execute job synchronously in current process (test mode).

    Invokes the worker execute flow for testing/debugging.
    Similar to Airflow's test execution mode.

    Args:
        job: Job to execute, or a ``@job`` factory to create the job from first
        **kwargs: Arguments for the factory; only valid when ``job`` is a factory

    Returns:
        Job: The executed job

    Example:
        job_test(my_pipeline, x=3)  # Creates the job, blocks until it completes
    """
    return asyncio.run(ajob_test(job, **kwargs))


async def ajob_test(job: Job | JobFactory, **kwargs) -> Job:
    """
    Async implementation of test execution.

    Runs all tasks for this job within an OrchContext.

    Args:
        job: Job to execute, or a ``@job`` factory to create the job from first
        **kwargs: Arguments for the factory; only valid when ``job`` is a factory

    Returns:
        Job: The executed job
    """
    if kwargs and not isinstance(job, JobFactory):
        raise TypeError("kwargs are only accepted with a @job factory, not a created Job")
    async with orch_context():
        if isinstance(job, JobFactory):
            job = await job(**kwargs)
        await run_job_tasks(job)
    return job
