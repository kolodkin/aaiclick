"""
aaiclick.orchestration - Orchestration backend for job and task management.

This module provides the public API for defining and executing distributed
workflows using @task and @job decorators.

Usage:
    from aaiclick.orchestration import task, job

    @task
    async def my_task(x: int) -> int:
        return x * 2

    @job("my_pipeline")
    def my_pipeline(value: int):
        result = my_task(x=value)
        return [result]

    created_job = await my_pipeline(value=42)
"""

from .context import commit_tasks, get_orch_session, orch_context
from .debug_execution import ajob_test, job_test
from .decorators import JobFactory, TaskFactory, job, task
from .job_queries import count_jobs, get_job, list_jobs
from .models import JobStatus, TaskStatus
