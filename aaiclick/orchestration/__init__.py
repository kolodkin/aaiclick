"""
aaiclick.orchestration - Orchestration backend for job and task management.

This module provides the public API for defining and executing distributed
workflows using @task and @job decorators.

@job marks a function as the entry point task of a job. @task marks functions
for execution. Any task returning Task/Group objects triggers dynamic
registration to the current job.

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

from .claiming import cancel_job
from .context import commit_tasks, get_orch_session, orch_context
from .debug_execution import ajob_test, job_test
from .decorators import JobFactory, TaskFactory, job, task
from .dynamic import map, map_apply
from .job_queries import (
    count_jobs,
    get_job,
    get_latest_job_by_name,
    get_tasks_for_job,
    list_jobs,
    resolve_job,
)
from .job_stats import JobStats, TaskStats, compute_job_stats, print_job_stats
from .models import JobStatus, TaskStatus
