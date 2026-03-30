"""
aaiclick.orchestration - Orchestration backend for job and task management.

This module provides the public API for defining and executing distributed
workflows using @task and @job decorators.

@job marks a function as the entry point task of a job. @task marks functions
for execution. Tasks returning TaskResult(tasks=[...]) trigger dynamic
registration of child tasks to the current job.

Usage:
    from aaiclick.orchestration import task, job

    @task
    async def my_task(x: int) -> int:
        return x * 2

    @job("my_pipeline")
    def my_pipeline(value: int):
        result = my_task(x=value)
        return tasks_list(result)

    created_job = await my_pipeline(value=42)
"""

from .execution import ajob_test, cancel_job, job_test
from .result import TaskResult, data_list, task_result, tasks_list
from .orch_context import commit_tasks, get_sql_session, orch_context
from .decorators import JobFactory, TaskFactory, job, task
from .operators import map, reduce
from .jobs import (
    count_jobs,
    get_job,
    get_job_result,
    get_latest_job_by_name,
    get_tasks_for_job,
    list_jobs,
    resolve_job,
    JobStats,
    TaskStats,
    compute_job_stats,
    print_job_stats,
)
from .models import JobStatus, TaskStatus
